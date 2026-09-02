use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::app::{to_string, CommandResult};
use crate::models::{AgentSubscriptionStatus, AgentSubscriptionWindow};
use crate::ui_notifications::{enqueue_ui_event_in_tx, UiEvent};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AgentSubscriptionSnapshot {
    pub(crate) provider: String,
    pub(crate) plan: Option<String>,
    pub(crate) status: String,
    pub(crate) windows: Vec<AgentSubscriptionWindow>,
}

pub(crate) async fn persist_agent_subscription_status(
    pool: &SqlitePool,
    agent_id: Uuid,
    snapshot: &AgentSubscriptionSnapshot,
) -> CommandResult<()> {
    let windows_json = serde_json::to_string(&snapshot.windows).map_err(to_string)?;
    let observed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut transaction = pool.begin().await.map_err(to_string)?;
    sqlx::query(
        r#"
        insert into agent_subscription_status (
            agent_id, provider, plan, status, windows_json, observed_at
        )
        values ($1, $2, $3, $4, $5, $6)
        on conflict (agent_id) do update set
            provider = excluded.provider,
            plan = excluded.plan,
            status = excluded.status,
            windows_json = excluded.windows_json,
            observed_at = excluded.observed_at
        "#,
    )
    .bind(agent_id)
    .bind(&snapshot.provider)
    .bind(&snapshot.plan)
    .bind(&snapshot.status)
    .bind(windows_json)
    .bind(&observed_at)
    .execute(&mut *transaction)
    .await
    .map_err(to_string)?;
    let subscription_status = AgentSubscriptionStatus {
        provider: snapshot.provider.clone(),
        plan: snapshot.plan.clone(),
        status: snapshot.status.clone(),
        windows: snapshot.windows.clone(),
        observed_at,
    };
    enqueue_ui_event_in_tx(
        &mut transaction,
        &UiEvent::AgentSubscriptionStatusUpsert {
            reason: "agent_subscription_status_updated",
            agent_id,
            subscription_status: &subscription_status,
        },
    )
    .await?;
    transaction.commit().await.map_err(to_string)
}

pub(crate) fn codex_subscription_status_from_response(
    value: &Value,
) -> Option<AgentSubscriptionSnapshot> {
    let result = value.get("result")?.as_object()?;
    let multi_bucket = result
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .filter(|limits| !limits.is_empty());
    let snapshots = if let Some(limits) = multi_bucket {
        limits
            .iter()
            .map(|(key, snapshot)| (Some(key.as_str()), snapshot))
            .collect::<Vec<_>>()
    } else {
        vec![(None, result.get("rateLimits")?)]
    };

    let include_bucket_name = snapshots.len() > 1;
    let mut plan = None;
    let mut reached = false;
    let mut windows = Vec::new();
    for (bucket_key, snapshot) in snapshots {
        plan = plan.or_else(|| {
            snapshot
                .get("planType")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        reached |= snapshot
            .get("rateLimitReachedType")
            .is_some_and(|value| !value.is_null());
        reached |= snapshot
            .get("spendControlReached")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let bucket_id = snapshot
            .get("limitId")
            .and_then(Value::as_str)
            .or(bucket_key)
            .unwrap_or("codex");
        let bucket_label = snapshot
            .get("limitName")
            .and_then(Value::as_str)
            .filter(|label| !label.trim().is_empty())
            .or(bucket_key.filter(|key| *key != "codex"));
        for (position, field) in [("primary", "primary"), ("secondary", "secondary")] {
            let Some(window) = snapshot.get(field).and_then(Value::as_object) else {
                continue;
            };
            let Some(used_percent) = window.get("usedPercent").and_then(Value::as_f64) else {
                continue;
            };
            let duration_mins = window.get("windowDurationMins").and_then(Value::as_i64);
            let base_label = duration_mins
                .map(subscription_window_label)
                .unwrap_or_else(|| title_case_identifier(position));
            let label = if include_bucket_name {
                bucket_label
                    .map(|bucket| format!("{} · {base_label}", title_case_identifier(bucket)))
                    .unwrap_or(base_label)
            } else {
                base_label
            };
            windows.push(AgentSubscriptionWindow {
                id: format!("{bucket_id}:{position}"),
                label,
                used_percent: used_percent.clamp(0.0, 100.0),
                resets_at: window.get("resetsAt").and_then(Value::as_i64),
            });
        }
    }

    let status = quota_status(reached, &windows);
    Some(AgentSubscriptionSnapshot {
        provider: "codex".to_owned(),
        plan,
        status,
        windows,
    })
}

pub(crate) fn claude_subscription_status_from_event(
    value: &Value,
) -> Option<AgentSubscriptionSnapshot> {
    if value.get("type").and_then(Value::as_str) != Some("rate_limit_event") {
        return None;
    }
    let info = value.get("rate_limit_info")?.as_object()?;
    let provider_status = info
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_ascii_lowercase();
    let is_using_overage = info
        .get("isUsingOverage")
        .or_else(|| info.get("is_using_overage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut raw_windows = info
        .get("unifiedWindows")
        .or_else(|| info.get("unified_windows"))
        .or_else(|| info.get("rate_limits"))
        .and_then(Value::as_object)
        .map(|windows| windows.iter().collect::<Vec<_>>())
        .unwrap_or_default();
    raw_windows.sort_by_key(|(key, _)| claude_window_order(key));

    let windows = raw_windows
        .into_iter()
        .filter(|(key, _)| is_using_overage || !key.ends_with("_overage_included"))
        .filter_map(|(key, window)| {
            let used_percent = window
                .get("utilization")
                .and_then(Value::as_f64)
                .map(|utilization| utilization * 100.0)
                .or_else(|| window.get("used_percentage").and_then(Value::as_f64))
                .or_else(|| window.get("usedPercent").and_then(Value::as_f64))?;
            Some(AgentSubscriptionWindow {
                id: key.to_owned(),
                label: claude_window_label(key),
                used_percent: used_percent.clamp(0.0, 100.0),
                resets_at: window
                    .get("resetsAt")
                    .or_else(|| window.get("resets_at"))
                    .and_then(Value::as_i64),
            })
        })
        .collect::<Vec<_>>();
    let reached = matches!(provider_status.as_str(), "rejected" | "limited" | "blocked");
    let status = if reached {
        "limited".to_owned()
    } else if provider_status.contains("warning") {
        "warning".to_owned()
    } else {
        quota_status(false, &windows)
    };

    Some(AgentSubscriptionSnapshot {
        provider: "claude".to_owned(),
        plan: None,
        status,
        windows,
    })
}

fn quota_status(reached: bool, windows: &[AgentSubscriptionWindow]) -> String {
    if reached || windows.iter().any(|window| window.used_percent >= 100.0) {
        "limited".to_owned()
    } else if windows.is_empty() {
        "unknown".to_owned()
    } else if windows.iter().any(|window| window.used_percent >= 90.0) {
        "warning".to_owned()
    } else {
        "available".to_owned()
    }
}

fn subscription_window_label(duration_mins: i64) -> String {
    match duration_mins {
        10_080 => "Weekly".to_owned(),
        minutes if minutes > 0 && minutes % 10_080 == 0 => {
            format!("{}-week", minutes / 10_080)
        }
        1_440 => "Daily".to_owned(),
        minutes if minutes > 0 && minutes % 1_440 == 0 => {
            format!("{}-day", minutes / 1_440)
        }
        minutes if minutes > 0 && minutes % 60 == 0 => {
            format!("{}-hour", minutes / 60)
        }
        minutes => format!("{minutes}-minute"),
    }
}

fn claude_window_label(key: &str) -> String {
    match key {
        "five_hour" => "5-hour".to_owned(),
        "seven_day" => "Weekly".to_owned(),
        "seven_day_overage_included" => "Weekly · extra usage".to_owned(),
        _ => title_case_identifier(key),
    }
}

fn claude_window_order(key: &str) -> (u8, &str) {
    let rank = match key {
        "five_hour" => 0,
        "seven_day" => 1,
        "seven_day_overage_included" => 2,
        _ => 3,
    };
    (rank, key)
}

fn title_case_identifier(value: &str) -> String {
    value
        .split(['_', '-'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::agent_profile::load_agents;
    use crate::models::AgentSubscriptionWindow;
    use crate::test_support::{drop_test_schema, insert_test_agent, test_pool};

    use super::{
        claude_subscription_status_from_event, codex_subscription_status_from_response,
        persist_agent_subscription_status, AgentSubscriptionSnapshot,
    };

    #[test]
    fn parses_codex_multi_bucket_rate_limits() {
        let snapshot = codex_subscription_status_from_response(&json!({
            "id": 7,
            "result": {
                "rateLimits": {},
                "rateLimitsByLimitId": {
                    "codex": {
                        "limitId": "codex",
                        "limitName": "Codex",
                        "primary": {
                            "usedPercent": 37.5,
                            "windowDurationMins": 300,
                            "resetsAt": 1_788_348_600
                        },
                        "secondary": {
                            "usedPercent": 81.0,
                            "windowDurationMins": 10_080,
                            "resetsAt": 1_788_400_800
                        },
                        "planType": "pro",
                        "rateLimitReachedType": null
                    }
                }
            }
        }))
        .expect("rate-limit response should parse");

        assert_eq!(snapshot.provider, "codex");
        assert_eq!(snapshot.plan.as_deref(), Some("pro"));
        assert_eq!(snapshot.status, "available");
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5-hour");
        assert_eq!(snapshot.windows[1].label, "Weekly");
        assert_eq!(snapshot.windows[1].used_percent, 81.0);
    }

    #[test]
    fn parses_claude_unified_windows_and_omits_inactive_overage() {
        let snapshot = claude_subscription_status_from_event(&json!({
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": "allowed_warning",
                "isUsingOverage": false,
                "unifiedWindows": {
                    "seven_day_overage_included": {
                        "utilization": 0.21,
                        "resetsAt": 1_788_400_800
                    },
                    "seven_day": {
                        "utilization": 0.10,
                        "resetsAt": 1_788_400_800
                    },
                    "five_hour": {
                        "utilization": 0.99,
                        "resetsAt": 1_788_348_600
                    }
                }
            }
        }))
        .expect("rate-limit event should parse");

        assert_eq!(snapshot.provider, "claude");
        assert_eq!(snapshot.status, "warning");
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5-hour");
        assert_eq!(snapshot.windows[0].used_percent, 99.0);
        assert_eq!(snapshot.windows[1].label, "Weekly");
    }

    #[test]
    fn accepts_claude_status_line_style_window_fields() {
        let snapshot = claude_subscription_status_from_event(&json!({
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": "allowed",
                "rate_limits": {
                    "five_hour": {
                        "used_percentage": 42.5,
                        "resets_at": 1_788_348_600
                    },
                    "seven_day": {
                        "used_percentage": 12,
                        "resets_at": 1_788_400_800
                    }
                }
            }
        }))
        .expect("alternate Claude rate-limit fields should parse");

        assert_eq!(snapshot.status, "available");
        assert_eq!(snapshot.windows[0].used_percent, 42.5);
        assert_eq!(snapshot.windows[0].resets_at, Some(1_788_348_600));
    }

    #[tokio::test]
    async fn persists_subscription_snapshot_with_transactional_ui_refresh() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "quota-agent").await?;
            let snapshot = AgentSubscriptionSnapshot {
                provider: "codex".to_owned(),
                plan: Some("plus".to_owned()),
                status: "available".to_owned(),
                windows: vec![AgentSubscriptionWindow {
                    id: "codex:primary".to_owned(),
                    label: "5-hour".to_owned(),
                    used_percent: 24.0,
                    resets_at: Some(1_788_348_600),
                }],
            };
            persist_agent_subscription_status(&pool, agent_id, &snapshot).await?;

            let agents = load_agents(&pool).await?;
            let status = agents
                .into_iter()
                .find(|agent| agent.id == agent_id)
                .and_then(|agent| agent.subscription_status)
                .expect("persisted subscription status should load with the agent");
            assert_eq!(status.provider, "codex");
            assert_eq!(status.plan.as_deref(), Some("plus"));
            assert_eq!(status.windows, snapshot.windows);

            let refresh_event: String = sqlx::query_scalar(
                "select json_extract(event_json, '$.type') from ui_events order by id desc limit 1",
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(refresh_event, "agent_subscription_status_upsert");
            let refresh_reason: String = sqlx::query_scalar(
                "select json_extract(event_json, '$.reason') from ui_events order by id desc limit 1",
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(refresh_reason, "agent_subscription_status_updated");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        result.unwrap();
    }
}
