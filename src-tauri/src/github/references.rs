//! Data for rendering `#123` / `rw#123` references in messages: every channel
//! binding (with its short alias) plus the state of PRs and issues that are
//! already in the synced queues. Read-only and local; never calls GitHub.

use std::collections::HashMap;

use serde::Serialize;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::app::{to_string, CommandResult};

const MAX_ALIAS_LEN: usize = 16;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct GithubReferenceRepository {
    pub(crate) channel_id: Uuid,
    pub(crate) name_with_owner: String,
    pub(crate) url: String,
    pub(crate) reference_alias: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct GithubReferenceItem {
    pub(crate) name_with_owner: String,
    pub(crate) number: i64,
    /// `pull` or `issue`.
    pub(crate) kind: &'static str,
    pub(crate) title: String,
    pub(crate) url: String,
    pub(crate) state: String,
    pub(crate) is_draft: bool,
    pub(crate) checks_status: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct GithubReferences {
    pub(crate) repositories: Vec<GithubReferenceRepository>,
    pub(crate) items: Vec<GithubReferenceItem>,
}

pub(crate) async fn load_github_references(pool: &SqlitePool) -> CommandResult<GithubReferences> {
    let repositories = sqlx::query(
        r#"
        select channel_id, name_with_owner, url, reference_alias
        from channel_github_repositories
        order by name_with_owner, channel_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?
    .iter()
    .map(|row| GithubReferenceRepository {
        channel_id: row.get("channel_id"),
        name_with_owner: row.get("name_with_owner"),
        url: row.get("url"),
        reference_alias: row.get("reference_alias"),
    })
    .collect();

    // A repository can be bound to several channels and queued for several
    // review logins; keep the most recently updated copy of each item.
    let rows = sqlx::query(
        r#"
        select
            binding.name_with_owner, 'pull' as kind, cache.pull_number as number,
            cache.title, cache.url, cache.state, cache.is_draft, cache.checks_status,
            cache.github_updated_at
        from github_review_request_cache cache
        join channel_github_repositories binding
          on binding.channel_id = cache.channel_id
         and binding.repository_id = cache.repository_id
        union all
        select
            binding.name_with_owner, 'issue' as kind, cache.issue_number as number,
            cache.title, cache.url, cache.state, 0 as is_draft, 'none' as checks_status,
            cache.github_updated_at
        from github_issue_cache cache
        join channel_github_repositories binding
          on binding.channel_id = cache.channel_id
         and binding.repository_id = cache.repository_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    let mut items: HashMap<(String, i64), GithubReferenceItem> = HashMap::new();
    for row in rows {
        let kind: String = row.get("kind");
        let item = GithubReferenceItem {
            name_with_owner: row.get("name_with_owner"),
            number: row.get("number"),
            kind: if kind == "pull" { "pull" } else { "issue" },
            title: row.get("title"),
            url: row.get("url"),
            state: row.get::<String, _>("state").to_ascii_lowercase(),
            is_draft: row.get("is_draft"),
            checks_status: row.get("checks_status"),
            updated_at: row.get("github_updated_at"),
        };
        let key = (item.name_with_owner.to_ascii_lowercase(), item.number);
        match items.get(&key) {
            Some(existing) if existing.updated_at >= item.updated_at => {}
            _ => {
                items.insert(key, item);
            }
        }
    }
    let mut items: Vec<_> = items.into_values().collect();
    items.sort_by(|left, right| {
        (&left.name_with_owner, left.number).cmp(&(&right.name_with_owner, right.number))
    });

    Ok(GithubReferences {
        repositories,
        items,
    })
}

/// Normalizes a user-entered alias: lower-case ASCII letter first, then
/// letters, digits or `-`. An empty alias clears it.
pub(crate) fn normalized_reference_alias(alias: &str) -> CommandResult<String> {
    let alias = alias.trim().trim_end_matches('#').to_ascii_lowercase();
    if alias.is_empty() {
        return Ok(alias);
    }
    let valid = alias.len() <= MAX_ALIAS_LEN
        && alias.starts_with(|ch: char| ch.is_ascii_lowercase())
        && alias
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-');
    if !valid {
        return Err(format!(
            "short name must start with a letter and use at most {MAX_ALIAS_LEN} letters, digits or '-'"
        ));
    }
    if alias == "task" {
        return Err("`task` is reserved for Lantor task references".to_owned());
    }
    Ok(alias)
}

pub(crate) async fn set_github_reference_alias(
    pool: &SqlitePool,
    channel_id: Uuid,
    alias: &str,
) -> CommandResult<GithubReferences> {
    let alias = normalized_reference_alias(alias)?;
    if !alias.is_empty() {
        let taken: Option<String> = sqlx::query_scalar(
            r#"
            select name_with_owner
            from channel_github_repositories
            where reference_alias = $1
              and channel_id <> $2
              and lower(name_with_owner) <> (
                  select lower(name_with_owner)
                  from channel_github_repositories
                  where channel_id = $2
              )
            limit 1
            "#,
        )
        .bind(&alias)
        .bind(channel_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
        if let Some(repository) = taken {
            return Err(format!(
                "short name `{alias}` is already used by {repository}"
            ));
        }
    }
    let updated = sqlx::query(
        r#"
        update channel_github_repositories
        set reference_alias = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where channel_id = $1
        "#,
    )
    .bind(channel_id)
    .bind(&alias)
    .execute(pool)
    .await
    .map_err(to_string)?;
    if updated.rows_affected() == 0 {
        return Err("channel has no GitHub repository binding".to_owned());
    }
    load_github_references(pool).await
}

#[cfg(test)]
mod tests;
