use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::models::SupervisorCommand;
use crate::{to_string, CommandResult};

pub(crate) async fn mark_orphaned_agent_runs(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        update agent_runs
        set status = 'unknown', stopped_at = coalesce(stopped_at, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        where stopped_at is null and status in ('starting', 'running', 'stopping')
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;
    sqlx::query(
        "update agents set status = 'idle' where status in ('queued', 'running', 'stopping')",
    )
    .execute(pool)
    .await
    .map_err(to_string)?;
    sqlx::query(
        r#"
        update agent_work_items
        set status = 'failed',
            completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where status = 'running'
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

pub(crate) async fn recover_supervisor_commands_at_startup(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        update supervisor_commands
        set status = 'done',
            error = 'skipped stale command for terminal work item',
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where status in ('pending', 'running')
          and exists (
              select 1
              from agent_work_items w
              where w.id = supervisor_commands.work_item_id
          and w.status in ('cancelled', 'failed', 'done', 'silent')
          )
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    sqlx::query(
        r#"
        update supervisor_commands
        set status = 'pending',
            error = 'requeued after supervisor restart',
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where status = 'running'
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

pub(crate) async fn write_supervisor_heartbeat(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into supervisor_state (id, pid, status, updated_at)
        values (1, $1, 'running', strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        on conflict (id) do update set
            pid = excluded.pid,
            status = excluded.status,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(std::process::id() as i32)
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

pub(crate) async fn claim_next_supervisor_command(
    pool: &SqlitePool,
) -> CommandResult<Option<SupervisorCommand>> {
    let row = sqlx::query(
        r#"
        update supervisor_commands
        set status = 'running',
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = (
            select id
            from supervisor_commands
            where status = 'pending'
            order by created_at asc
            limit 1
        )
        returning id, command_type, agent_id, run_id, work_item_id
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    let Some(row) = row else {
        return Ok(None);
    };

    let command = SupervisorCommand {
        id: row.get("id"),
        command_type: row.get("command_type"),
        agent_id: row.get("agent_id"),
        run_id: row.get("run_id"),
        work_item_id: row.get("work_item_id"),
    };

    Ok(Some(command))
}

pub(crate) async fn finish_supervisor_command(
    pool: &SqlitePool,
    command_id: Uuid,
    error: Option<String>,
) -> CommandResult<()> {
    let (status, error) = match error {
        Some(error) => ("failed", error),
        None => ("done", String::new()),
    };

    sqlx::query(
        r#"
        update supervisor_commands
        set status = $2, error = $3, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(command_id)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

pub(crate) async fn cleanup_supervisor_commands(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        delete from supervisor_commands
        where status in ('done', 'failed')
          and updated_at < strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-7 days')
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs as std_fs;

    use sqlx::SqlitePool;
    use uuid::Uuid;

    use super::{claim_next_supervisor_command, recover_supervisor_commands_at_startup};
    use crate::db::{db_connect_with_url, migrate};

    #[tokio::test]
    async fn startup_recovery_requeues_running_supervisor_commands_and_skips_terminal_work() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "recovery-agent").await?;
            let channel_id = insert_test_channel(&pool, "recovery").await?;
            let queued_work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (agent_id, channel_id, title, context, status)
                values ($1, $2, 'queued request', 'context', 'queued')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let cancelled_work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (agent_id, channel_id, title, context, status)
                values ($1, $2, 'cancelled request', 'context', 'cancelled')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let running_command_id: Uuid = sqlx::query_scalar(
                r#"
                insert into supervisor_commands (command_type, agent_id, work_item_id, status)
                values ('start_agent', $1, $2, 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(queued_work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let terminal_command_id: Uuid = sqlx::query_scalar(
                r#"
                insert into supervisor_commands (command_type, agent_id, work_item_id, status)
                values ('start_agent', $1, $2, 'pending')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(cancelled_work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            recover_supervisor_commands_at_startup(&pool).await?;

            let running_status: String =
                sqlx::query_scalar("select status from supervisor_commands where id = $1")
                    .bind(running_command_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            let terminal_status: String =
                sqlx::query_scalar("select status from supervisor_commands where id = $1")
                    .bind(terminal_command_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;

            assert_eq!(running_status, "pending");
            assert_eq!(terminal_status, "done");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn supervisor_command_claim_is_single_consumer() {
        let Some((pool, schema)) = test_pool_with_connections(8).await else {
            return;
        };
        let result: Result<(), String> = async {
            let command_id: Uuid = sqlx::query_scalar(
                r#"
                insert into supervisor_commands (command_type)
                values ('start_agent')
                returning id
                "#,
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let (r1, r2, r3, r4) = tokio::join!(
                claim_next_supervisor_command(&pool),
                claim_next_supervisor_command(&pool),
                claim_next_supervisor_command(&pool),
                claim_next_supervisor_command(&pool)
            );
            let mut claimed = Vec::new();
            for result in [r1, r2, r3, r4] {
                if let Some(command) = result? {
                    claimed.push(command.id);
                }
            }

            assert_eq!(claimed, vec![command_id]);
            let status: String =
                sqlx::query_scalar("select status from supervisor_commands where id = $1")
                    .bind(command_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(status, "running");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    async fn test_pool() -> Option<(SqlitePool, String)> {
        test_pool_with_connections(1).await
    }

    async fn test_pool_with_connections(max_connections: u32) -> Option<(SqlitePool, String)> {
        let database_path =
            std::env::temp_dir().join(format!("lantor-test-{}.sqlite", Uuid::new_v4().simple()));
        let database_path = database_path.to_string_lossy().into_owned();
        let database_url = format!("sqlite://{database_path}");
        let pool = match db_connect_with_url(&database_url, max_connections).await {
            Ok(pool) => pool,
            Err(err) => {
                eprintln!("skipping SQLite-backed Lantor test: {err}");
                return None;
            }
        };
        if let Err(err) = migrate(&pool).await {
            eprintln!("skipping SQLite-backed Lantor test: {err}");
            pool.close().await;
            drop_sqlite_test_files(&database_path);
            return None;
        }
        Some((pool, database_path))
    }

    fn drop_sqlite_test_files(database_path: &str) {
        let _ = std_fs::remove_file(database_path);
        let _ = std_fs::remove_file(format!("{database_path}-wal"));
        let _ = std_fs::remove_file(format!("{database_path}-shm"));
    }

    async fn drop_test_schema(pool: SqlitePool, database_path: String) {
        pool.close().await;
        drop_sqlite_test_files(&database_path);
    }

    async fn insert_test_agent(pool: &SqlitePool, handle: &str) -> Result<Uuid, String> {
        sqlx::query_scalar(
            r#"
            insert into agents (handle, display_name, role, status, runtime, model, avatar, description)
            values ($1, $2, 'agent', 'idle', 'codex', 'gpt-5.5', 'D', 'test agent')
            returning id
            "#,
        )
        .bind(handle)
        .bind(handle)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
    }

    async fn insert_test_channel(pool: &SqlitePool, name: &str) -> Result<Uuid, String> {
        sqlx::query_scalar(
            r#"
            insert into channels (name, description, kind)
            values ($1, 'test channel', 'channel')
            returning id
            "#,
        )
        .bind(name)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
    }
}
