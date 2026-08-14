use super::{
    channel_wiki_wake_block, list_channel_wiki_revisions, load_channel_wiki_head,
    publish_channel_wiki_revision, ChannelWikiPublishOutcome, CHANNEL_WIKI_INLINE_BYTES,
    CHANNEL_WIKI_MAX_BYTES,
};
use crate::test_support::{drop_test_schema, insert_test_channel, test_pool};

#[tokio::test]
async fn publish_read_and_cas_conflict_flow() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let result: Result<(), String> = async {
        let channel_id = insert_test_channel(&pool, "wiki-flow").await?;
        assert!(load_channel_wiki_head(&pool, channel_id).await?.is_none());

        let first = match publish_channel_wiki_revision(
            &pool,
            channel_id,
            None,
            "# Wiki v1\nread this first",
            "@vegapunk",
            "initial version",
        )
        .await?
        {
            ChannelWikiPublishOutcome::Published(revision) => revision,
            other => return Err(format!("expected publish, got {other:?}")),
        };
        assert_eq!(first.parent_id, None);

        let head = load_channel_wiki_head(&pool, channel_id)
            .await?
            .ok_or("missing head after first publish")?;
        assert_eq!(head.id, first.id);
        assert_eq!(head.author, "@vegapunk");
        assert_eq!(head.note, "initial version");

        // Advance from the current head succeeds.
        let second = match publish_channel_wiki_revision(
            &pool,
            channel_id,
            Some(first.id),
            "# Wiki v2",
            "@arch",
            "add reliability section",
        )
        .await?
        {
            ChannelWikiPublishOutcome::Published(revision) => revision,
            other => return Err(format!("expected publish, got {other:?}")),
        };
        assert_eq!(second.parent_id, Some(first.id));

        // A concurrent editor still holding the stale parent must conflict and
        // must not overwrite the new head.
        match publish_channel_wiki_revision(
            &pool,
            channel_id,
            Some(first.id),
            "# Wiki v2 (stale editor)",
            "@mk",
            "lost update attempt",
        )
        .await?
        {
            ChannelWikiPublishOutcome::Conflict(current) => assert_eq!(current.id, second.id),
            other => return Err(format!("expected conflict, got {other:?}")),
        }
        let head = load_channel_wiki_head(&pool, channel_id)
            .await?
            .ok_or("missing head after conflict")?;
        assert_eq!(head.id, second.id);
        assert_eq!(head.content, "# Wiki v2");

        // Publishing with no parent while a head exists is also a conflict.
        match publish_channel_wiki_revision(&pool, channel_id, None, "# reset", "@mk", "").await? {
            ChannelWikiPublishOutcome::Conflict(current) => assert_eq!(current.id, second.id),
            other => return Err(format!("expected conflict, got {other:?}")),
        }

        let revisions = list_channel_wiki_revisions(&pool, channel_id, 10).await?;
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[0].id, second.id);
        assert_eq!(revisions[1].id, first.id);
        Ok(())
    }
    .await;
    drop_test_schema(pool, schema).await;
    assert!(result.is_ok(), "{:?}", result.err());
}

#[tokio::test]
async fn publish_rejects_empty_and_oversized_content() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let result: Result<(), String> = async {
        let channel_id = insert_test_channel(&pool, "wiki-limits").await?;
        let empty = publish_channel_wiki_revision(&pool, channel_id, None, "  \n", "@a", "").await;
        assert!(empty.is_err(), "empty content must be rejected");

        let oversized = "x".repeat(CHANNEL_WIKI_MAX_BYTES + 1);
        let too_big =
            publish_channel_wiki_revision(&pool, channel_id, None, &oversized, "@a", "").await;
        let err = too_big.err().ok_or("oversized content must be rejected")?;
        assert!(err.contains("hard limit"), "unexpected error: {err}");

        assert!(load_channel_wiki_head(&pool, channel_id).await?.is_none());
        Ok(())
    }
    .await;
    drop_test_schema(pool, schema).await;
    assert!(result.is_ok(), "{:?}", result.err());
}

#[tokio::test]
async fn wake_block_inlines_small_wikis_and_announces_large_ones() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let result: Result<(), String> = async {
        let channel_id = insert_test_channel(&pool, "wiki-wake").await?;
        assert!(channel_wiki_wake_block(&pool, channel_id, "#wiki-wake")
            .await?
            .is_none());

        let small = match publish_channel_wiki_revision(
            &pool,
            channel_id,
            None,
            "# Small wiki\nalways read me",
            "@vegapunk",
            "",
        )
        .await?
        {
            ChannelWikiPublishOutcome::Published(revision) => revision,
            other => return Err(format!("expected publish, got {other:?}")),
        };
        let block = channel_wiki_wake_block(&pool, channel_id, "#wiki-wake")
            .await?
            .ok_or("expected wake block for small wiki")?;
        assert!(block.contains("Channel wiki for #wiki-wake"));
        assert!(block.contains("always read me"), "small wiki must inline");

        let large_content = format!("# Large wiki\n{}", "y".repeat(CHANNEL_WIKI_INLINE_BYTES));
        match publish_channel_wiki_revision(
            &pool,
            channel_id,
            Some(small.id),
            &large_content,
            "@vegapunk",
            "grow past inline threshold",
        )
        .await?
        {
            ChannelWikiPublishOutcome::Published(_) => {}
            other => return Err(format!("expected publish, got {other:?}")),
        }
        let block = channel_wiki_wake_block(&pool, channel_id, "#wiki-wake")
            .await?
            .ok_or("expected wake block for large wiki")?;
        assert!(
            !block.contains("yyyy"),
            "large wiki must not inline content"
        );
        assert!(
            block.contains("wiki-read"),
            "large wiki must point at wiki-read"
        );
        Ok(())
    }
    .await;
    drop_test_schema(pool, schema).await;
    assert!(result.is_ok(), "{:?}", result.err());
}

#[tokio::test]
async fn publish_announces_system_message_and_application_flow_reports_conflict() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let result: Result<(), String> = async {
        use uuid::Uuid;

        use crate::application::wiki::{
            load_channel_wiki, publish_channel_wiki, LoadChannelWikiRequest,
            PublishChannelWikiRequest,
        };

        let channel_id = insert_test_channel(&pool, "wiki-ui").await?;

        let empty = load_channel_wiki(&pool, LoadChannelWikiRequest { channel_id }).await?;
        assert!(empty.head.is_none());
        assert!(empty.revisions.is_empty());
        assert_eq!(empty.max_bytes, CHANNEL_WIKI_MAX_BYTES);

        // First publish from the owner UI succeeds and announces in chat.
        let first = publish_channel_wiki(
            &pool,
            PublishChannelWikiRequest {
                channel_id,
                parent_id: None,
                content: "# Wiki v1".to_owned(),
                note: "first version".to_owned(),
            },
        )
        .await?;
        assert_eq!(first.outcome, "published");
        let head = first.overview.head.as_ref().ok_or("missing head")?;
        assert_eq!(head.author, "owner");
        assert_eq!(first.overview.revisions.len(), 1);

        let announcements: i64 = sqlx::query_scalar(
            r#"
            select count(*)
            from messages
            where channel_id = $1
              and sender_role = 'system'
              and body like '%created the channel wiki%'
            "#,
        )
        .bind(channel_id)
        .fetch_one(&pool)
        .await
        .map_err(|err| err.to_string())?;
        assert_eq!(announcements, 1, "publish must announce in chat");

        // A stale parent (still None while a head exists) must come back as a
        // structured conflict with the current head attached, and must not
        // announce or advance the head.
        let conflict = publish_channel_wiki(
            &pool,
            PublishChannelWikiRequest {
                channel_id,
                parent_id: None,
                content: "# Stale rewrite".to_owned(),
                note: String::new(),
            },
        )
        .await?;
        assert_eq!(
            conflict.outcome, "conflict",
            "parentless republish must conflict"
        );
        assert_eq!(conflict.overview.revisions.len(), 1);

        let stale_parent = Uuid::new_v4();
        let conflict = publish_channel_wiki(
            &pool,
            PublishChannelWikiRequest {
                channel_id,
                parent_id: Some(stale_parent),
                content: "# Stale rewrite".to_owned(),
                note: String::new(),
            },
        )
        .await?;
        assert_eq!(conflict.outcome, "conflict");
        assert_eq!(
            conflict.overview.head.as_ref().map(|rev| rev.id),
            first.overview.head.as_ref().map(|rev| rev.id),
            "conflict must return the unchanged head"
        );

        let system_messages: i64 = sqlx::query_scalar(
            "select count(*) from messages where channel_id = $1 and sender_role = 'system'",
        )
        .bind(channel_id)
        .fetch_one(&pool)
        .await
        .map_err(|err| err.to_string())?;
        assert_eq!(system_messages, 1, "conflict must not announce");

        // Advancing from the real head publishes and announces an update.
        let head_id = first.overview.head.as_ref().map(|rev| rev.id);
        let second = publish_channel_wiki(
            &pool,
            PublishChannelWikiRequest {
                channel_id,
                parent_id: head_id,
                content: "# Wiki v2".to_owned(),
                note: "expand conventions".to_owned(),
            },
        )
        .await?;
        assert_eq!(second.outcome, "published");
        assert_eq!(second.overview.revisions.len(), 2);
        let updates: i64 = sqlx::query_scalar(
            r#"
            select count(*)
            from messages
            where channel_id = $1
              and sender_role = 'system'
              and body like '%updated the channel wiki%'
              and body like '%expand conventions%'
            "#,
        )
        .bind(channel_id)
        .fetch_one(&pool)
        .await
        .map_err(|err| err.to_string())?;
        assert_eq!(updates, 1);
        Ok(())
    }
    .await;
    drop_test_schema(pool, schema).await;
    assert!(result.is_ok(), "{:?}", result.err());
}
