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
