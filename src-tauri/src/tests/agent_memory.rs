use super::{format_memory_index_entry, insert_memory_index_entry};

#[test]
fn memory_append_can_add_work_log_link_without_timestamp_log() {
    let memory = "# @agent\n\n## Role\nLantor agent.\n\n## Key Knowledge\n- Add stable facts and links that help a restarted agent recover quickly.\n\n## Active Context\n- Currently working on: none.";

    let updated = insert_memory_index_entry(
        memory,
        &format_memory_index_entry("`notes/work-log.md` - staged durable updates."),
    );

    assert!(updated.contains("## Key Knowledge\n- `notes/work-log.md` - staged durable updates."));
    assert!(updated.contains("\n## Active Context"));
    assert!(!updated.contains("Memory update"));
    assert!(!updated.contains("Add stable facts and links"));
}
