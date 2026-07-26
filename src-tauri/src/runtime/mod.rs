pub(crate) mod claude;
pub(crate) mod codex;
pub(crate) mod process;
pub(crate) mod streaming;
pub(crate) mod supervisor;
pub(crate) mod surface;
pub(crate) mod turn_outcome;

fn normalized_memory_context(memory_context: Option<&str>) -> Option<&str> {
    memory_context
        .map(str::trim)
        .filter(|context| !context.is_empty())
}

pub(super) fn runtime_launch_context_changed(
    current_environment_variables: &str,
    current_memory_context: Option<&str>,
    next_environment_variables: &str,
    next_memory_context: Option<&str>,
) -> bool {
    current_environment_variables != next_environment_variables
        || normalized_memory_context(current_memory_context)
            != normalized_memory_context(next_memory_context)
}

#[cfg(test)]
mod tests {
    use super::runtime_launch_context_changed;

    #[test]
    fn runtime_launch_context_detects_memory_changes() {
        assert!(!runtime_launch_context_changed("", None, "", None));
        assert!(!runtime_launch_context_changed(
            "",
            Some("  memory  "),
            "",
            Some("memory")
        ));
        assert!(runtime_launch_context_changed(
            "",
            Some("old memory"),
            "",
            Some("new memory")
        ));
        assert!(runtime_launch_context_changed(
            "KEY=old",
            Some("memory"),
            "KEY=new",
            Some("memory")
        ));
    }
}
