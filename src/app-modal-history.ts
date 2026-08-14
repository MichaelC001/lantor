export type AppModal = "search" | "activity" | "saved";

type AppModalHistoryEntry = {
  index: number;
  activeModal: AppModal | null;
};

type ShouldPopAppModalHistoryInput = {
  activeModal: AppModal | null;
  expectedModal: AppModal;
  canNavigateBack: boolean;
  currentIndex: number;
  historyState: AppModalHistoryEntry | null;
};

export function shouldReplaceAppModalHistory(
  currentModal: AppModal | null,
  nextModal: AppModal,
) {
  return currentModal !== null && currentModal !== nextModal;
}

export function shouldPopAppModalHistory({
  activeModal,
  expectedModal,
  canNavigateBack,
  currentIndex,
  historyState,
}: ShouldPopAppModalHistoryInput) {
  return activeModal === expectedModal
    && canNavigateBack
    && historyState?.index === currentIndex
    && historyState.activeModal === expectedModal;
}
