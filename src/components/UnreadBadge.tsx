type UnreadBadgeProps = {
  value: number | string;
  className?: string;
};

export function UnreadBadge({ value, className }: UnreadBadgeProps) {
  const rawValue = String(value);
  const numericValue = Number(rawValue);
  const displayValue = Number.isFinite(numericValue) && numericValue >= 10 ? "10+" : rawValue;
  const badgeClassName = ["unread-badge", className ?? ""].filter(Boolean).join(" ");

  return (
    <span className={badgeClassName} title={displayValue !== rawValue ? `${rawValue} unread` : undefined}>
      {displayValue}
    </span>
  );
}
