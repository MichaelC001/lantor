type UnreadBadgeProps = {
  value: number | string;
  className?: string;
};

export function UnreadBadge({ value, className }: UnreadBadgeProps) {
  return (
    <span className={className ? `unread-badge ${className}` : "unread-badge"}>
      {value}
    </span>
  );
}
