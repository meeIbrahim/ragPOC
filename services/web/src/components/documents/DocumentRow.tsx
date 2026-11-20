import { Button } from "../ui/Button";
import { CheckIcon } from "../ui/icons";
import { StatusBadge } from "../ui/StatusBadge";
import { FileName } from "./FileName";
import { accessibleName, type RowModel } from "./rowModel";
import styles from "./DocumentRow.module.css";

interface Props {
  row: RowModel;
  selected: boolean;
  active: boolean;
  flash: boolean;
  onActivate: () => void;
  onRetry: () => void;
  onDismiss: () => void;
}

export function DocumentRow({ row, selected, active, flash, onActivate, onRetry, onDismiss }: Props) {
  const classes = [styles.row, row.selectable && styles.selectable, selected && styles.selected, active && styles.active, flash && styles.flash]
    .filter(Boolean)
    .join(" ");
  return (
    <li
      id={row.domId}
      role="option"
      aria-label={accessibleName(row)}
      aria-selected={row.selectable ? selected : undefined}
      aria-disabled={row.selectable ? undefined : true}
      className={classes}
      onClick={onActivate}
    >
      <span className={styles.check} aria-hidden="true">
        {row.selectable && <span className={selected ? styles.boxChecked : styles.box}>{selected && <CheckIcon />}</span>}
      </span>
      <span className={styles.body}>
        <FileName name={row.filename} strong={selected} />
        <span className={styles.status}>
          <StatusBadge status={row.badge} />
          {row.detail && (
            <span className={`${styles.detail} ${styles[row.detailTone]}`} title={row.detailTitle}>
              {row.detail}
            </span>
          )}
        </span>
        {row.retryable && (
          <span className={styles.actions}>
            <Button
              variant="ghost"
              onClick={(e) => {
                e.stopPropagation();
                onRetry();
              }}
            >
              Retry
            </Button>
            <Button
              variant="ghost"
              onClick={(e) => {
                e.stopPropagation();
                onDismiss();
              }}
            >
              Dismiss
            </Button>
          </span>
        )}
      </span>
    </li>
  );
}
