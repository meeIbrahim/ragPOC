import { useEffect, useState, type KeyboardEvent } from "react";
import { DocumentRow } from "./DocumentRow";
import type { RowModel } from "./rowModel";
import styles from "./DocumentList.module.css";

interface Props {
  rows: RowModel[];
  selected: ReadonlySet<string>;
  onToggle: (entryId: string) => void;
  onRetry: (localKey: number) => void;
  onDismiss: (localKey: number) => void;
  /** The entry id that just absorbed a duplicate; n restarts the flash. */
  flash: { id: string; n: number } | undefined;
}

/**
 * One tab stop with roving focus through aria-activedescendant (design 3.2): Up, Down, Home and
 * End move; Space toggles a selectable row. 500 rows never mean 500 tab stops.
 */
export function DocumentList({ rows, selected, onToggle, onRetry, onDismiss, flash }: Props) {
  const [activeId, setActiveId] = useState<string | undefined>(undefined);
  const [focused, setFocused] = useState(false);
  const index = rows.findIndex((r) => r.domId === activeId);
  const current = index >= 0 ? index : 0;
  const activeRow = rows[current];

  useEffect(() => {
    if (!flash) {
      return;
    }
    document.getElementById(`doc-${flash.id}`)?.scrollIntoView({ block: "nearest" });
  }, [flash]);

  const move = (i: number) => {
    const row = rows[Math.max(0, Math.min(rows.length - 1, i))];
    if (row) {
      setActiveId(row.domId);
      document.getElementById(row.domId)?.scrollIntoView({ block: "nearest" });
    }
  };

  const toggle = (row: RowModel | undefined) => {
    if (row?.selectable && row.entryId) {
      onToggle(row.entryId);
    }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLUListElement>) => {
    const keys: Record<string, () => void> = {
      ArrowDown: () => move(current + 1),
      ArrowUp: () => move(current - 1),
      Home: () => move(0),
      End: () => move(rows.length - 1),
      " ": () => toggle(activeRow),
    };
    const action = keys[e.key];
    if (action && e.target === e.currentTarget) {
      e.preventDefault();
      action();
    }
  };

  return (
    <ul
      role="listbox"
      aria-label="Documents"
      aria-multiselectable="true"
      aria-activedescendant={activeRow?.domId}
      tabIndex={0}
      className={styles.list}
      onKeyDown={onKeyDown}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
    >
      {rows.map((row) => (
        <DocumentRow
          key={flash && row.entryId === flash.id ? `${row.domId}-${flash.n}` : row.domId}
          row={row}
          selected={row.entryId !== undefined && selected.has(row.entryId)}
          active={focused && row.domId === activeRow?.domId}
          flash={flash !== undefined && row.entryId === flash.id}
          onActivate={() => {
            setActiveId(row.domId);
            toggle(row);
          }}
          onRetry={() => row.localKey !== undefined && onRetry(row.localKey)}
          onDismiss={() => row.localKey !== undefined && onDismiss(row.localKey)}
        />
      ))}
    </ul>
  );
}
