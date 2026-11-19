import styles from "./FileName.module.css";

const TAIL = 10;

/**
 * Middle truncation in CSS: the head shrinks with an ellipsis, the tail (with the extension)
 * always shows. The full name is in title. dir="auto" lets Urdu or Arabic names run right to left (T5).
 */
export function FileName({ name, strong = false }: { name: string; strong?: boolean }) {
  const cut = name.length > TAIL * 2 ? name.length - TAIL : name.length;
  return (
    <span className={strong ? `${styles.name} ${styles.strong}` : styles.name} title={name} dir="auto">
      <span className={styles.head}>{name.slice(0, cut)}</span>
      {cut < name.length && <span className={styles.tail}>{name.slice(cut)}</span>}
    </span>
  );
}
