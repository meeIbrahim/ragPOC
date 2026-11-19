import styles from "./FileName.module.css";

const TAIL = 10;

/**
 * Middle truncation in CSS: the head shrinks with an ellipsis, the tail (with the extension)
 * always shows. The full name is in title. dir="auto" lets Urdu or Arabic names run right to left (T5).
 */
export function FileName({ name, strong = false }: { name: string; strong?: boolean }) {
  // Code points, so an emoji is never split at the cut.
  const chars = [...name];
  const cut = chars.length > TAIL * 2 ? chars.length - TAIL : chars.length;
  const head = chars.slice(0, cut).join("");
  const tail = chars.slice(cut).join("");
  return (
    <span className={strong ? `${styles.name} ${styles.strong}` : styles.name} title={name} dir="auto">
      <span className={styles.head}>{head}</span>
      {tail && <span className={styles.tail}>{tail}</span>}
    </span>
  );
}
