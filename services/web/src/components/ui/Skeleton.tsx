import styles from "./Skeleton.module.css";

export function Skeleton({ className }: { className?: string }) {
  return <div aria-hidden="true" className={className ? `${styles.skeleton} ${className}` : styles.skeleton} />;
}
