import type { SVGProps } from "react";
import styles from "./icons.module.css";

// Decorative icons (spec A5): always next to text, so always aria-hidden.
function Svg({ className, children, ...rest }: SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
      className={className ? `${styles.icon} ${className}` : styles.icon}
      {...rest}
    >
      {children}
    </svg>
  );
}

export const CheckIcon = () => (
  <Svg>
    <path d="M3.5 8.5l3 3 6-7" />
  </Svg>
);

export const CrossIcon = () => (
  <Svg>
    <path d="M4 4l8 8M12 4l-8 8" />
  </Svg>
);

export const ClockIcon = () => (
  <Svg>
    <circle cx="8" cy="8" r="6" />
    <path d="M8 4.5V8l2.5 1.5" />
  </Svg>
);

export const InfoIcon = () => (
  <Svg>
    <circle cx="8" cy="8" r="6" />
    <path d="M8 7.2V11M8 5h.01" />
  </Svg>
);

export const ErrorIcon = () => (
  <Svg>
    <circle cx="8" cy="8" r="6" />
    <path d="M8 4.8V8.6M8 11h.01" />
  </Svg>
);

export const ChevronIcon = ({ open }: { open?: boolean }) => (
  <Svg className={open ? styles.open : styles.chevron}>
    <path d="M6 4l4 4-4 4" />
  </Svg>
);

export const SpinnerIcon = () => (
  <Svg className={styles.spin}>
    <path d="M8 2a6 6 0 1 1-6 6" />
  </Svg>
);
