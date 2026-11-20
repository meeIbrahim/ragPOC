import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";

type Announce = (text: string) => void;

const AnnounceContext = createContext<Announce>(() => {});

/**
 * The page's one polite status region (design 7). Each announcement replaces the text node, so a
 * repeated message is read again. The answer region itself is never live.
 */
export function LiveRegionProvider({ children }: { children: ReactNode }) {
  const [message, setMessage] = useState({ text: "", n: 0 });
  const announce = useCallback<Announce>((text) => setMessage((m) => ({ text, n: m.n + 1 })), []);
  const value = useMemo(() => announce, [announce]);
  return (
    <AnnounceContext.Provider value={value}>
      {children}
      <div role="status" aria-live="polite" className="visually-hidden">
        <span key={message.n}>{message.text}</span>
      </div>
    </AnnounceContext.Provider>
  );
}

export function useAnnounce(): Announce {
  return useContext(AnnounceContext);
}
