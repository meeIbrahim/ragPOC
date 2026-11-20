import { useRef, useState, type DragEvent } from "react";
import { ALLOWED_EXTENSIONS } from "../../limits";
import { Button } from "../ui/Button";
import { InlineError } from "../ui/InlineError";
import styles from "./UploadDropzone.module.css";

export const UPLOAD_ERROR_ID = "upload-error";

/** Button first, drop as a bonus (design 3.1). The whole area is the drop target. */
export function UploadDropzone({ error, onFiles }: { error: string | undefined; onFiles: (files: File[]) => void }) {
  const input = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);

  const onDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setOver(false);
    onFiles(Array.from(e.dataTransfer.files));
  };

  return (
    <div>
      <div
        data-testid="dropzone"
        className={over ? `${styles.zone} ${styles.over}` : styles.zone}
        onDragOver={(e) => {
          e.preventDefault();
          setOver(true);
        }}
        onDragLeave={() => setOver(false)}
        onDrop={onDrop}
      >
        <Button variant="primary" onClick={() => input.current?.click()} aria-describedby={error ? UPLOAD_ERROR_ID : undefined}>
          Upload file
        </Button>
        {over ? (
          <span className={styles.helper}>Drop to upload</span>
        ) : (
          <span className={styles.helper}>
            PDF, TXT or MD
            <br />
            or drop a file here
          </span>
        )}
        <input
          ref={input}
          type="file"
          multiple
          hidden
          accept={ALLOWED_EXTENSIONS.join(",")}
          data-testid="file-input"
          onChange={(e) => {
            onFiles(Array.from(e.target.files ?? []));
            e.target.value = "";
          }}
        />
      </div>
      {error && <InlineError id={UPLOAD_ERROR_ID}>{error}</InlineError>}
    </div>
  );
}
