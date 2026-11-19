import { ApiError } from "./http";

/**
 * PUTs the file straight to the presigned MinIO URL. XHR, because fetch has no upload progress
 * (spec A8). onProgress gets a fraction from 0 to 1. Errors never carry the URL: it holds the
 * signature.
 */
export function putFile(url: string, file: File, onProgress: (fraction: number) => void, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url);
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && e.total > 0) {
        onProgress(e.loaded / e.total);
      }
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve();
      } else {
        reject(new ApiError("server", `Upload storage answered ${xhr.status}.`, xhr.status));
      }
    };
    xhr.onerror = () => reject(new ApiError("network", "Couldn't reach upload storage."));
    xhr.onabort = () => reject(new ApiError("aborted", "upload aborted"));
    if (signal) {
      if (signal.aborted) {
        xhr.abort();
        return;
      }
      signal.addEventListener("abort", () => xhr.abort(), { once: true });
    }
    xhr.send(file);
  });
}
