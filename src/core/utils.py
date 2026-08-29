import hashlib
from pathlib import Path

def hash_document(document_path: Path) -> str:
        sha256 = hashlib.sha256()
        with document_path.open("rb") as f:
            while chunk := f.read(1024*1024):
                sha256.update(chunk)
        return sha256.hexdigest()

def _get_repo_root() -> Path:
    """Traverses up from the current file to find the repository root."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        # Check for common repository root indicators
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    # Fallback to the current file's directory if no root marker is found
    return current.parent

REPO_ROOT = _get_repo_root()
