from pathlib import Path
from unittest.mock import patch

from storage import minio_storage


@patch("storage.minio_storage._client")
def test_download_to_tempfile_fetches_object_and_returns_path(mock_client):
    def fake_fget(bucket, object_path, tmp_path):
        Path(tmp_path).write_bytes(b"%PDF-1.4 fake content")

    mock_client.fget_object.side_effect = fake_fget

    result = minio_storage.download_to_tempfile("some/object.pdf")

    assert result.exists()
    assert result.suffix == ".pdf"
    assert result.read_bytes().startswith(b"%PDF")
    result.unlink()
