"""
Abstração de acesso ao Azure Blob Storage com retry exponencial.

Dois clients:
  focus_client()    → FOCUS_STORAGE_ACCOUNT / FOCUS_CONTAINER    (leitura)
  pipeline_client() → PIPELINE_STORAGE_ACCOUNT / PIPELINE_CONTAINER (leitura e escrita)
"""

import io
import logging
import os
import re
import time

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

_RETRIES = 3
_RETRY_DELAY_SEC = 2.0


def _service_client(account: str) -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )


def _retry(fn):
    last_exc: Exception | None = None
    for attempt in range(1, _RETRIES + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < _RETRIES:
                wait = _RETRY_DELAY_SEC * (2 ** (attempt - 1))
                logger.warning("⚠ tentativa %d/%d falhou: %s — aguardando %.1fs", attempt, _RETRIES, exc, wait)
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


class StorageClient:
    def __init__(self, account: str, container: str) -> None:
        self.account = account
        self.container = container
        self._svc = _service_client(account)

    def _container(self):
        return self._svc.get_container_client(self.container)

    # ------------------------------------------------------------------
    # Raw bytes
    # ------------------------------------------------------------------

    def download_bytes(self, blob_name: str) -> bytes:
        logger.info("⬇️  %s/%s/%s", self.account, self.container, blob_name)
        return _retry(lambda: self._container().download_blob(blob_name).readall())

    def upload_bytes(self, blob_name: str, data: bytes, overwrite: bool = True) -> None:
        logger.info("⬆️  %s/%s/%s (%d bytes)", self.account, self.container, blob_name, len(data))
        _retry(lambda: self._container().upload_blob(blob_name, data, overwrite=overwrite))

    def exists(self, blob_name: str) -> bool:
        return _retry(lambda: self._svc.get_blob_client(self.container, blob_name).exists())

    def list_blobs(self, prefix: str) -> list[str]:
        return _retry(lambda: [b.name for b in self._container().list_blobs(name_starts_with=prefix)])

    # ------------------------------------------------------------------
    # CSV  (sep=";", encoding=UTF-8, float 6 decimais)
    # ------------------------------------------------------------------

    def read_csv(self, blob_name: str, **kwargs) -> pd.DataFrame:
        raw = self.download_bytes(blob_name)
        return pd.read_csv(io.BytesIO(raw), sep=";", encoding="utf-8", **kwargs)

    def write_csv(self, blob_name: str, df: pd.DataFrame) -> None:
        buf = io.StringIO()
        df.to_csv(buf, sep=";", index=False, encoding="utf-8", float_format="%.6f")
        self.upload_bytes(blob_name, buf.getvalue().encode("utf-8"))

    # ------------------------------------------------------------------
    # Parquet
    # ------------------------------------------------------------------

    def read_parquet(self, blob_name: str, **kwargs) -> pd.DataFrame:
        raw = self.download_bytes(blob_name)
        return pd.read_parquet(io.BytesIO(raw), **kwargs)

    def write_parquet(self, blob_name: str, df: pd.DataFrame) -> None:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self.upload_bytes(blob_name, buf.getvalue())


# ------------------------------------------------------------------
# Factories
# ------------------------------------------------------------------

def focus_client() -> StorageClient:
    """Read-only client para o storage do FOCUS Report."""
    return StorageClient(
        account=os.environ["FOCUS_STORAGE_ACCOUNT"],
        container=os.environ["FOCUS_CONTAINER"],
    )


def pipeline_client() -> StorageClient:
    """Read/write client para o storage do pipeline (finops-v2)."""
    return StorageClient(
        account=os.environ["PIPELINE_STORAGE_ACCOUNT"],
        container=os.environ["PIPELINE_CONTAINER"],
    )


def upload_parquet(
    df: pd.DataFrame,
    storage_account: str,
    container: str,
    blob_path: str,
) -> None:
    """Faz upload de um DataFrame como parquet snappy para o Blob Storage."""
    svc = _service_client(storage_account)

    def _do():
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
        buf.seek(0)
        svc.get_blob_client(container=container, blob=blob_path).upload_blob(buf, overwrite=True)
        logger.info("⬆️  %s/%s/%s (%d linhas)", storage_account, container, blob_path, len(df))

    _retry(_do)


def download_csv_with_fallback(
    account: str,
    container: str,
    prefix: str,
    date: str,
    pattern: str,
) -> tuple[pd.DataFrame, str, str]:
    """List {prefix}/dt={date}/, return first blob matching pattern as CSV.

    Falls back to any .parquet in the same folder if no CSV match is found.
    Returns (df, blob_path, file_type) where file_type is "csv", "parquet", or "empty".
    """
    store = StorageClient(account, container)
    partition_prefix = f"{prefix}/dt={date}/"
    blobs = store.list_blobs(partition_prefix)

    csv_match = next(
        (b for b in blobs if re.search(pattern, b.split("/")[-1], re.IGNORECASE)),
        None,
    )
    if csv_match:
        return store.read_csv(csv_match), csv_match, "csv"

    parquet_match = next((b for b in blobs if b.endswith(".parquet")), None)
    if parquet_match:
        return store.read_parquet(parquet_match), parquet_match, "parquet"

    logger.warning("⚠ Nenhum arquivo encontrado em %s/%s/%s", account, container, partition_prefix)
    return pd.DataFrame(), "", "empty"


def upload_csv(
    df: pd.DataFrame,
    storage_account: str,
    container: str,
    blob_path: str,
) -> None:
    """Faz upload de um DataFrame como CSV (sep=';', UTF-8, float 6 decimais) para o Blob Storage."""
    svc = _service_client(storage_account)

    def _do():
        buf = io.StringIO()
        df.to_csv(buf, sep=";", index=False, encoding="utf-8", float_format="%.6f")
        data = buf.getvalue().encode("utf-8")
        svc.get_blob_client(container=container, blob=blob_path).upload_blob(
            io.BytesIO(data), overwrite=True, length=len(data)
        )
        logger.info("⬆️  %s/%s/%s (%d bytes)", storage_account, container, blob_path, len(data))

    _retry(_do)
