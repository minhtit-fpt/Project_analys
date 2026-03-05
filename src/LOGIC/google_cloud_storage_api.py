"""
Google Cloud Storage API Handler
Responsible for all Google Cloud Storage interactions using service account authentication.

Drop-in replacement for the former GoogleDriveAPI class.
Exposes the same public interface (list_files, upload_file, delete_file,
download_file) so that callers (SaveData, ChartGenerator, GUI) require
only an import-path change.
"""

import os
import io
import logging
import tempfile
from typing import Dict, List, Optional

from google.cloud import storage
from google.oauth2 import service_account


class GoogleCloudStorageAPI:
    """
    Handles all Google Cloud Storage interactions.

    Responsibilities:
    - Service-account authentication
    - File upload, list, download, and delete operations
    - Preserves the same method signatures as the old GoogleDriveAPI

    Environment variables used:
        GCS_BUCKET_NAME          – target bucket name (required)
        GCS_FOLDER_PREFIX        – virtual folder prefix inside the bucket
                                   (optional, default: "")
        GOOGLE_APPLICATION_CREDENTIALS – path to the service-account JSON key
                                         (standard GCP variable, required)
    """

    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the Google Cloud Storage API handler.

        Args:
            logger: Optional logger instance. If not provided, creates a new one.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.Bucket] = None
        self.folder_prefix: str = ""

        # Initialize the service
        self._initialize()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize(self):
        """
        Initialize GCS client and bucket reference.

        Reads configuration from environment variables and authenticates
        using the service-account JSON key pointed to by
        GOOGLE_APPLICATION_CREDENTIALS.
        """
        try:
            bucket_name = os.getenv("GCS_BUCKET_NAME")
            if not bucket_name:
                raise ValueError(
                    "GCS_BUCKET_NAME environment variable is not set"
                )

            # Optional virtual folder prefix (e.g. "binance_data/")
            self.folder_prefix = os.getenv("GCS_FOLDER_PREFIX", "").strip()
            if self.folder_prefix and not self.folder_prefix.endswith("/"):
                self.folder_prefix += "/"

            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path
                )
                self.client = storage.Client(credentials=credentials)
            else:
                # Fall back to Application Default Credentials
                self.client = storage.Client()

            self.bucket = self.client.bucket(bucket_name)

            # Quick connectivity check using object-level permission
            # (does NOT require storage.buckets.get, only storage.objects.list)
            try:
                next(self.client.list_blobs(self.bucket, max_results=1).__iter__(), None)
            except Exception as check_err:
                raise ValueError(
                    f"Cannot access bucket '{bucket_name}'. "
                    f"Ensure the service account has 'Storage Object Admin' role. "
                    f"Detail: {check_err}"
                )

            self.logger.info(
                f"GCS service initialized successfully "
                f"(bucket={bucket_name}, prefix='{self.folder_prefix}')"
            )

        except Exception as e:
            self.logger.error(f"Error initializing GCS service: {e}")
            raise

    # ------------------------------------------------------------------
    # Public API – same signatures as the former GoogleDriveAPI
    # ------------------------------------------------------------------

    def list_files(self, name_pattern: str = None) -> List[Dict]:
        """
        List blobs (files) in the configured GCS bucket/prefix.

        Args:
            name_pattern: Optional substring to filter blob names.

        Returns:
            List of dicts with 'id' (blob name) and 'name' (file name only)
            matching the shape returned by the old GoogleDriveAPI.
        """
        try:
            blobs = self.client.list_blobs(
                self.bucket, prefix=self.folder_prefix
            )

            results: List[Dict] = []
            for blob in blobs:
                # Extract just the filename (strip folder prefix)
                file_name = blob.name
                if self.folder_prefix and file_name.startswith(self.folder_prefix):
                    file_name = file_name[len(self.folder_prefix):]

                # Skip "directory" markers
                if not file_name:
                    continue

                if name_pattern and name_pattern not in file_name:
                    continue

                results.append({"id": blob.name, "name": file_name})

            return results

        except Exception as e:
            self.logger.error(f"Error listing files from GCS: {e}")
            return []

    def delete_file(self, file_id: str) -> bool:
        """
        Delete a blob from Google Cloud Storage.

        Args:
            file_id: The full blob name (used as ID) to delete.

        Returns:
            True if successful, False otherwise.
        """
        try:
            blob = self.bucket.blob(file_id)
            blob.delete()
            return True
        except Exception as e:
            self.logger.error(f"Error deleting blob {file_id} from GCS: {e}")
            return False

    def upload_file(self, file_path: str, filename: str) -> Optional[str]:
        """
        Upload a local file to Google Cloud Storage.

        Args:
            file_path: Local path to the file to upload.
            filename:  Destination name inside the bucket (prefix is prepended).

        Returns:
            Blob name (acts as the file ID) if successful, None otherwise.
        """
        try:
            self.logger.info(f"Preparing to upload: {filename}")

            if not os.path.exists(file_path):
                self.logger.error(f"Local file does not exist: {file_path}")
                return None

            file_size = os.path.getsize(file_path)
            self.logger.info(f"File size: {file_size / 1024 / 1024:.2f} MB")

            blob_name = f"{self.folder_prefix}{filename}"
            blob = self.bucket.blob(blob_name)

            # Upload (overwrites if the blob already exists)
            blob.upload_from_filename(file_path)

            self.logger.info(
                f"✓ Uploaded file: {filename} (blob: {blob_name})"
            )
            return blob_name

        except Exception as e:
            self.logger.error(f"Error uploading file to GCS: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None

    def download_file(self, file_id: str) -> Optional[str]:
        """
        Download a blob from GCS to a temporary local path.

        Args:
            file_id: The full blob name (used as ID) to download.

        Returns:
            Local file path if successful, None otherwise.
        """
        try:
            blob = self.bucket.blob(file_id)

            # Create a temp file
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
            os.close(tmp_fd)

            blob.download_to_filename(tmp_path)

            self.logger.info(f"  Downloaded blob to: {tmp_path}")
            return tmp_path

        except Exception as e:
            self.logger.error(f"Error downloading blob {file_id}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
