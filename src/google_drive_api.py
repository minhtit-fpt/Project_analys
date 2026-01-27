"""
Google Drive API Handler
Responsible for all Google Drive interactions using OAuth 2.0 authentication.
"""

import os
import logging
from typing import Dict, List, Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


class GoogleDriveAPI:
    """
    Handles all Google Drive API interactions.
    
    Responsibilities:
    - OAuth 2.0 authentication
    - Access token and refresh token lifecycle management
    - Refresh token validation and renewal
    - File upload, list, and delete operations
    """
    
    # Google Drive API scopes
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the Google Drive API handler.
        
        Args:
            logger: Optional logger instance. If not provided, creates a new one.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.drive_service = None
        self.folder_id = None
        
        # Initialize the service
        self._initialize()
    
    def _initialize(self):
        """
        Initialize Google Drive API service using OAuth 2.0 authentication.
        Reads OAuth credentials and tokens from environment variables.
        Validates existing refresh token and re-authenticates if necessary.
        """
        try:
            # Get configuration from environment variables
            self.folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
            client_id = os.getenv('GOOGLE_CLIENT_ID')
            client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
            refresh_token = os.getenv('GOOGLE_REFRESH_TOKEN')
            
            # Validate required configuration
            if not self.folder_id:
                raise ValueError("GOOGLE_DRIVE_FOLDER_ID environment variable is not set")
            
            if not client_id or not client_secret:
                raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set in .env file")
            
            # Clean refresh token (remove quotes if present)
            if refresh_token:
                refresh_token = refresh_token.strip().strip("'\"")
                if not refresh_token:
                    refresh_token = None
            
            creds = None
            needs_reauth = False
            
            # Try to use existing refresh token
            if refresh_token:
                creds = self._validate_refresh_token(refresh_token, client_id, client_secret)
                if creds is None:
                    needs_reauth = True
            else:
                self.logger.info("No refresh token found, authentication required")
                needs_reauth = True
            
            # Perform OAuth flow if needed
            if creds is None or needs_reauth:
                creds = self._perform_oauth_flow(client_id, client_secret)
            
            # Build the Google Drive API service
            self.drive_service = build('drive', 'v3', credentials=creds)
            
            self.logger.info("Google Drive service initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing Google Drive service: {e}")
            raise
    
    def _validate_refresh_token(self, refresh_token: str, client_id: str, client_secret: str) -> Optional[Credentials]:
        """
        Validate an existing refresh token by attempting to use it.
        
        Args:
            refresh_token: The refresh token to validate
            client_id: OAuth client ID
            client_secret: OAuth client secret
            
        Returns:
            Valid Credentials object if successful, None otherwise
        """
        self.logger.info("Found existing refresh token, attempting to use it...")
        try:
            creds = Credentials(
                token=None,
                refresh_token=refresh_token,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=client_id,
                client_secret=client_secret,
                scopes=self.SCOPES
            )
            
            # Force refresh to validate the token
            creds.refresh(Request())
            self.logger.info("Successfully refreshed credentials using existing refresh token")
            
            # Validate by making a test API call
            test_service = build('drive', 'v3', credentials=creds)
            test_service.files().list(pageSize=1).execute()
            self.logger.info("Refresh token validated successfully")
            
            return creds
            
        except Exception as e:
            self.logger.warning(f"Existing refresh token is invalid or expired: {e}")
            self.logger.info("Will trigger re-authentication...")
            return None
    
    def _perform_oauth_flow(self, client_id: str, client_secret: str) -> Credentials:
        """
        Perform the OAuth 2.0 authentication flow.
        
        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret
            
        Returns:
            New Credentials object
        """
        self.logger.info("Starting OAuth 2.0 authentication flow...")
        print("\n" + "=" * 60)
        print("Google Drive Authentication Required")
        print("A browser window will open for you to log in.")
        print("=" * 60 + "\n")
        
        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost"]
                }
            },
            self.SCOPES
        )
        creds = flow.run_local_server(port=0)
        
        # Save the new refresh token to .env file
        if creds.refresh_token:
            self._save_refresh_token(creds.refresh_token)
            self.logger.info("New refresh token saved to .env file")
        else:
            self.logger.warning("No refresh token received from OAuth flow")
        
        return creds
    
    def _save_refresh_token(self, refresh_token: str):
        """
        Save the refresh token to the .env file.
        
        Args:
            refresh_token: The OAuth refresh token to save
        """
        try:
            # Find the .env file path
            env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
            
            # Read the current .env file
            with open(env_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Find and update the GOOGLE_REFRESH_TOKEN line
            token_found = False
            for i, line in enumerate(lines):
                if line.startswith('GOOGLE_REFRESH_TOKEN='):
                    lines[i] = f'GOOGLE_REFRESH_TOKEN={refresh_token}\n'
                    token_found = True
                    break
            
            # If not found, append it
            if not token_found:
                lines.append(f'\nGOOGLE_REFRESH_TOKEN={refresh_token}\n')
            
            # Write back to .env file
            with open(env_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            # Also update the environment variable in current session
            os.environ['GOOGLE_REFRESH_TOKEN'] = refresh_token
            
        except Exception as e:
            self.logger.error(f"Failed to save refresh token to .env file: {e}")
            raise
    
    def list_files(self, name_pattern: str = None) -> List[Dict]:
        """
        List files in the Google Drive folder.
        
        Args:
            name_pattern: Optional pattern to filter files by name
            
        Returns:
            List of file metadata dictionaries with 'id' and 'name'
        """
        try:
            query = f"'{self.folder_id}' in parents and trashed = false"
            
            if name_pattern:
                query += f" and name contains '{name_pattern}'"
            
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            return results.get('files', [])
            
        except Exception as e:
            self.logger.error(f"Error listing files from Google Drive: {e}")
            return []
    
    def delete_file(self, file_id: str) -> bool:
        """
        Delete a file from Google Drive.
        
        Args:
            file_id: The ID of the file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.drive_service.files().delete(fileId=file_id).execute()
            return True
        except Exception as e:
            self.logger.error(f"Error deleting file {file_id} from Google Drive: {e}")
            return False
    
    def upload_file(self, file_path: str, filename: str) -> Optional[str]:
        """
        Upload a file to Google Drive.
        
        Args:
            file_path: Local path to the file to upload
            filename: Name to give the file in Google Drive
            
        Returns:
            File ID if successful, None otherwise
        """
        try:
            file_metadata = {
                'name': filename,
                'parents': [self.folder_id]
            }
            
            # Determine MIME type based on file extension
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            
            media = MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=True
            )
            
            # Check if file with same name already exists
            existing_files = self.list_files(filename)
            for existing_file in existing_files:
                if existing_file['name'] == filename:
                    # Update existing file
                    updated_file = self.drive_service.files().update(
                        fileId=existing_file['id'],
                        media_body=media
                    ).execute()
                    self.logger.info(f"Updated existing file: {filename}")
                    return updated_file.get('id')
            
            # Create new file
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            self.logger.info(f"Uploaded new file: {filename}")
            return file.get('id')
            
        except Exception as e:
            self.logger.error(f"Error uploading file to Google Drive: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
