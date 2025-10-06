import os, json, pathlib, datetime as dt
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

DATA_DIR = pathlib.Path("data")
LATEST = DATA_DIR / "latest.json"
if not LATEST.exists():
    raise SystemExit("latest.json not found (scraper may have failed)")

# Load credentials from GitHub Secrets
client_secret_json = json.loads(os.environ["GDRIVE_CLIENT_SECRET_JSON"])
token_json = json.loads(os.environ["GDRIVE_TOKEN_JSON"])

creds = Credentials.from_authorized_user_info(
    token_json, scopes=["https://www.googleapis.com/auth/drive.file"]
)
drive = build("drive", "v3", credentials=creds)
folder_id = os.environ["GDRIVE_FOLDER_ID"]

# Generate archive file name
try:
    scraped_at = json.loads(LATEST.read_text()).get("scraped_at_utc")
except Exception:
    scraped_at = None
date_str = (scraped_at or dt.datetime.utcnow().strftime("%Y-%m-%d")).split("T")[0]
archive_name = f"reddit_top_week_{date_str}.json"

def find_file(name, parent_id):
    q = f"name = '{name}' and '{parent_id}' in parents and trashed = false"
    res = drive.files().list(
        q=q,
        fields="files(id,name)",
        supportsAllDrives=True
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def upload_or_update(local_path, name, parent_id):
    file_id = find_file(name, parent_id)
    media = MediaFileUpload(str(local_path), mimetype="application/json", resumable=False)
    if file_id:
        drive.files().update(fileId=file_id, media_body=media).execute()
        print(f"Updated {name}")
    else:
        drive.files().create(
            body={"name": name, "parents": [parent_id]},
            media_body=media,
            fields="id"
        ).execute()
        print(f"Uploaded {name}")

# Upload latest.json and archive
upload_or_update(LATEST, "latest.json", folder_id)
if not find_file(archive_name, folder_id):
    upload_or_update(LATEST, archive_name, folder_id)
else:
    print(f"Archive {archive_name} already exists; skipping.")
