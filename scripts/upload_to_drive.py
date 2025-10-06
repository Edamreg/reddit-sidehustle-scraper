# scripts/upload_to_drive.py
import os, json, pathlib, datetime as dt
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

DATA_DIR = pathlib.Path("data")
LATEST = DATA_DIR / "latest.json"
if not LATEST.exists():
    raise SystemExit("latest.json not found (scraper may have failed)")

# Build Drive client from service account key in env
sa_info = json.loads(os.environ["GCP_SA_KEY"])
scopes = ["https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
drive = build("drive", "v3", credentials=creds)
folder_id = os.environ["GDRIVE_FOLDER_ID"]

# Archive filename from scraped_at_utc or today
try:
    scraped_at = json.loads(LATEST.read_text()).get("scraped_at_utc")
except Exception:
    scraped_at = None
date_str = (scraped_at or dt.datetime.utcnow().strftime("%Y-%m-%d")).split("T")[0]
archive_name = f"reddit_top_week_{date_str}.json"

def find_file(name, parent_id):
    q = f"name = '{name.replace(\"'\",\"\\'\")}' and '{parent_id}' in parents and trashed = false"
    res = drive.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def upload_or_update(local_path, name, parent_id, mime="application/json"):
    file_id = find_file(name, parent_id)
    media = MediaFileUpload(str(local_path), mimetype=mime, resumable=False)
    if file_id:
        drive.files().update(fileId=file_id, media_body=media).execute()
        print(f"Updated {name}")
    else:
        drive.files().create(
            body={"name": name, "parents": [parent_id]},
            media_body=media,
            fields="id"
        ).execute()
        print(f"Created {name}")

# 1) Overwrite/create latest.json
upload_or_update(LATEST, "latest.json", folder_id)

# 2) Create dated archive if not present
if not find_file(archive_name, folder_id):
    media = MediaFileUpload(str(LATEST), mimetype="application/json", resumable=False)
    drive.files().create(
        body={"name": archive_name, "parents": [folder_id]},
        media_body=media,
        fields="id"
    ).execute()
    print(f"Archived {archive_name}")
else:
    print(f"Archive {archive_name} already exists; skipping")
