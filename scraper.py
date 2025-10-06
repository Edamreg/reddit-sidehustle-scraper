import os, time, json, pathlib, datetime as dt
import requests

# -------- Config --------
SUBS = [
    "sidehustle",
    "passive_income",
    "WorkOnline",
    "EntrepreneurRideAlong",
    "JustStart",
    "Entrepreneur",
    "SmallBusiness",
    "BeerMoney"
    "WorkOnline",
]
LIMIT_PER_SUB = int(os.getenv("LIMIT_PER_SUB", "10"))  # top posts per sub
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.7"))  # be polite

CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET"]
USER_AGENT = os.environ["USER_AGENT"]

OUT_DIR = pathlib.Path("data")
OUT_DIR.mkdir(exist_ok=True, parents=True)

def get_oauth_token():
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": USER_AGENT}
    r = requests.post("https://www.reddit.com/api/v1/access_token",
                      auth=auth, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def get_headers(token):
    return {"Authorization": f"bearer {token}", "User-Agent": USER_AGENT}

def fetch_top_posts(sub, headers, limit=10):
    url = f"https://oauth.reddit.com/r/{sub}/top"
    params = {"t": "week", "limit": str(limit)}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    posts = []
    for child in data.get("data", {}).get("children", []):
        p = child["data"]
        posts.append({
            "subreddit": sub,
            "id": p["id"],
            "title": p.get("title"),
            "author": p.get("author"),
            "score": p.get("score"),
            "num_comments": p.get("num_comments"),
            "created_utc": p.get("created_utc"),
            "permalink": "https://www.reddit.com" + p.get("permalink", ""),
            "url": p.get("url_overridden_by_dest") or p.get("url"),
            "selftext": p.get("selftext") or "",
        })
    return posts

def fetch_top_level_comments(post_id, headers, limit=60):
    # depth=1 for top-level only, sort=top for most useful first
    url = f"https://oauth.reddit.com/comments/{post_id}"
    params = {"limit": str(limit), "depth": "1", "sort": "top"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    comments = []
    if isinstance(payload, list) and len(payload) > 1:
        comment_listing = payload[1]
        for child in comment_listing.get("data", {}).get("children", []):
            if child.get("kind") != "t1":
                continue
            c = child["data"]
            comments.append({
                "id": c.get("id"),
                "author": c.get("author"),
                "score": c.get("score"),
                "body": c.get("body") or "",
                "permalink": "https://www.reddit.com" + c.get("permalink", "")
            })
    return comments

def main():
    token = get_oauth_token()
    headers = get_headers(token)

    scraped_at = dt.datetime.utcnow().isoformat() + "Z"
    results = {
        "scraped_at_utc": scraped_at,
        "window": "top?t=week (rolling 7 days)",
        "subs": SUBS,
        "posts": []
    }

    for sub in SUBS:
        posts = fetch_top_posts(sub, headers, limit=LIMIT_PER_SUB)
        for p in posts:
            time.sleep(SLEEP_BETWEEN_CALLS)
            try:
                comments = fetch_top_level_comments(p["id"], headers, limit=100)
            except Exception as e:
                comments = []
            p["top_level_comments"] = comments
            results["posts"].append(p)
        time.sleep(SLEEP_BETWEEN_CALLS)

    # write versioned file + latest
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    versioned = OUT_DIR / f"reddit_top_week_{today}.json"
    latest = OUT_DIR / "latest.json"
    with open(versioned, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(latest, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Wrote {versioned} and {latest} with {len(results['posts'])} posts.")

if __name__ == "__main__":
    main()
