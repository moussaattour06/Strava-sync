import os
import asyncio
import logging
from datetime import datetime, timezone
import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from supabase import create_client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

STRAVA_CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
STRAVA_CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]
STRAVA_REFRESH_TOKEN = os.environ["STRAVA_REFRESH_TOKEN"]
STRAVA_VERIFY_TOKEN = os.environ["STRAVA_VERIFY_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()

_access_token = None
_token_expires_at = 0

async def get_access_token():
    global _access_token, _token_expires_at
    now = int(datetime.now(timezone.utc).timestamp())
    if _access_token and now < _token_expires_at - 60:
        return _access_token
    async with httpx.AsyncClient() as client:
        r = await client.post("https://www.strava.com/oauth/token", data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN,
        })
        r.raise_for_status()
        data = r.json()
        _access_token = data["access_token"]
        _token_expires_at = data["expires_at"]
        return _access_token

async def strava_get(path, params={}):
    token = await get_access_token()
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            "https://www.strava.com/api/v3" + path,
            headers={"Authorization": "Bearer " + token},
            params=params
        )
        r.raise_for_status()
        return r.json()

async def sync_activity(activity_id):
    log.info("Sync activite " + str(activity_id))
    try:
        a = await strava_get("/activities/" + str(activity_id), {"include_all_efforts": True})
    except Exception as e:
        log.error("Erreur: " + str(e))
        return
    supabase.table("activities").upsert({
        "id": a["id"],
        "name": a.get("name"),
        "sport_type": a.get("sport_type") or a.get("type"),
        "start_date": a.get("start_date"),
        "start_date_local": a.get("start_date_local"),
        "distance_m": a.get("distance"),
        "moving_time_s": a.get("moving_time"),
        "elapsed_time_s": a.get("elapsed_time"),
        "total_elevation_gain": a.get("total_elevation_gain"),
        "average_heartrate": a.get("average_heartrate"),
        "max_heartrate": a.get("max_heartrate"),
        "average_watts": a.get("average_watts"),
        "suffer_score": a.get("suffer_score"),
        "pr_count": a.get("pr_count", 0),
        "calories": a.get("calories"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }).execute()
    log.info("Sauvegarde: " + str(a.get("name")))

@app.get("/webhook")
async def webhook_challenge(hub_mode: str, hub_challenge: str, hub_verify_token: str):
    if hub_mode == "subscribe" and hub_verify_token == STRAVA_VERIFY_TOKEN:
        return {"hub.challenge": hub_challenge}
    return {"error": "invalid token"}

@app.post("/webhook")
async def webhook_receive(request: Request, background_tasks: BackgroundTasks):
    event = await request.json()
    if event.get("object_type") == "activity":
        if event.get("aspect_type") in ("create", "update"):
            background_tasks.add_task(sync_activity, event["object_id"])
        elif event.get("aspect_type") == "delete":
            supabase.table("activities").delete().eq("id", event["object_id"]).execute()
    return {"status": "ok"}

@app.post("/backfill")
async def backfill(background_tasks: BackgroundTasks, pages: int = 10):
    background_tasks.add_task(_run_backfill, pages)
    return {"status": "backfill demarre"}

async def _run_backfill(pages):
    for page in range(1, pages + 1):
        activities = await strava_get("/athlete/activities", {"per_page": 20, "page": page})
        if not activities:
            break
        for a in activities:
            await sync_activity(a["id"])
        await asyncio.sleep(1)

@app.get("/health")
async def health():
    return {"status": "ok"}
