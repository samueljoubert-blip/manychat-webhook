"""
Many Coup de Grace — lecoupdegrace.ca
Remplacement complet de ManyChat pour Instagram et Facebook Messenger.
Quand un abonne envoie un keyword en DM, on repond automatiquement
avec la recette (carte riche avec image + bouton).
"""

import os
import re
import hmac
import hashlib
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
app = FastAPI(title="Many Coup de Grace — LCDG", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("many-cdg")

DB_PATH = Path(os.getenv("BIBLE_DB_PATH", "./bible.db"))
SUBSCRIBERS_DB_PATH = Path(os.getenv("SUBSCRIBERS_DB_PATH", "./subscribers.db"))

WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "")
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "")
INSTAGRAM_PAGE_ACCESS_TOKEN = os.getenv("INSTAGRAM_PAGE_ACCESS_TOKEN", "")

GRAPH_API_VERSION = "v21.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_bible_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_subscribers_db():
    conn = sqlite3.connect(str(SUBSCRIBERS_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_subscribers_db():
    """Create subscribers tables if they don't exist."""
    conn = get_subscribers_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            psid TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'instagram',
            page_id TEXT,
            email TEXT,
            first_message_at TEXT,
            last_message_at TEXT,
            message_count INTEGER DEFAULT 0,
            opted_in_broadcast INTEGER DEFAULT 0,
            conversation_state TEXT DEFAULT 'idle',
            UNIQUE(psid, platform)
        );

        CREATE TABLE IF NOT EXISTS message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_id INTEGER,
            direction TEXT NOT NULL,
            message_text TEXT,
            keyword_matched TEXT,
            platform TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (subscriber_id) REFERENCES subscribers(id)
        );

        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_text TEXT NOT NULL,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()
    logger.info("Subscribers DB initialized")


@app.on_event("startup")
async def startup():
    init_subscribers_db()


# ---------------------------------------------------------------------------
# Recipe lookup
# ---------------------------------------------------------------------------
def lookup_recipe(keyword: str) -> Optional[dict]:
    """Look up a recipe by keyword in bible.db."""
    conn = get_bible_db()
    row = conn.execute(
        "SELECT title, url, keyword, image_url FROM recipes "
        "WHERE wpml_lang='fr' AND LOWER(keyword) = LOWER(?)",
        (keyword,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "title": row["title"],
        "url": row["url"],
        "keyword": row["keyword"],
        "image_url": row["image_url"] or "",
    }


# ---------------------------------------------------------------------------
# Subscriber management
# ---------------------------------------------------------------------------
def upsert_subscriber(psid: str, platform: str, page_id: str = "") -> int:
    """Create or update a subscriber. Returns subscriber ID."""
    now = datetime.now(timezone.utc).isoformat()
    conn = get_subscribers_db()
    existing = conn.execute(
        "SELECT id FROM subscribers WHERE psid = ? AND platform = ?",
        (psid, platform),
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE subscribers SET last_message_at = ?, message_count = message_count + 1, page_id = ? "
            "WHERE psid = ? AND platform = ?",
            (now, page_id, psid, platform),
        )
        conn.commit()
        sub_id = existing["id"]
    else:
        cur = conn.execute(
            "INSERT INTO subscribers (psid, platform, page_id, first_message_at, last_message_at, message_count) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (psid, platform, page_id, now, now),
        )
        conn.commit()
        sub_id = cur.lastrowid
    conn.close()
    return sub_id


def log_message(subscriber_id: int, direction: str, message_text: str,
                keyword_matched: str = "", platform: str = ""):
    """Log a message to the message_log table."""
    conn = get_subscribers_db()
    conn.execute(
        "INSERT INTO message_log (subscriber_id, direction, message_text, keyword_matched, platform) "
        "VALUES (?, ?, ?, ?, ?)",
        (subscriber_id, direction, message_text, keyword_matched, platform),
    )
    conn.commit()
    conn.close()


def set_conversation_state(psid: str, platform: str, state: str):
    """Set the conversation state for a subscriber."""
    conn = get_subscribers_db()
    conn.execute(
        "UPDATE subscribers SET conversation_state = ? WHERE psid = ? AND platform = ?",
        (state, psid, platform),
    )
    conn.commit()
    conn.close()


def get_conversation_state(psid: str, platform: str) -> str:
    """Get the conversation state for a subscriber."""
    conn = get_subscribers_db()
    row = conn.execute(
        "SELECT conversation_state FROM subscribers WHERE psid = ? AND platform = ?",
        (psid, platform),
    ).fetchone()
    conn.close()
    return row["conversation_state"] if row else "idle"


def save_email(psid: str, platform: str, email: str):
    """Save email for a subscriber."""
    conn = get_subscribers_db()
    conn.execute(
        "UPDATE subscribers SET email = ? WHERE psid = ? AND platform = ?",
        (email, psid, platform),
    )
    conn.commit()
    conn.close()


def set_broadcast_optin(psid: str, platform: str, opted_in: bool):
    """Set broadcast opt-in for a subscriber."""
    conn = get_subscribers_db()
    conn.execute(
        "UPDATE subscribers SET opted_in_broadcast = ? WHERE psid = ? AND platform = ?",
        (1 if opted_in else 0, psid, platform),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Meta Graph API — Send Messages
# ---------------------------------------------------------------------------
async def send_text_message(psid: str, text: str, platform: str = "instagram"):
    """Send a plain text message via Meta Send API."""
    token = INSTAGRAM_PAGE_ACCESS_TOKEN if platform == "instagram" else FACEBOOK_PAGE_ACCESS_TOKEN
    if not token:
        logger.error(f"No access token for platform {platform}")
        return None

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{GRAPH_API_BASE}/me/messages",
            params={"access_token": token},
            json={
                "recipient": {"id": psid},
                "messaging_type": "RESPONSE",
                "message": {"text": text},
            },
        )
        if resp.status_code != 200:
            logger.error(f"Send text failed: {resp.status_code} {resp.text}")
        else:
            logger.info(f"Sent text to {psid} on {platform}")
        return resp.json() if resp.status_code == 200 else None


async def send_recipe_card(psid: str, recipe: dict, platform: str = "instagram"):
    """Send a rich recipe card (Generic Template) via Meta Send API."""
    token = INSTAGRAM_PAGE_ACCESS_TOKEN if platform == "instagram" else FACEBOOK_PAGE_ACCESS_TOKEN
    if not token:
        logger.error(f"No access token for platform {platform}")
        return None

    message = {
        "attachment": {
            "type": "template",
            "payload": {
                "template_type": "generic",
                "elements": [
                    {
                        "title": recipe["title"],
                        "image_url": recipe["image_url"],
                        "default_action": {
                            "type": "web_url",
                            "url": recipe["url"],
                        },
                        "buttons": [
                            {
                                "type": "web_url",
                                "title": "Voir la recette",
                                "url": recipe["url"],
                            }
                        ],
                    }
                ],
            },
        }
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{GRAPH_API_BASE}/me/messages",
            params={"access_token": token},
            json={
                "recipient": {"id": psid},
                "messaging_type": "RESPONSE",
                "message": message,
            },
        )
        if resp.status_code != 200:
            logger.error(f"Send recipe card failed: {resp.status_code} {resp.text}")
            # Fallback to text if template fails (Instagram sometimes doesn't support templates)
            fallback_text = f"{recipe['title']}\n\n{recipe['url']}"
            return await send_text_message(psid, fallback_text, platform)
        else:
            logger.info(f"Sent recipe card '{recipe['keyword']}' to {psid} on {platform}")
        return resp.json() if resp.status_code == 200 else None


# ---------------------------------------------------------------------------
# Webhook signature verification
# ---------------------------------------------------------------------------
def verify_signature(body: bytes, signature: str) -> bool:
    """Verify X-Hub-Signature-256 from Meta."""
    if not FACEBOOK_APP_SECRET or not signature:
        return True  # Skip verification if no secret configured (dev mode)
    if not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        FACEBOOK_APP_SECRET.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


# ---------------------------------------------------------------------------
# Message processing
# ---------------------------------------------------------------------------
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


async def process_incoming_message(sender_psid: str, page_id: str,
                                    message_text: str, platform: str):
    """Process an incoming DM and respond accordingly."""
    logger.info(f"[{platform}] Message from {sender_psid}: {message_text}")

    # 1. Upsert subscriber
    sub_id = upsert_subscriber(sender_psid, platform, page_id)
    log_message(sub_id, "incoming", message_text, platform=platform)

    # 2. Check conversation state
    state = get_conversation_state(sender_psid, platform)
    text_lower = message_text.lower().strip()

    # Handle email collection state
    if state == "waiting_email":
        email_match = EMAIL_REGEX.search(message_text)
        if email_match:
            email = email_match.group()
            save_email(sender_psid, platform, email)
            set_conversation_state(sender_psid, platform, "waiting_optin")
            await send_text_message(
                sender_psid,
                f"Merci ! Ton courriel {email} a ete enregistre.\n\n"
                "Veux-tu aussi recevoir un message ici quand une nouvelle recette sort ? "
                "Reponds OUI ou NON !",
                platform,
            )
            log_message(sub_id, "outgoing", "Email saved + ask opt-in", platform=platform)
            return
        elif text_lower in ("non", "no", "skip", "passer"):
            set_conversation_state(sender_psid, platform, "waiting_optin")
            await send_text_message(
                sender_psid,
                "Pas de probleme !\n\n"
                "Veux-tu recevoir un message ici quand une nouvelle recette sort ? "
                "Reponds OUI ou NON !",
                platform,
            )
            log_message(sub_id, "outgoing", "Email skipped + ask opt-in", platform=platform)
            return
        # If not an email and not skip, treat as keyword (fall through)

    # Handle opt-in state
    if state == "waiting_optin":
        if text_lower in ("oui", "yes", "ok", "sure", "ya", "ouais"):
            set_broadcast_optin(sender_psid, platform, True)
            set_conversation_state(sender_psid, platform, "idle")
            await send_text_message(
                sender_psid,
                "Parfait ! Tu recevras un message quand une nouvelle recette sort.\n\n"
                "Envoie un keyword a tout moment pour recevoir une recette !",
                platform,
            )
            log_message(sub_id, "outgoing", "Opted in to broadcast", platform=platform)
            return
        elif text_lower in ("non", "no", "nah"):
            set_broadcast_optin(sender_psid, platform, False)
            set_conversation_state(sender_psid, platform, "idle")
            await send_text_message(
                sender_psid,
                "OK, pas de probleme ! Envoie un keyword a tout moment pour recevoir une recette.",
                platform,
            )
            log_message(sub_id, "outgoing", "Opted out of broadcast", platform=platform)
            return
        # If neither yes/no, treat as keyword (fall through)

    # 3. Try to match a recipe keyword
    # Clean the keyword (remove spaces, special chars for matching)
    keyword_clean = re.sub(r"[^a-zA-Z0-9àâäéèêëïîôùûüÿçœæ]", "", text_lower)
    recipe = lookup_recipe(keyword_clean)

    if not recipe:
        # Also try the raw text (some keywords might have special formatting)
        recipe = lookup_recipe(text_lower)

    if recipe:
        # Send recipe card
        await send_recipe_card(sender_psid, recipe, platform)
        log_message(sub_id, "outgoing", f"Recipe: {recipe['title']}", keyword_matched=recipe["keyword"], platform=platform)

        # Ask for email if we don't have one yet
        conn = get_subscribers_db()
        sub = conn.execute(
            "SELECT email, opted_in_broadcast FROM subscribers WHERE psid = ? AND platform = ?",
            (sender_psid, platform),
        ).fetchone()
        conn.close()

        if not sub["email"]:
            set_conversation_state(sender_psid, platform, "waiting_email")
            await send_text_message(
                sender_psid,
                "Tu veux recevoir nos meilleures recettes par courriel ? "
                "Envoie-moi ton email ! (ou reponds NON pour passer)",
                platform,
            )
        elif not sub["opted_in_broadcast"]:
            set_conversation_state(sender_psid, platform, "waiting_optin")
            await send_text_message(
                sender_psid,
                "Veux-tu recevoir un message quand une nouvelle recette sort ? "
                "Reponds OUI ou NON !",
                platform,
            )
    else:
        # No recipe found
        await send_text_message(
            sender_psid,
            "Desole, je n'ai pas trouve de recette pour ce mot-cle.\n\n"
            "Essaie un autre mot-cle ou visite lecoupdegrace.ca pour explorer nos recettes !",
            platform,
        )
        log_message(sub_id, "outgoing", "Not found", keyword_matched=message_text, platform=platform)


# ---------------------------------------------------------------------------
# Meta Webhook endpoints
# ---------------------------------------------------------------------------
@app.get("/webhook")
async def webhook_verify(
    request: Request,
):
    """
    Meta Webhook verification (GET).
    Meta sends hub.mode, hub.challenge, hub.verify_token as query params.
    We must echo back hub.challenge if the verify_token matches.
    """
    mode = request.query_params.get("hub.mode", "")
    challenge = request.query_params.get("hub.challenge", "")
    verify_token = request.query_params.get("hub.verify_token", "")

    if mode == "subscribe" and verify_token == WEBHOOK_VERIFY_TOKEN:
        logger.info("Webhook verified successfully!")
        return PlainTextResponse(content=challenge)

    logger.warning(f"Webhook verification failed: mode={mode}, token={verify_token}")
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhook")
async def webhook_receive(request: Request):
    """
    Meta Webhook message receiver (POST).
    Receives incoming messages from Instagram and Facebook Messenger.
    Must respond 200 OK within 20 seconds.
    """
    # Verify signature
    body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not verify_signature(body, signature):
        logger.warning("Invalid webhook signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    data = await request.json()
    logger.info(f"Webhook received: {data}")

    # Determine platform from the object field
    obj = data.get("object", "")
    platform = "instagram" if obj == "instagram" else "facebook"

    # Process each entry
    for entry in data.get("entry", []):
        # Instagram uses "messaging", Facebook uses "messaging" too
        messaging_events = entry.get("messaging", [])
        for event in messaging_events:
            sender_psid = event.get("sender", {}).get("id", "")
            page_id = event.get("recipient", {}).get("id", "")

            if not sender_psid:
                continue

            # Handle text messages
            if "message" in event and "text" in event["message"]:
                message_text = event["message"]["text"]
                # Don't respond to echoes (messages we sent)
                if event["message"].get("is_echo"):
                    continue
                await process_incoming_message(sender_psid, page_id, message_text, platform)

            # Handle postbacks (button clicks)
            elif "postback" in event:
                payload = event["postback"].get("payload", "")
                if payload:
                    await process_incoming_message(sender_psid, page_id, payload, platform)

    # Always return 200 OK quickly
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Broadcast endpoint
# ---------------------------------------------------------------------------
@app.post("/broadcast")
async def broadcast_message(request: Request):
    """
    Send a message to all opted-in subscribers who interacted in the last 24h.
    Body: {"message": "text...", "force": false}
    force=true sends to ALL opted-in (ignoring 24h window — may fail per Meta policy)
    """
    body = await request.json()
    message_text = body.get("message", "").strip()
    force = body.get("force", False)

    if not message_text:
        return {"error": "missing_message"}

    conn = get_subscribers_db()

    if force:
        rows = conn.execute(
            "SELECT psid, platform FROM subscribers WHERE opted_in_broadcast = 1"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT psid, platform FROM subscribers "
            "WHERE opted_in_broadcast = 1 "
            "AND last_message_at >= datetime('now', '-24 hours')"
        ).fetchall()

    # Log broadcast
    cur = conn.execute(
        "INSERT INTO broadcasts (message_text, sent_count) VALUES (?, 0)",
        (message_text,),
    )
    broadcast_id = cur.lastrowid
    conn.commit()
    conn.close()

    sent = 0
    failed = 0

    for row in rows:
        try:
            result = await send_text_message(row["psid"], message_text, row["platform"])
            if result:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Broadcast failed for {row['psid']}: {e}")
            failed += 1

    # Update broadcast stats
    conn = get_subscribers_db()
    conn.execute(
        "UPDATE broadcasts SET sent_count = ?, failed_count = ? WHERE id = ?",
        (sent, failed, broadcast_id),
    )
    conn.commit()
    conn.close()

    logger.info(f"Broadcast sent: {sent} success, {failed} failed")
    return {
        "broadcast_id": broadcast_id,
        "message": message_text,
        "sent": sent,
        "failed": failed,
        "total_targeted": len(rows),
    }


# ---------------------------------------------------------------------------
# Subscriber management endpoints
# ---------------------------------------------------------------------------
@app.get("/subscribers")
async def list_subscribers():
    """List all subscribers."""
    conn = get_subscribers_db()
    rows = conn.execute(
        "SELECT id, psid, platform, email, first_message_at, last_message_at, "
        "message_count, opted_in_broadcast, conversation_state "
        "FROM subscribers ORDER BY last_message_at DESC"
    ).fetchall()
    conn.close()
    return {
        "subscribers": [dict(r) for r in rows],
        "total": len(rows),
    }


@app.get("/subscribers/stats")
async def subscriber_stats():
    """Subscriber statistics."""
    conn = get_subscribers_db()
    total = conn.execute("SELECT COUNT(*) FROM subscribers").fetchone()[0]
    with_email = conn.execute("SELECT COUNT(*) FROM subscribers WHERE email IS NOT NULL AND email != ''").fetchone()[0]
    opted_in = conn.execute("SELECT COUNT(*) FROM subscribers WHERE opted_in_broadcast = 1").fetchone()[0]
    active_24h = conn.execute(
        "SELECT COUNT(*) FROM subscribers WHERE last_message_at >= datetime('now', '-24 hours')"
    ).fetchone()[0]
    total_messages = conn.execute("SELECT COUNT(*) FROM message_log").fetchone()[0]
    ig_count = conn.execute("SELECT COUNT(*) FROM subscribers WHERE platform = 'instagram'").fetchone()[0]
    fb_count = conn.execute("SELECT COUNT(*) FROM subscribers WHERE platform = 'facebook'").fetchone()[0]
    conn.close()
    return {
        "total_subscribers": total,
        "with_email": with_email,
        "opted_in_broadcast": opted_in,
        "active_24h": active_24h,
        "total_messages": total_messages,
        "by_platform": {"instagram": ig_count, "facebook": fb_count},
    }


@app.get("/subscribers/emails")
async def export_emails():
    """Export list of collected emails."""
    conn = get_subscribers_db()
    rows = conn.execute(
        "SELECT email, platform, first_message_at FROM subscribers "
        "WHERE email IS NOT NULL AND email != '' ORDER BY first_message_at DESC"
    ).fetchall()
    conn.close()
    return {
        "emails": [{"email": r["email"], "platform": r["platform"], "subscribed_at": r["first_message_at"]} for r in rows],
        "total": len(rows),
    }


@app.get("/broadcasts")
async def list_broadcasts():
    """List all broadcasts."""
    conn = get_subscribers_db()
    rows = conn.execute(
        "SELECT id, message_text, sent_count, failed_count, created_at "
        "FROM broadcasts ORDER BY created_at DESC LIMIT 50"
    ).fetchall()
    conn.close()
    return {"broadcasts": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# Original endpoints (keep for compatibility)
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok", "service": "Many Coup de Grace — lecoupdegrace.ca", "version": "2.0.0"}


@app.get("/health")
async def health():
    try:
        conn = get_bible_db()
        count = conn.execute("SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr'").fetchone()[0]
        conn.close()
        sub_conn = get_subscribers_db()
        sub_count = sub_conn.execute("SELECT COUNT(*) FROM subscribers").fetchone()[0]
        sub_conn.close()
        return {
            "status": "healthy",
            "recipes_fr": count,
            "subscribers": sub_count,
            "webhook_verify_token_set": bool(WEBHOOK_VERIFY_TOKEN),
            "instagram_token_set": bool(INSTAGRAM_PAGE_ACCESS_TOKEN),
            "facebook_token_set": bool(FACEBOOK_PAGE_ACCESS_TOKEN),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/manychat/webhook")
async def manychat_webhook(request: Request):
    """Legacy ManyChat External Request endpoint (kept for compatibility)."""
    keyword = ""
    try:
        body = await request.json()
        keyword = body.get("keyword", "").strip()
    except Exception:
        keyword = request.query_params.get("keyword", "").strip()

    if not keyword:
        return {"error": "missing_keyword", "message": "Le champ 'keyword' est requis."}

    recipe = lookup_recipe(keyword)
    if not recipe:
        return {"error": "not_found", "message": f"Aucune recette trouvee pour le keyword '{keyword}'."}

    return recipe


@app.get("/manychat/keywords")
async def manychat_keywords():
    """Liste tous les keywords (recettes FR)."""
    conn = get_bible_db()
    rows = conn.execute(
        """SELECT title, url, keyword, image_url
           FROM recipes
           WHERE wpml_lang = 'fr' AND keyword IS NOT NULL AND keyword != ''
           ORDER BY keyword COLLATE NOCASE"""
    ).fetchall()
    conn.close()
    return {
        "keywords": [
            {
                "keyword": r["keyword"],
                "title": r["title"],
                "url": r["url"],
                "image_url": r["image_url"] or "",
                "has_image": bool(r["image_url"] and r["image_url"].strip()),
            }
            for r in rows
        ],
        "total": len(rows),
    }


@app.get("/manychat/stats")
async def manychat_stats():
    """Statistiques de couverture des keywords."""
    conn = get_bible_db()
    total_fr = conn.execute("SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr'").fetchone()[0]
    with_kw = conn.execute(
        "SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr' AND keyword IS NOT NULL AND keyword != ''"
    ).fetchone()[0]
    without_kw = total_fr - with_kw
    with_img = conn.execute(
        "SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr' AND keyword IS NOT NULL AND keyword != '' AND image_url IS NOT NULL AND image_url != ''"
    ).fetchone()[0]
    without_img = with_kw - with_img
    dupes = conn.execute(
        """SELECT LOWER(keyword) as kw, COUNT(*) as cnt
           FROM recipes
           WHERE wpml_lang='fr' AND keyword IS NOT NULL AND keyword != ''
           GROUP BY LOWER(keyword) HAVING cnt > 1"""
    ).fetchall()
    conn.close()
    return {
        "total_fr": total_fr,
        "with_keyword": with_kw,
        "without_keyword": without_kw,
        "coverage_pct": round(with_kw / total_fr * 100, 1) if total_fr > 0 else 0,
        "with_image": with_img,
        "without_image": without_img,
        "duplicates": [{"keyword": d["kw"], "count": d["cnt"]} for d in dupes],
    }


@app.get("/manychat/missing")
async def manychat_missing():
    """Recettes FR sans keyword."""
    conn = get_bible_db()
    rows = conn.execute(
        """SELECT title, url, image_url
           FROM recipes
           WHERE wpml_lang = 'fr' AND (keyword IS NULL OR keyword = '')
           ORDER BY title COLLATE NOCASE"""
    ).fetchall()
    conn.close()
    return {
        "missing": [
            {"title": r["title"], "url": r["url"], "image_url": r["image_url"] or ""}
            for r in rows
        ],
        "total": len(rows),
    }
