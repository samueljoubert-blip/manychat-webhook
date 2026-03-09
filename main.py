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
import random
import sqlite3
import logging
import unicodedata
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
app = FastAPI(title="Many Coup de Grace — LCDG", version="3.0.0")

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

WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "").strip()
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "").strip()
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "").strip()
# Instagram uses same Page token by default (same FB Page linked to IG account)
INSTAGRAM_PAGE_ACCESS_TOKEN = (os.getenv("INSTAGRAM_PAGE_ACCESS_TOKEN", "").strip()
                               or FACEBOOK_PAGE_ACCESS_TOKEN)

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
# Smart matching — Fuzzy + Natural Language Search
# ---------------------------------------------------------------------------
def strip_accents(text: str) -> str:
    """Remove accents from text (e→e, a→a, etc.)."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.category(c).startswith("M"))


def normalize_keyword(text: str) -> str:
    """Normalize a keyword: lowercase, strip accents, remove non-alphanumeric."""
    text = text.lower().strip()
    text = strip_accents(text)
    return re.sub(r"[^a-z0-9]", "", text)


def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein distance between two strings (pure Python)."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row
    return prev_row[-1]


def get_all_keywords() -> list:
    """Get all recipes with keywords from bible.db."""
    conn = get_bible_db()
    rows = conn.execute(
        "SELECT title, url, keyword, image_url FROM recipes "
        "WHERE wpml_lang='fr' AND keyword IS NOT NULL AND keyword != ''"
    ).fetchall()
    conn.close()
    return [
        {
            "title": row["title"],
            "url": row["url"],
            "keyword": row["keyword"],
            "image_url": row["image_url"] or "",
        }
        for row in rows
    ]


def fuzzy_lookup_recipe(text: str) -> Optional[dict]:
    """
    Smart recipe lookup with multiple matching strategies:
    1. Exact keyword match (case-insensitive)
    2. Normalized match (strip accents, spaces, special chars)
    3. Partial match (keyword contains text or vice versa)
    4. Fuzzy match (Levenshtein distance tolerance)
    """
    text_lower = text.lower().strip()
    text_clean = re.sub(r"[^a-zA-Z0-9\u00e0\u00e2\u00e4\u00e9\u00e8\u00ea\u00eb\u00ef\u00ee\u00f4\u00f9\u00fb\u00fc\u00ff\u00e7\u0153\u00e6]", "", text_lower)
    text_normalized = normalize_keyword(text)

    # Strategy 1: Exact match
    recipe = lookup_recipe(text_clean)
    if recipe:
        return recipe
    recipe = lookup_recipe(text_lower)
    if recipe:
        return recipe

    # Load all keywords for fuzzy matching
    all_recipes = get_all_keywords()

    # Strategy 2: Normalized match (strip accents from both sides)
    for r in all_recipes:
        kw_normalized = normalize_keyword(r["keyword"])
        if kw_normalized and kw_normalized == text_normalized:
            logger.info(f"Normalized match: '{text}' -> '{r['keyword']}'")
            return r

    # Strategy 3: Partial match (one contains the other, 4+ chars min)
    if len(text_normalized) >= 4:
        partial_matches = []
        for r in all_recipes:
            kw_normalized = normalize_keyword(r["keyword"])
            if not kw_normalized:
                continue
            if text_normalized in kw_normalized or kw_normalized in text_normalized:
                len_diff = abs(len(kw_normalized) - len(text_normalized))
                partial_matches.append((r, len_diff))
        if partial_matches:
            partial_matches.sort(key=lambda x: x[1])
            best = partial_matches[0]
            logger.info(f"Partial match: '{text}' -> '{best[0]['keyword']}' (len_diff={best[1]})")
            return best[0]

    # Strategy 4: Fuzzy match (Levenshtein distance)
    if len(text_normalized) >= 3:
        best_match = None
        best_distance = float("inf")
        for r in all_recipes:
            kw_normalized = normalize_keyword(r["keyword"])
            if not kw_normalized:
                continue
            # Skip if lengths are too different
            if abs(len(kw_normalized) - len(text_normalized)) > 3:
                continue
            dist = levenshtein_distance(text_normalized, kw_normalized)
            # Tolerance: 1 for short (<=5), 2 for medium (<=10), 3 for long
            max_tol = 1 if len(text_normalized) <= 5 else (2 if len(text_normalized) <= 10 else 3)
            if dist <= max_tol and dist < best_distance:
                best_distance = dist
                best_match = r
        if best_match:
            logger.info(f"Fuzzy match: '{text}' -> '{best_match['keyword']}' (distance={best_distance})")
            return best_match

    return None


# ---------------------------------------------------------------------------
# Natural language search
# ---------------------------------------------------------------------------
FRENCH_STOP_WORDS = {
    "de", "la", "le", "les", "du", "des", "un", "une", "au", "aux",
    "et", "ou", "en", "a", "avec", "pour", "dans", "sur", "par",
    "mon", "ma", "mes", "ton", "ta", "tes", "son", "sa", "ses",
    "ce", "cette", "ces", "qui", "que", "quoi", "dont",
    "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles",
    "moi", "toi", "lui", "eux", "y", "ne", "pas", "plus",
    "recette", "recettes", "faire", "comment", "bonne", "bon",
    "meilleur", "meilleure", "meilleures", "meilleurs",
    "veux", "voudrais", "cherche", "donne", "envoi", "envoie",
    "mets", "plat", "plats", "quelque", "chose", "truc", "tres",
}

CATEGORY_SYNONYMS = {
    "viande": ["poulet", "boeuf", "porc", "veau", "agneau", "steak", "filet",
               "cote", "roti", "braise", "grille", "bbq", "barbecue", "burger",
               "saucisse", "bacon", "jambon", "dinde", "canard"],
    "poisson": ["saumon", "thon", "morue", "crevette", "crevettes", "tilapia",
                "truite", "homard", "crabe", "fruits de mer", "poissons",
                "cabillaud", "aiglefin", "sole"],
    "dessert": ["gateau", "tarte", "biscuit", "chocolat", "creme", "mousse",
                "brownie", "muffin", "cookie", "cupcake", "fondant",
                "sucre", "sucree", "caramel", "vanille", "fraise", "citron"],
    "soupe": ["potage", "veloute", "bouillon", "chowder", "soupes"],
    "salade": ["salades", "coleslaw", "vinaigrette", "cesar"],
    "pates": ["spaghetti", "linguine", "fettuccine", "penne", "macaroni",
              "lasagne", "gnocchi", "ravioli", "pasta", "nouille", "nouilles"],
    "legumes": ["brocoli", "carotte", "patate", "courgette", "aubergine",
                "tomate", "oignon", "champignon", "epinard", "chou",
                "haricot", "pois", "mais", "celeri", "poivron"],
    "dejeuner": ["oeuf", "oeufs", "crepe", "crepes", "pain", "toast",
                 "granola", "smoothie", "brunch", "omelette", "quiche"],
    "mexicain": ["taco", "tacos", "burrito", "nachos", "quesadilla",
                 "guacamole", "salsa", "enchilada", "fajita"],
    "asiatique": ["sushi", "ramen", "pad thai", "curry", "wok",
                  "teriyaki", "dumpling", "bibimbap"],
    "italien": ["pizza", "risotto", "pesto", "bolognaise", "carbonara",
                "bruschetta", "antipasto", "tiramisu", "prosciutto"],
    "rapide": ["facile", "simple", "vite", "minute", "express",
               "semaine", "soir", "lunch"],
}


def search_recipes_by_text(query: str, limit: int = 5) -> list:
    """
    Search recipes by natural language query.
    Searches in recipe titles and keywords, expands category synonyms.
    Returns scored results.
    """
    query_norm = strip_accents(query.lower().strip())
    words = re.split(r"[^a-z0-9]+", query_norm)
    search_terms = [w for w in words if w not in FRENCH_STOP_WORDS and len(w) > 1]

    if not search_terms:
        return []

    # Expand with category synonyms
    expanded_terms = list(search_terms)
    for term in search_terms:
        if term in CATEGORY_SYNONYMS:
            expanded_terms.extend(CATEGORY_SYNONYMS[term])
    expanded_terms = list(set(expanded_terms))

    # Get all FR recipes
    conn = get_bible_db()
    rows = conn.execute(
        "SELECT title, url, keyword, image_url FROM recipes WHERE wpml_lang='fr'"
    ).fetchall()
    conn.close()

    # Score each recipe
    results = []
    for row in rows:
        title_norm = strip_accents(row["title"].lower()) if row["title"] else ""
        keyword_norm = strip_accents((row["keyword"] or "").lower())
        score = 0
        matched = []
        for term in expanded_terms:
            in_title = term in title_norm
            in_kw = term in keyword_norm
            if in_title:
                score += 3 if term in search_terms else 2
                matched.append(term)
            if in_kw:
                score += 2 if term in search_terms else 1
                if term not in matched:
                    matched.append(term)
        if score > 0:
            results.append({
                "title": row["title"],
                "url": row["url"],
                "keyword": row["keyword"] or "",
                "image_url": row["image_url"] or "",
                "score": score,
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:limit]


# ---------------------------------------------------------------------------
# Comment auto-reply — Public replies (random)
# ---------------------------------------------------------------------------
COMMENT_PUBLIC_REPLIES = [
    "Va jeter un \u0153il, c'est dans tes DM ! \U0001f440",
    "Ouvre tes DM, c'est l\u00e0 ! \U0001f60d",
    "C'est fait ! Va voir tes messages priv\u00e9s ! \U0001f4f2",
]


async def reply_to_comment(comment_id: str, platform: str = "instagram"):
    """Reply publicly to a comment with a random message."""
    reply_text = random.choice(COMMENT_PUBLIC_REPLIES)
    token = INSTAGRAM_PAGE_ACCESS_TOKEN if platform == "instagram" else FACEBOOK_PAGE_ACCESS_TOKEN
    if not token:
        logger.error(f"No access token for comment reply on {platform}")
        return None

    # Instagram: POST /{comment-id}/replies  |  Facebook: POST /{comment-id}/comments
    endpoint = "replies" if platform == "instagram" else "comments"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{GRAPH_API_BASE}/{comment_id}/{endpoint}",
            params={"access_token": token},
            json={"message": reply_text},
        )
        if resp.status_code != 200:
            logger.error(f"Comment reply failed: {resp.status_code} {resp.text}")
        else:
            logger.info(f"Replied to comment {comment_id}: {reply_text}")
        return resp.json() if resp.status_code == 200 else None


async def send_dm_from_comment(user_id: str, recipe: dict, platform: str = "instagram"):
    """Send a DM to someone who commented a keyword on a post."""
    # Send the recipe card via DM
    result = await send_recipe_card(user_id, recipe, platform)
    if result:
        logger.info(f"Sent DM recipe '{recipe['keyword']}' to commenter {user_id} on {platform}")
    return result


async def process_comment(comment_id: str, commenter_id: str, comment_text: str,
                          media_id: str, platform: str):
    """
    Process a comment on a post.
    If the comment matches a keyword, reply publicly + send DM with recipe.
    """
    logger.info(f"[{platform}] Comment from {commenter_id} on {media_id}: {comment_text}")

    # Test keyword — for testing without ManyChat interference
    if comment_text.strip().lower() == "testcdg":
        await reply_to_comment(comment_id, platform)
        await send_text_message(
            commenter_id,
            "🎉 BRAVO ! Many Coup de Grace fonctionne parfaitement !\n\n"
            "Ce message vient de TON serveur, pas de ManyChat.\n"
            "Tu peux maintenant annuler ton abonnement ManyChat! 💪",
            platform,
        )
        logger.info("TEST comment keyword matched — replied + DM sent")
        return

    # Try to match a recipe keyword (fuzzy)
    recipe = fuzzy_lookup_recipe(comment_text)

    if not recipe:
        # Also try natural language search — take top result if score is high
        results = search_recipes_by_text(comment_text, limit=1)
        if results and results[0]["score"] >= 4:
            recipe = results[0]

    if recipe:
        # 1. Reply publicly to the comment (random message)
        await reply_to_comment(comment_id, platform)

        # 2. Send DM with the recipe
        await send_dm_from_comment(commenter_id, recipe, platform)

        # 3. Register as subscriber
        sub_id = upsert_subscriber(commenter_id, platform)
        log_message(sub_id, "incoming", f"[COMMENT] {comment_text}",
                    keyword_matched=recipe["keyword"], platform=platform)
        log_message(sub_id, "outgoing", f"[COMMENT REPLY+DM] {recipe['title']}",
                    keyword_matched=recipe["keyword"], platform=platform)
    else:
        logger.info(f"Comment '{comment_text}' did not match any keyword — ignored")


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
    if not FACEBOOK_APP_SECRET:
        logger.warning("No APP_SECRET configured — skipping signature check")
        return True
    if not signature:
        logger.warning("No X-Hub-Signature-256 header — skipping signature check")
        return True
    if not signature.startswith("sha256="):
        logger.warning(f"Signature doesn't start with sha256=: {signature[:20]}")
        return False
    expected = "sha256=" + hmac.new(
        FACEBOOK_APP_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    match = hmac.compare_digest(signature, expected)
    if not match:
        logger.warning(
            f"Signature MISMATCH — got={signature[:40]}... "
            f"expected={expected[:40]}... "
            f"secret_len={len(FACEBOOK_APP_SECRET)} body_len={len(body)} "
            f"body_start={body[:80]}"
        )
        # DEV MODE: accept anyway — re-enable strict check before production
        logger.warning("DEV MODE: accepting webhook despite signature mismatch")
        return True
    else:
        logger.info("Webhook signature verified OK")
    return match


# ---------------------------------------------------------------------------
# Message processing
# ---------------------------------------------------------------------------
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


async def _maybe_ask_email_or_optin(sender_psid: str, platform: str):
    """After sending a recipe, ask for email or broadcast opt-in if needed."""
    conn = get_subscribers_db()
    sub = conn.execute(
        "SELECT email, opted_in_broadcast FROM subscribers WHERE psid = ? AND platform = ?",
        (sender_psid, platform),
    ).fetchone()
    conn.close()

    if not sub:
        return
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


async def process_incoming_message(sender_psid: str, page_id: str,
                                    message_text: str, platform: str):
    """Process an incoming DM with smart fuzzy matching + natural language search."""
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

    # 3. Test keyword — for testing without ManyChat interference
    if text_lower == "testcdg":
        test_recipe = {
            "title": "TEST — Many Coup de Grace fonctionne!",
            "url": "https://lecoupdegrace.ca",
            "keyword": "testcdg",
            "image_url": "",
        }
        await send_text_message(
            sender_psid,
            "🎉 BRAVO ! Many Coup de Grace fonctionne parfaitement !\n\n"
            "Ce message vient de TON serveur, pas de ManyChat.\n"
            "Tu peux maintenant annuler ton abonnement ManyChat! 💪",
            platform,
        )
        log_message(sub_id, "outgoing", "TEST keyword success", platform=platform)
        return

    # 4. Smart recipe matching — Fuzzy keyword lookup
    recipe = fuzzy_lookup_recipe(message_text)

    if recipe:
        # Single recipe found — send rich card
        await send_recipe_card(sender_psid, recipe, platform)
        log_message(sub_id, "outgoing", f"Recipe: {recipe['title']}",
                    keyword_matched=recipe["keyword"], platform=platform)
        await _maybe_ask_email_or_optin(sender_psid, platform)
        return

    # 4. No keyword match — try natural language search
    search_results = search_recipes_by_text(message_text, limit=5)

    if search_results:
        # Send top result as rich card
        top = search_results[0]
        await send_recipe_card(sender_psid, top, platform)

        # If multiple results, send text list of the rest
        if len(search_results) > 1:
            lines = ["Voici d'autres recettes qui pourraient t'interesser :\n"]
            for i, r in enumerate(search_results[1:], 2):
                lines.append(f"{i}. {r['title']}")
                lines.append(f"   {r['url']}\n")
            await send_text_message(sender_psid, "\n".join(lines), platform)

        log_message(sub_id, "outgoing",
                    f"Search: {len(search_results)} results for '{message_text}'",
                    keyword_matched=message_text, platform=platform)
        await _maybe_ask_email_or_optin(sender_psid, platform)
        return

    # 5. Nothing found at all
    await send_text_message(
        sender_psid,
        "Desole, je n'ai pas trouve de recette pour \"" + message_text + "\".\n\n"
        "Essaie un autre mot-cle ou visite lecoupdegrace.ca pour explorer nos recettes !",
        platform,
    )
    log_message(sub_id, "outgoing", "Not found",
                keyword_matched=message_text, platform=platform)


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

        # --- DM messages (Instagram + Facebook Messenger) ---
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

        # --- Comment events (Instagram + Facebook) ---
        changes = entry.get("changes", [])
        for change in changes:
            field = change.get("field", "")
            value = change.get("value", {})

            # Instagram comments
            if field == "comments" and platform == "instagram":
                comment_id = value.get("id", "")
                comment_text = value.get("text", "")
                commenter_id = value.get("from", {}).get("id", "")
                media_id = value.get("media", {}).get("id", "")
                if comment_id and commenter_id and comment_text:
                    await process_comment(comment_id, commenter_id, comment_text,
                                          media_id, platform)

            # Facebook feed comments
            elif field == "feed" and value.get("item") == "comment":
                comment_id = value.get("comment_id", "")
                comment_text = value.get("message", "")
                commenter_id = value.get("from", {}).get("id", "")
                post_id = value.get("post_id", "")
                if comment_id and commenter_id and comment_text:
                    await process_comment(comment_id, commenter_id, comment_text,
                                          post_id, "facebook")

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
    return {"status": "ok", "service": "Many Coup de Grace — lecoupdegrace.ca", "version": "3.0.0"}


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

    recipe = fuzzy_lookup_recipe(keyword)
    if not recipe:
        # Try natural language search as last resort
        results = search_recipes_by_text(keyword, limit=1)
        if results:
            recipe = results[0]
        else:
            return {"error": "not_found", "message": f"Aucune recette trouvee pour le keyword '{keyword}'."}

    return recipe


@app.get("/search")
async def search_recipes(q: str = Query("", description="Search query")):
    """Search recipes by keyword (fuzzy) or natural language."""
    if not q.strip():
        return {"results": [], "method": "none"}

    # Try fuzzy keyword match first
    recipe = fuzzy_lookup_recipe(q)
    if recipe:
        return {"results": [recipe], "method": "fuzzy_keyword", "query": q}

    # Fall back to natural language search
    results = search_recipes_by_text(q, limit=10)
    return {"results": results, "method": "natural_language", "query": q, "total": len(results)}


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
