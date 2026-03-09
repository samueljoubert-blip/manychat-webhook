"""
ManyChat Webhook — lecoupdegrace.ca
Quand un abonné envoie un keyword sur Instagram, ManyChat appelle ce webhook
et reçoit la recette correspondante (titre, URL, image).
"""

import os
import sqlite3
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="ManyChat Webhook — LCDG", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = Path(os.getenv("BIBLE_DB_PATH", "./bible.db"))


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/")
async def root():
    return {"status": "ok", "service": "ManyChat Webhook — lecoupdegrace.ca"}


@app.get("/health")
async def health():
    try:
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr'").fetchone()[0]
        conn.close()
        return {"status": "healthy", "recipes_fr": count}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/manychat/webhook")
async def manychat_webhook(request: Request):
    """
    ManyChat External Request endpoint.
    Body: {"keyword": "NACHOS"} or query param ?keyword=NACHOS
    Returns: {title, url, image_url, keyword} or {error: "not_found"}
    """
    # Support both JSON body and query params
    keyword = ""
    try:
        body = await request.json()
        keyword = body.get("keyword", "").strip()
    except Exception:
        keyword = request.query_params.get("keyword", "").strip()

    if not keyword:
        return {"error": "missing_keyword", "message": "Le champ 'keyword' est requis."}

    conn = get_db()
    row = conn.execute(
        "SELECT title, url, keyword, image_url FROM recipes WHERE wpml_lang='fr' AND LOWER(keyword) = LOWER(?)",
        (keyword,),
    ).fetchone()
    conn.close()

    if not row:
        return {"error": "not_found", "message": f"Aucune recette trouvée pour le keyword '{keyword}'."}

    return {
        "title": row["title"],
        "url": row["url"],
        "keyword": row["keyword"],
        "image_url": row["image_url"] or "",
    }


@app.get("/manychat/keywords")
async def manychat_keywords():
    """Liste tous les keywords ManyChat (recettes FR)."""
    conn = get_db()
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
    conn = get_db()
    total_fr = conn.execute("SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr'").fetchone()[0]
    with_kw = conn.execute(
        "SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr' AND keyword IS NOT NULL AND keyword != ''"
    ).fetchone()[0]
    without_kw = total_fr - with_kw
    with_img = conn.execute(
        "SELECT COUNT(*) FROM recipes WHERE wpml_lang='fr' AND keyword IS NOT NULL AND keyword != '' AND image_url IS NOT NULL AND image_url != ''"
    ).fetchone()[0]
    without_img = with_kw - with_img

    # Check duplicates
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
    """Recettes FR sans keyword ManyChat."""
    conn = get_db()
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
