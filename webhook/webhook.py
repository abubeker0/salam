from fastapi import FastAPI, Request
import asyncpg
import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

@app.post("/chapa/callback")
async def chapa_callback(request: Request):
    data = await request.json()

    if data.get("event") != "charge.success":
        return {"status": "ignored"}

    tx_ref = data["data"].get("tx_ref")
    if not tx_ref:
        return {"status": "invalid"}

    parts = tx_ref.split("_")  # Expected: vip_chapa_1m_userid_timestamp
    if len(parts) < 4:
        return {"status": "bad_tx_ref"}

    subscription_type = parts[2]
    user_id = int(parts[3])

    # Calculate expiry
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    if subscription_type == "1m":
        expires_at = now + timedelta(days=30)
    elif subscription_type == "6m":
        expires_at = now + timedelta(days=182)
    elif subscription_type == "1y":
        expires_at = now + timedelta(days=365)
    else:
        return {"status": "unknown_plan"}

    # Update database
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute(
        """
        INSERT INTO users (user_id, is_vip, vip_expires_at)
        VALUES ($1, TRUE, $2)
        ON CONFLICT (user_id) DO UPDATE SET
            is_vip = TRUE,
            vip_expires_at = EXCLUDED.vip_expires_at
        """,
        user_id, expires_at
    )
    await conn.close()

    return {"status": "success"}
