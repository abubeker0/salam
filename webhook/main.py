import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

import webhook.config as config
import webhook.handlers as handlers

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
dp.include_router(handlers.router)

app = FastAPI()

@app.on_event("startup")
async def on_startup():
    logging.basicConfig(level=logging.INFO)

    await handlers.create_tables()
    await handlers.set_commands(bot)
    await bot.set_webhook(f"{config.WEBHOOK_URL}/webhook")
    print("Webhook set successfully")

@app.get("/")
async def root():
    return {"status": "Webhook is live"}

@app.post("/webhook")
async def telegram_webhook(req: Request):
    try:
        update = await req.json()
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        logging.error(f"Webhook error: {e}")
    return JSONResponse(status_code=status.HTTP_200_OK, content={"ok": True})
