import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

import webhook.config as config
import webhook.handlers as handlers
from webhook.localization import setup_i18n
from webhook.handlers import create_pool, db_pool  # ✅ Import your pool creation

logger = logging.getLogger(__name__)


# --- Periodic VIP check task ---
async def periodic_vip_check(bot: Bot):
    """
    Runs the VIP expiry check function periodically.
    """
    while True:
        try:
            await handlers.check_and_deactivate_expired_vip(bot)
        except Exception as e:
            logger.error(f"Error in periodic_vip_check loop: {e}", exc_info=True)
        # Check every 6 hours
        await asyncio.sleep(6 * 3600)


# --- Function to set up Aiogram Dispatcher + aiohttp Web Application ---
async def create_bot_app(bot: Bot) -> tuple[web.Application, Dispatcher]:
    dp = Dispatcher()
    dp.include_router(handlers.router)

    app = web.Application()
    app["bot"] = bot

    request_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    request_handler.register(app, path="/bot")

    # Your custom Chapa webhook route
    app.router.add_post(config.WEBHOOK_PATH, handlers.chapa_webhook_handler)

    logger.info("Aiogram Dispatcher and aiohttp web application configured.")
    return app, dp


# --- Optional: graceful shutdown ---
async def on_shutdown():
    if db_pool:
        await db_pool.close()
        logger.info("✅ Database pool closed.")


async def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting bot setup and localization initialization.")

    # Initialize localization
    setup_i18n()
    logger.info("Localization setup completed.")

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    # ✅ NEW: Create the DB pool at startup
    await create_pool()

    # ✅ Now it's safe to run DB queries
    await handlers.create_tables()
    logger.info("Database tables created or already exist.")

    # Create the web app and dispatcher
    app, dp = await create_bot_app(bot)

    # Remove webhook if set previously
    await bot.delete_webhook(drop_pending_updates=True)

    # Check if BASE_WEBHOOK_URL is configured
    if not config.BASE_WEBHOOK_URL:
        logger.error("BASE_WEBHOOK_URL is not set. This is required for webhooks.")
        logger.warning("Falling back to long polling for development.")
        await dp.start_polling(bot)
        return

    telegram_webhook_url = f"{config.BASE_WEBHOOK_URL}/bot"
    logger.info(f"Setting Telegram webhook to: {telegram_webhook_url}")
    await bot.set_webhook(url=telegram_webhook_url)

    # Launch background VIP check task
    asyncio.create_task(periodic_vip_check(bot))
    logger.info("Started periodic VIP expiry check task.")

    # Start aiohttp web server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(
        runner,
        host=config.WEB_SERVER_HOST,
        port=config.WEB_SERVER_PORT
    )
    logger.info(
        f"Starting web server on {config.WEB_SERVER_HOST}:{config.WEB_SERVER_PORT}"
    )
    await site.start()

    # Keep main task running forever
    await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot shutdown requested.")
        # Optionally, close pool on exit
        asyncio.run(on_shutdown())
