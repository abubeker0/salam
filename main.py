import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
import config
import handlers


async def main():
    logging.basicConfig(level=logging.INFO)

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.include_router(handlers.router)

    await handlers.create_tables()
    await handlers.set_commands(bot)
    await bot.delete_webhook(drop_pending_updates=True)

    

   

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())