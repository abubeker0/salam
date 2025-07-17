import os
import uuid
import time
import random
import logging
import asyncio
import aiofiles
import aiohttp
import asyncpg
import psycopg2
import psycopg2.extras
import gettext
import os
from .localization import _
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from aiohttp import web, ClientSession, ClientError
from aiogram import Bot, Router, F, types
from aiogram.types import (Message, CallbackQuery, ReplyKeyboardRemove,
                           ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton,
                           FSInputFile, LabeledPrice, PreCheckoutQuery,
                           SuccessfulPayment, BotCommand)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from datetime import datetime
import webhook.config as config
from .config import (CHAPA_SECRET_KEY, CHAPA_BASE_URL, CHAPA_CALLBACK_URL,
                     WEBHOOK_PATH, CHAPA_VERIFY_URL, BASE_WEBHOOK_URL,
                     WEB_SERVER_HOST, WEB_SERVER_PORT)

# Logger setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global router instance
router = Router()

# Other global variables
now = datetime.now(timezone.utc)
vip_search_locks = {}

db_pool = None  # Global pool


async def create_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=1,
            max_size=5,
        )
        logger.info("✅ Connection pool created successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to create DB pool: {e}", exc_info=True)
        raise


# main.py or webhook.py


async def set_commands(bot: Bot):
    commands = [
        types.BotCommand(command="start", description="Start the bot"),
        types.BotCommand(command="search",
                         description="🔍 Search for a partner"),
        types.BotCommand(command="stop",
                         description="🛑 Stop the current chat"),
        types.BotCommand(command="next", description="➡️ Find a new partner"),
        types.BotCommand(command="settings",
                         description="⚙️ Update gender, age or location"),
        types.BotCommand(command="vip", description="💎 Become a VIP member"),
        types.BotCommand(command="credit", description="💰 Earn credit"),
        types.BotCommand(command="userid",
                         description="🆔 Display your user ID"),
        types.BotCommand(command="privacy",
                         description="📜 View privacy,Terms and Conditions"),
    ]
    await bot.set_my_commands(commands)


def location_keyboard(
    user_id: int,
    lang_code: str = 'en'
) -> types.ReplyKeyboardMarkup:  # <--- THIS LINE MUST BE UPDATED
    """
    Creates a reply keyboard for location sharing, localized for the given user_id's language.
    """
    return types.ReplyKeyboardMarkup(
        keyboard=[[
            types.KeyboardButton(
                text=_("📍 Share Location",
                       lang_code=lang_code),  # <--- THIS LINE MUST BE UPDATED
                request_location=True)
        ]],
        resize_keyboard=True,
        one_time_keyboard=True)


async def get_city_from_coords(lat, lon):
    url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json"
    headers = {"User-Agent": "TelegramBot"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("address", {}).get("city") or \
                   data.get("address", {}).get("town") or \
                   data.get("address", {}).get("village")


@router.message(CommandStart())
async def cmd_start(message: types.Message, bot: Bot):
    """Handles the /start command."""
    user_id = message.from_user.id
    logger.info(f"Received /start from user {user_id}")

    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow(
                """
                SELECT user_id, gender, age, location, language
                FROM users
                WHERE user_id = $1
                """, user_id)
            logger.info(f"User data for {user_id}: {user}")

            # --- NEW USER FLOW ---
            if not user:
                logger.info(
                    f"New user {user_id}. Prompting for language selection.")

                await conn.execute(
                    """
                    INSERT INTO users (user_id, language)
                    VALUES ($1, $2)
                    ON CONFLICT (user_id) DO NOTHING
                    """, user_id, 'en')

                await message.answer(
                    "👋 Welcome to Selam Chat Bot!\n\n"
                    "Please choose your preferred language:",
                    reply_markup=language_keyboard())
                logger.info(
                    f"Sent language selection keyboard to new user {user_id}.")
                return

            # --- EXISTING USER FLOW ---
            user_language = user[
                'language'] if user and user['language'] else 'en'
            logger.info(f"User {user_id} exists. Language: {user_language}")

            if user['gender'] is None or user['age'] is None:
                logger.info(
                    f"User {user_id} profile incomplete. Asking for gender.")
                await message.answer(_(
                    "⚠️ Your profile is incomplete. Please finish the setup.\n\n"
                    "Select your gender:",
                    lang_code=user_language),
                                     reply_markup=gender_keyboard(
                                         user_id,
                                         context="start",
                                         lang_code=user_language))
                logger.info(
                    f"Sent gender keyboard for incomplete profile to {user_id}."
                )

            elif user['location'] is None:
                logger.info(
                    f"User {user_id} location is missing. Asking for location."
                )
                await message.answer(_(
                    "📍 Would you like to share your location for better matches?\n\n"
                    "This is optional, but helps us find people near you. If not, use /search command to find a match.",
                    lang_code=user_language),
                                     reply_markup=location_keyboard(
                                         lang_code=user_language))
                logger.info(
                    f"Sent location keyboard for missing location to {user_id}."
                )

            else:
                logger.info(
                    f"User {user_id} profile is complete. Sending welcome back message."
                )
                await message.answer(
                    _("🎉 Welcome back! You're all set.",
                      lang_code=user_language))
                logger.info(f"Sent welcome back message to {user_id}.")

            # Set bot commands
            await set_commands(bot)
            logger.info("Bot commands set successfully.")

    except Exception as e:
        logger.error(f"Error in cmd_start for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code='en'))


@router.message(F.location)
async def location_handler(message: types.Message, bot: Bot):
    """Handles location sharing and saves city name."""
    user_id = message.from_user.id
    location = message.location
    lat, lon = location.latitude, location.longitude

    # Get city from coordinates using reverse geocoding
    city = await get_city_from_coords(lat, lon)

    if not city:
        # If city can't be detected, inform the user
        await message.answer(_(
            "⚠️ Could not detect your city. Please try again later.",
            lang_code='en'),
                             reply_markup=ReplyKeyboardRemove())
        return

    try:
        async with db_pool.acquire() as conn:
            # Fetch the user's language directly from the database
            row = await conn.fetchrow(
                """
                SELECT language FROM users WHERE user_id = $1
                """, user_id)

            lang_code = row['language'] if row and row.get(
                'language') else 'en'

            # Update the user's location in the database (only saving city)
            await conn.execute(
                "UPDATE users SET location = $1 WHERE user_id = $2", city,
                user_id)

            logger.info(f"User {user_id} location updated to {city}.")

    except Exception as e:
        logger.error(
            f"Database error updating user location for {user_id}: {e}",
            exc_info=True)
        await message.answer(_(
            "❌ An internal database error occurred while saving your location. Please try again.",
            lang_code=lang_code),
                             reply_markup=ReplyKeyboardRemove())
        return

    # Send confirmation message
    await message.answer(_("✅ Location set to: {city}",
                           lang_code=lang_code).format(city=city),
                         reply_markup=ReplyKeyboardRemove())

    # Set bot commands after updating location
    await set_commands(bot)


current_chats = {}


def gender_keyboard(
    user_id: int,
    context: str = "start",
    lang_code: str = 'en'
) -> InlineKeyboardMarkup:  # <--- Add lang_code here, with a default
    """
    Creates an inline keyboard for gender selection, localized using the provided lang_code.
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=_("👱‍♂️ Male",
                       lang_code=lang_code),  # <--- Use lang_code here
                callback_data=f"gender:{context}:male")
        ],
        [
            InlineKeyboardButton(
                text=_("👩‍🦰 Female",
                       lang_code=lang_code),  # <--- Use lang_code here
                callback_data=f"gender:{context}:female")
        ],
        [
            InlineKeyboardButton(
                text=_("Any", lang_code=lang_code),  # <--- Use lang_code here
                callback_data=f"gender:{context}:any")
        ],
    ])
    return keyboard


def location_keyboard(lang_code: str) -> ReplyKeyboardMarkup:
    """
    Creates a reply keyboard for location sharing, localized using the provided lang_code.
    """
    keyboard = ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text=_("📍 Share Location", lang_code=lang_code),
                       request_location=True)
    ]],
                                   resize_keyboard=True,
                                   one_time_keyboard=True)
    return keyboard


# Placeholder for set_commands and logger if not already imported/defined
async def set_commands(bot: Bot):
    # This is a placeholder. Replace with your actual implementation.
    logging.info("Setting commands for bot.")


logger = logging.getLogger(__name__)  # Use the logger for consistent logging


@router.message(F.location)
async def location_handler(message: types.Message, bot: Bot):
    """Handles location sharing and saves city name."""
    user_id = message.from_user.id
    lang_code = 'en'
    location = message.location
    lat, lon = location.latitude, location.longitude
    city = await get_city_from_coords(lat, lon)

    if not city:
        await message.answer(_(
            "⚠️ Could not detect your city. Please try again later.",
            lang_code=lang_code),
                             reply_markup=ReplyKeyboardRemove())
        return

    try:
        async with db_pool.acquire() as conn:
            # Fetch user's language from DB
            user = await conn.fetchrow(
                """
                SELECT language FROM users WHERE user_id = $1
                """, user_id)
            if user and user['language']:
                lang_code = user['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Defaulting to 'en'."
                )

            # Update location in DB
            await conn.execute(
                "UPDATE users SET location = $1 WHERE user_id = $2", city,
                user_id)
            logger.info(f"User {user_id} location updated to {city}.")

    except Exception as e:
        logger.error(
            f"Database error updating user location for {user_id}: {e}",
            exc_info=True)
        await message.answer(_(
            "❌ An internal database error occurred while saving your location. Please try again.",
            lang_code=lang_code),
                             reply_markup=ReplyKeyboardRemove())
        return

    await message.answer(_("✅ Location set to: {city}",
                           lang_code=lang_code).format(city=city),
                         reply_markup=ReplyKeyboardRemove())

    await set_commands(bot)


# --- NEW CALLBACK QUERY HANDLER FOR LANGUAGE SELECTION ---
@router.callback_query(F.data.startswith("lang_select:"))
async def language_selection_callback(call: CallbackQuery):
    user_id = call.from_user.id
    selected_lang_code = call.data.split(":")[1]
    logger.info(f"User {user_id} selected language: {selected_lang_code}")

    try:
        async with db_pool.acquire() as conn:
            # Update user's language in the database
            await conn.execute(
                "UPDATE users SET language = $1 WHERE user_id = $2",
                selected_lang_code, user_id)
            logger.info(
                f"Updated language for user {user_id} to {selected_lang_code}."
            )

            await call.answer(
                _("Language set successfully!", lang_code=selected_lang_code))

            await call.message.edit_text(_(
                "Language set to <b>{lang}</b>.".format(
                    lang=selected_lang_code),
                lang_code=selected_lang_code),
                                         parse_mode=ParseMode.HTML)

            # Fetch user gender to determine next step
            user_row = await conn.fetchrow(
                "SELECT gender FROM users WHERE user_id = $1", user_id)

            if user_row and not user_row["gender"]:
                await call.message.answer(_(
                    "👋 Welcome to the Selam Chat Bot! Let's get you set up.\n\n"
                    "By using this bot, you confirm you're 18+ and agree to our Terms and Conditions (/privacy).\n\n"
                    "Please select your gender:",
                    lang_code=selected_lang_code),
                                          reply_markup=gender_keyboard(
                                              user_id,
                                              context="start",
                                              lang_code=selected_lang_code))
                logger.info(
                    f"Sent gender keyboard to user {user_id} after language selection."
                )
            else:
                await call.message.answer(
                    _("✅ Language updated successfully. You will only be matched with users who speak your language if possible.",
                      lang_code=selected_lang_code))

    except Exception as e:
        logger.error(
            f"Error in language_selection_callback for user {user_id}: {e}",
            exc_info=True)
        await call.answer(
            _("❌ An unexpected error occurred. Please try again.",
              lang_code=selected_lang_code))
        await call.message.edit_text(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=selected_lang_code))


# This list defines your supported languages and their display names
# You can define this globally or load from config
AVAILABLE_LANGUAGES = {
    'en': 'English 🇬🇧',
    'am': 'Amharic 🇪🇹',
    'or': 'Oromo 🇪🇹',
    'ar': 'العربية 🇸🇦'  # Assuming a generic Arabic flag or adjust as needed
}


def language_keyboard() -> InlineKeyboardMarkup:
    """
    Creates an inline keyboard for language selection.
    This keyboard does NOT need localization itself, as it's for choosing a language.
    """
    builder = InlineKeyboardBuilder()
    for lang_code, lang_name in AVAILABLE_LANGUAGES.items():
        # Callback data format: "lang_select:<code>"
        builder.button(text=lang_name,
                       callback_data=f"lang_select:{lang_code}")
    builder.adjust(2)  # Adjust layout as desired
    return builder.as_markup()


async def get_user_language_from_db(user_id: int, conn) -> str:
    """
    Fetches a user's language from the database.
    Defaults to 'en' if not found or on error.
    """
    if not conn:
        logger.warning(
            f"No DB connection provided to get_user_language_from_db for user {user_id}. Defaulting to 'en'."
        )
        return 'en'
    try:
        language_code = await conn.fetchval(
            "SELECT language FROM users WHERE user_id = $1", user_id)
        if language_code:
            return language_code
        else:
            logger.info(
                f"Language not set for user {user_id}. Defaulting to 'en'.")
            return 'en'
    except Exception as e:
        logger.error(f"Error fetching language for user {user_id}: {e}",
                     exc_info=True)
        return 'en'


@router.callback_query(F.data == "set_language")
async def show_language_options_from_settings(call: CallbackQuery):
    user_id = call.from_user.id
    user_language = 'en'

    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            if user and user.get("language"):
                user_language = user["language"]
            else:
                logger.warning(
                    f"Language not set for user {user_id}, defaulting to 'en'."
                )

            await call.answer()

            await call.message.edit_text(_(
                "Please choose your preferred language:",
                lang_code=user_language),
                                         reply_markup=language_keyboard())
            logger.info(
                f"User {user_id} requested language change from settings.")

    except Exception as e:
        logger.error(
            f"Error in show_language_options_from_settings for user {user_id}: {e}",
            exc_info=True)
        await call.answer(_("❌ An error occurred.", lang_code=user_language))
        try:
            await call.message.edit_text(
                _("❌ Please try again later.", lang_code=user_language))
        except Exception:
            pass


# In webhook/handlers.py (inside the gender_callback function)


@router.callback_query(F.data.startswith("gender:"))
async def gender_callback(query: types.CallbackQuery, bot: Bot):
    """Handles gender selection callback."""
    await query.answer()

    user_id = query.from_user.id
    _item, context, gender = query.data.split(":")

    user_language = 'en'  # Fallback default

    try:
        async with db_pool.acquire() as conn:
            # Fetch user language
            user = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            user_language = user[
                'language'] if user and user['language'] else 'en'
            logger.info(
                f"Gender callback for user {user_id}. Fetched language: {user_language}"
            )

            # Update gender
            await conn.execute(
                "UPDATE users SET gender = $1 WHERE user_id = $2", gender,
                user_id)
            logger.info(f"User {user_id} gender updated to {gender}.")

        # Remove inline keyboard from the previous message
        await query.message.delete_reply_markup()

        if context == "change":
            await query.message.answer(
                _("✅ Gender updated!", lang_code=user_language))
            logger.info(f"Sent 'gender updated' message to {user_id}.")

        elif context == "start":
            await query.message.answer(
                _("🔢 Please enter your age:", lang_code=user_language))
            logger.info(f"Sent 'enter age' message to {user_id}.")

        await set_commands(bot)

    except Exception as e:
        logger.error(f"Database error updating gender for user {user_id}: {e}",
                     exc_info=True)
        await query.message.answer(
            _("❌ An unexpected error occurred while saving your gender. Please try again later.",
              lang_code='en'))


@router.message(F.text.isdigit())
async def age_handler(message: types.Message, bot: Bot):
    """Handles age input."""

    age = int(message.text)
    user_id = message.from_user.id
    user_language = 'en'  # fallback default

    try:
        async with db_pool.acquire() as conn:
            # Fetch user language
            user = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            user_language = user[
                'language'] if user and user['language'] else 'en'
            logger.info(f"User {user_id} language: {user_language}")

            # Update user's age
            await conn.execute("UPDATE users SET age = $1 WHERE user_id = $2",
                               age, user_id)
            logger.info(f"User {user_id} age updated to {age}.")

        # Send success messages
        await message.answer(
            _("✅ Your profile is complete!", lang_code=user_language))
        await message.answer(_(
            "📍 Would you like to share your location for better matches?\n\n"
            "This is optional, but helps us find people near you. If not, use /search command to find a match.",
            lang_code=user_language),
                             reply_markup=location_keyboard(user_language))

        await set_commands(bot)

    except Exception as e:
        logger.error(f"Database error updating age for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred while saving your age. Please try again later.",
              lang_code='en'))


# This function does not interact with the database, so no changes needed
@router.callback_query(F.data == "set_gender")
async def set_gender_handler(query: types.CallbackQuery):
    # Always answer the callback query first to dismiss the loading state
    await query.answer()

    user_id = query.from_user.id
    user_language = "en"  # Default fallback

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            if row and row["language"]:
                user_language = row["language"]
            else:
                logger.warning(
                    f"No language found for user {user_id}. Defaulting to 'en'."
                )
    except Exception as e:
        logger.error(f"Failed to fetch language for user {user_id}: {e}",
                     exc_info=True)

    await query.message.answer(_("👱‍♂️ Select your new gender👩‍🦰:",
                                 lang_code=user_language),
                               reply_markup=gender_keyboard(
                                   user_id=user_id,
                                   context="change",
                                   lang_code=user_language))
    logger.info(f"Sent gender change options to user {user_id}.")


# This function does not interact with the database, so no changes needed
current_chats = {
}  # Dictionary to store active chat pairs (user_id: partner_id)


# This function does not interact with the database, so no changes needed
def gender_selection_keyboard(user_language: str):
    """Creates an inline keyboard for gender selection with localization."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=_("🙋🏻‍♂️ Male", lang_code=user_language),
                                 callback_data="gender_pref:male")
        ],
        [
            InlineKeyboardButton(text=_("🙋🏻‍♀️ Female",
                                        lang_code=user_language),
                                 callback_data="gender_pref:female")
        ],
        [
            InlineKeyboardButton(text=_("🌐 Any", lang_code=user_language),
                                 callback_data="gender_pref:any")
        ],
    ])
    return keyboard


search_queue = [
]  # List to store searching users (user_id, timestamp, gender_pref)
current_chats = {
}  # Dictionary to store active chat pairs (user_id: partner_id)

find_match_lock = asyncio.Lock()  # Already defined


async def find_match(
    user_id: int,
    gender_pref: str,
    is_vip: bool,
):
    global current_chats, search_queue

    logger.debug(f"find_match called for user {user_id}. Pref: {gender_pref}, VIP: {is_vip}")

    try:
        async with db_pool.acquire() as conn:

            async with find_match_lock:
                # Check if user is still in search_queue
                if not any(uid == user_id for uid, _, _ in search_queue):
                    logger.debug(f"User {user_id} no longer in search_queue at start of find_match (within lock).")
                    return None, False

                user_ids_in_queue = [uid for uid, _, _ in search_queue]
                rows = await conn.fetch(
                    """
                    SELECT user_id, is_vip, gender, language
                    FROM users
                    WHERE user_id = ANY($1)
                    """, user_ids_in_queue)
                user_info_map = {row["user_id"]: row for row in rows}

                user_row = user_info_map.get(user_id)
                if not user_row or not user_row.get("gender"):
                    logger.warning(f"User {user_id} has no gender set. Removing from queue.")
                    search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
                    return None, False

                user_own_gender = user_row["gender"]
                user_language = user_row.get("language", "en")
                current_user_effective_pref = gender_pref if is_vip else "any"

                potential_partners = []

                def passes_criteria(current_user_effective_pref,
                                    user_own_gender, other_user_id,
                                    other_user_row, other_user_gender_pref_in_queue):
                    other_user_gender = other_user_row["gender"]
                    other_user_is_vip = other_user_row["is_vip"]
                    other_user_language = other_user_row.get("language", "en")
                    other_user_effective_pref = (
                        other_user_gender_pref_in_queue if other_user_is_vip else "any")

                    if not other_user_gender:
                        return False

                    current_user_likes_other = (
                        current_user_effective_pref == "any"
                        or other_user_gender == current_user_effective_pref)
                    other_user_likes_current = (
                        other_user_effective_pref == "any"
                        or user_own_gender == other_user_effective_pref)

                    if not (current_user_likes_other and other_user_likes_current):
                        return False

                    # VIPs can match with anyone, but we can still check if language is compatible
                    if is_vip:
                        # Allow VIPs to match even if languages are different
                        return True

                    # Only non-VIPs enforce same-language initially
                    if not is_vip and not other_user_is_vip:
                        return user_language == other_user_language

                    return True

                # === Phase 1: same-language matches for non-VIPs, or full match for VIPs ===
                for other_user_id, _, other_user_gender_pref_in_queue in search_queue:
                    if other_user_id == user_id:
                        continue
                    other_user_row = user_info_map.get(other_user_id)
                    if not other_user_row:
                        continue
                    if passes_criteria(current_user_effective_pref,
                                       user_own_gender, other_user_id,
                                       other_user_row,
                                       other_user_gender_pref_in_queue):
                        potential_partners.append((other_user_id, other_user_row["is_vip"]))

                # === Phase 2: fallback to any language for non-VIPs if no same-language match is found ===
                if not potential_partners and not is_vip:
                    logger.debug("No same-language match found. Trying fallback to any language.")
                    for other_user_id, _, other_user_gender_pref_in_queue in search_queue:
                        if other_user_id == user_id:
                            continue
                        other_user_row = user_info_map.get(other_user_id)
                        if not other_user_row:
                            continue

                        other_user_gender = other_user_row["gender"]
                        other_user_is_vip = other_user_row["is_vip"]
                        other_user_effective_pref = (
                            other_user_gender_pref_in_queue if other_user_is_vip else "any")

                        current_user_likes_other = (
                            current_user_effective_pref == "any" or
                            other_user_gender == current_user_effective_pref)
                        other_user_likes_current = (
                            other_user_effective_pref == "any"
                            or user_own_gender == other_user_effective_pref)

                        if current_user_likes_other and other_user_likes_current:
                            potential_partners.append(
                                (other_user_id, other_user_is_vip))

                if potential_partners:
                    partner_id, partner_is_vip = random.choice(potential_partners)
                    current_queue_user_ids = {uid for uid, _, _ in search_queue}
                    if user_id not in current_queue_user_ids or partner_id not in current_queue_user_ids:
                        logger.warning(f"User {user_id} or {partner_id} no longer in queue during final match.")
                        return None, False

                    search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id and uid != partner_id]
                    current_chats[user_id] = partner_id
                    current_chats[partner_id] = user_id

                    logger.info(f"MATCHED: {user_id} <-> {partner_id} (VIP:{is_vip} vs PartnerVIP:{partner_is_vip})")
                    return partner_id, partner_is_vip

                logger.debug(f"No suitable partners found for {user_id}.")
                return None, False

    except Exception as e:
        logger.error(f"ERROR in find_match() for user {user_id}: {e}", exc_info=True)
        return None, False


# 🔧 Ensure tuple on error


async def handle_vip_search(message: types.Message, bot: Bot):
    """Handles /search for VIP users."""
    user_id = message.from_user.id
    user_language = "en"  # Default fallback

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            if row and row["language"]:
                user_language = row["language"]
    except Exception as e:
        logger.error(f"Failed to fetch language for VIP user {user_id}: {e}",
                     exc_info=True)

    await message.answer(_("💎Choose the gender you want to chat with:",
                           lang_code=user_language),
                         reply_markup=gender_selection_keyboard(user_language))


@router.callback_query(F.data.startswith("gender_pref:"))
async def gender_preference_callback(query: types.CallbackQuery, bot: Bot):
    user_id = query.from_user.id
    gender_pref = query.data.split(":")[1]
    user_language = "en"
    current_time = time.time()

    await query.answer()

    # --- Check if user is already searching ---
    if any(uid == user_id for uid, _, _ in search_queue):
        await query.message.answer(
            _("⏳ You are already in the search queue. Please wait for your current search to complete or /stop.",
              lang_code=user_language))
        await bot.delete_message(chat_id=query.message.chat.id,
                                 message_id=query.message.message_id)
        return

    await bot.delete_message(chat_id=query.message.chat.id,
                             message_id=query.message.message_id)

    # --- Handle disconnect if in chat ---
    if user_id in current_chats:
        partner_id = current_chats.pop(user_id, None)
        if partner_id and partner_id in current_chats:
            del current_chats[partner_id]
            try:
                await bot.send_message(
                    partner_id,
                    _("😔Your partner has disconnected. Use /search to find a partner.",
                      lang_code=user_language))
            except Exception as e:
                logger.error(
                    f"Could not send disconnect message to {partner_id}: {e}")
        await query.message.answer(
            _("You were in a chat. Disconnected.", lang_code=user_language))
        return

    # --- DB Fetch (VIP status, gender, language) ---
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT is_vip, gender, language FROM users WHERE user_id = $1",
                user_id)

            if not row:
                await query.message.answer(
                    _("⚠️ Could not retrieve your user info. Please try again.",
                      lang_code=user_language))
                logger.warning(f"User {user_id} not found in DB.")
                return

            is_vip = row["is_vip"]
            user_own_gender = row["gender"]
            if row.get("language"):
                user_language = row["language"]

            if not user_own_gender:
                await query.message.answer(
                    _("⚠️ Please set your gender first using /setgender.",
                      lang_code=user_language))
                return

            if not is_vip:
                await query.message.answer(
                    _("💎 Gender-based matching is a VIP-only feature.\nBecome a /vip member",
                      lang_code=user_language))
                return

            # Add user to queue
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue
                               if uid != user_id]
            search_queue.append((user_id, current_time, gender_pref))
            logger.info(
                f"User {user_id} added to VIP search queue with preference {gender_pref}."
            )

            searching_message = await query.message.answer(
                _("🔍 Searching for a partner...", lang_code=user_language))
            searching_message_id = searching_message.message_id

            partner_id = None
            partner_is_vip = False

            for attempt in range(20):
                partner_id, partner_is_vip = await find_match(
                    user_id, gender_pref, is_vip)
                if partner_id:
                    break
                await asyncio.sleep(1)

            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue
                               if uid != user_id]
            logger.info(
                f"User {user_id} removed from queue after VIP match search.")

            try:
                await bot.delete_message(chat_id=query.message.chat.id,
                                         message_id=searching_message_id)
            except Exception as e:
                logger.error(
                    f"Could not delete searching message for user {user_id}: {e}"
                )

            if partner_id:
                current_chats[user_id] = partner_id
                current_chats[partner_id] = user_id

                if partner_is_vip:
                    await query.message.answer(_(
                        "💎 You found another VIP partner! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat",
                        lang_code=user_language),
                                               parse_mode=ParseMode.HTML)
                else:
                    await query.message.answer(
                        _("☺️ Partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat",
                          lang_code=user_language))

                try:
                    await bot.send_message(
                        partner_id,
                        _("💎 VIP partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat",
                          lang_code=user_language),
                        parse_mode=ParseMode.HTML)
                except Exception as e:
                    logger.error(
                        f"Could not send match message to partner {partner_id}: {e}"
                    )
            else:
                logger.info(
                    f"No match found for VIP user {user_id} after timeout.")

    except Exception as e:
        logger.error(
            f"Error in gender_preference_callback for user {user_id}: {e}",
            exc_info=True)
        await query.message.answer(
            _("❌ An unexpected error occurred during search. Please try again later.",
              lang_code=user_language))


async def get_partner_searching_message_id(partner_id: int) -> int | None:
    try:
        async with db_pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT message_id FROM search_messages WHERE user_id = $1",
                partner_id)
            if result:
                return result['message_id']
            else:
                return None
    except Exception as e:
        logger.error(
            f"ERROR: Error in get_partner_searching_message_id for {partner_id}: {e}",
            exc_info=True)
        return None


# If no VIP match found, ask if they want to search free users
#if is_vip:
# keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
# [types.InlineKeyboardButton(text="Yes", callback_data=f"search_free:{gender}")],
# [types.InlineKeyboardButton(text="No", callback_data="cancel_search")],
# ])
# await query.message.answer(f"No VIP users are currently available for chat with your selected gender ({gender}). Would you like to search free users?", reply_markup=keyboard)
# else:
#await query.message.answer("No users are currently available for chat with your selected gender.")
# await query.answer()

#@router.callback_query(F.data.startswith("search_free:"))
#async def search_free_callback(query: types.CallbackQuery, bot: Bot):
# """Handles the callback for searching free users after no VIP match."""
# user_id = query.from_user.id
#gender = query.data.split(":")[1]

# if await find_match(user_id, gender, bot, is_vip=False): #sets is_vip to false, so it searches free users.
# await query.answer()
# return

# await query.message.answer("No free users are currently available for chat with your selected gender.")
# await query.answer()

#@router.callback_query(F.data == "cancel_search")
#async def cancel_search_callback(query: types.CallbackQuery):
#"""Handles the callback to cancel the search."""
#await query.message.answer("Search canceled.")


@router.message(lambda message: message.text == "🙋🏻‍♂️ Search by Gender🙋🏻‍♀️")
async def search_by_gender_handler(message: types.Message, bot: Bot):
    await handle_vip_search(message, bot)


def search_menu_reply_keyboard(user_language: str):
    return ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(
                text=_("🏙️ Search by City", lang_code=user_language))
        ],
                  [
                      KeyboardButton(text=_("🙋🏻‍♂️ Search by Gender🙋🏻‍♀️",
                                            lang_code=user_language))
                  ]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


@router.callback_query(F.data == "set_location")
async def set_location_callback(query: types.CallbackQuery):
    # Always answer the callback query first to dismiss the loading state on the client
    await query.answer()

    user_id = query.from_user.id
    user_language = "en"  # Default fallback

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            if row and row["language"]:
                user_language = row["language"]
    except Exception as e:
        logger.error(
            f"Failed to fetch language for user {user_id} in set_location_callback: {e}",
            exc_info=True)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text=_("📍 Share Location", lang_code=user_language),
                           request_location=True)
        ]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

    await query.message.answer(_("Please share your live location:",
                                 lang_code=user_language),
                               reply_markup=keyboard)


@router.message(Command("search"))
async def search_command(message: types.Message, bot: Bot):
    user_id = message.from_user.id
    user_language = "en"
    try:
        async with db_pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT is_vip FROM users WHERE user_id = $1", user_id)

            if result and result["is_vip"]:
                # VIP flow
                await quick_vip_search(message)
            else:
                # Non-VIP flow
                await handle_non_vip_search(message, bot)

    except Exception as e:
        logger.error(f"Error in search_command for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
             lang_code=user_language))


# You would put this function in a suitable place,
# like a 'utils.py' or 'database.py' file,
# and then import it as shown in Option 1.

# You would also need to ensure 'create_database_connection' is available
# and 'logger' is defined.

# In webhook/handlers.py (or wherever your global state is)
# ...
search_queue = []
current_chats = {}
user_search_cooldowns = {}  # New dictionary to track cooldowns
SEARCH_COOLDOWN_SECONDS = 30  # For example, 30 seconds


async def quick_vip_search(message: types.Message):
    user_id = message.from_user.id
    user_language = 'en'

    try:
        async with db_pool.acquire() as conn:
            # Fetch user language
            user_data = await get_user_credits(user_id, conn=conn)
            user_language = user_data.get('language') if user_data and user_data.get('language') else 'en'

            # --- COOLDOWN CHECK ---
            if any(uid == user_id for uid, _, _ in search_queue):
                await message.answer(
                    _("⏳ You are already in the search queue. Please wait for your current search to complete, or you can /stop",
                      lang_code=user_language))
                return

            # --- CURRENT CHAT CHECK ---
            if user_id in current_chats:
                await message.answer(
                    _("🤔 You are already in a dialog right now.\n/next — find a new partner\n/stop — stop this dialog",
                      lang_code=user_language))
                return

            # --- ADD TO QUEUE ---
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue
                               if uid != user_id]
            search_queue.append(
                (user_id, time.time(), "any"))  # VIP quick search = any
            logger.info(f"User {user_id} started quick VIP search and added to queue.")

            search_msg = await message.answer(
                _("🔍 Searching for a partner...", lang_code=user_language))

            timeout = 20  # Increased timeout
            interval = 2  # Increased sleep interval
            elapsed = 0
            partner_id = None
            partner_is_vip = False

            # Search for a partner considering language preference
            while elapsed < timeout:
                partner_id, partner_is_vip = await find_match(
                    user_id, user_language, True)  # Pass the user's language preference
                if partner_id:
                    break
                await asyncio.sleep(interval)
                elapsed += interval

            # Remove user from queue no matter what
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue
                               if uid != user_id]
            logger.info(f"User {user_id} removed from search queue after attempt.")

            try:
                await message.bot.delete_message(
                    chat_id=message.chat.id, message_id=search_msg.message_id)
            except Exception as e:
                logger.error(f"Failed to delete search message for user {user_id}: {e}")

            if partner_id:
                current_chats[user_id] = partner_id
                current_chats[partner_id] = user_id

                # Fetch partner's language
                partner_language = 'en'
                try:
                    partner_row = await conn.fetchrow(
                        "SELECT language FROM users WHERE user_id = $1",
                        partner_id)
                    if partner_row and partner_row['language']:
                        partner_language = partner_row['language']
                except Exception as db_e:
                    logger.error(f"Error fetching partner language for {partner_id}: {db_e}")

                # Notify both users
                if partner_is_vip:
                    await message.answer(_(
                        "💎 You found another VIP partner! Start chatting!\n\n/next — new partner\n/stop — end chat",
                        lang_code=user_language),
                                         parse_mode=ParseMode.HTML)
                else:
                    await message.answer(
                        _("☺️ Partner found! Start chatting!\n\n/next — new partner\n/stop — end chat",
                          lang_code=user_language))

                try:
                    await message.bot.send_message(
                        partner_id,
                        _("💎 VIP partner found! Start chatting!\n\n/next — new partner\n/stop — end chat",
                          lang_code=partner_language),
                        parse_mode=ParseMode.HTML)
                except Exception as e:
                    logger.error(f"Failed to send match message to partner {partner_id}: {e}")

                logger.info(f"Quick VIP search: Match found between {user_id} and {partner_id}")

    except Exception as e:
        logger.error(f"Unhandled error in quick_vip_search for user {user_id}: {e}", exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


@router.message(Command("stop"))
async def stop_command(message: types.Message, bot: Bot):
    """Handles the /stop command."""
    global current_chats, search_queue
    user_id = message.from_user.id
    logger.info(f"Stop command from {user_id}. Current chats: {current_chats}")

    user_language = 'en'
    partner_language = 'en'

    try:
        async with db_pool.acquire() as conn:
            # Load user (and partner if any) language
            partner_id = current_chats.get(user_id)
            ids = [user_id]
            if partner_id:
                ids.append(partner_id)

            rows = await conn.fetch(
                """
                SELECT user_id, language
                FROM users
                WHERE user_id = ANY($1)
                """, ids)
            langs = {r["user_id"]: r["language"] or 'en' for r in rows}
            user_language = langs.get(user_id, 'en')
            partner_language = langs.get(partner_id,
                                         'en') if partner_id else 'en'

            # Check if user is in an active chat
            if user_id in current_chats:
                partner_id = current_chats[user_id]

                # Check mutual chat mapping
                if partner_id in current_chats and current_chats[
                        partner_id] == user_id:
                    # Remove both from chat
                    del current_chats[user_id]
                    del current_chats[partner_id]

                    # Notify partner
                    await bot.send_message(
                        partner_id,
                        _("😔 Your partner has stopped the chat. /search to find a new partner",
                          lang_code=partner_language),
                        reply_markup=search_menu_reply_keyboard(
                            partner_language))

                    await bot.send_message(
                        partner_id,
                        _("How was your experience with your last partner?",
                          lang_code=partner_language),
                        reply_markup=await feedback_keyboard(partner_id))

                    # Notify user
                    await message.answer(
                        _("✅ Chat stopped. /search to find a new partner",
                          lang_code=user_language),
                        reply_markup=search_menu_reply_keyboard(user_language))

                    await message.answer(
                        _("How was your experience with your last partner?",
                          lang_code=user_language),
                        reply_markup=await feedback_keyboard(user_id))

                    # Remove both from search queue
                    search_queue[:] = [(uid, ts, gen)
                                       for uid, ts, gen in search_queue
                                       if uid != user_id and uid != partner_id]

                else:
                    # Inconsistent mapping
                    del current_chats[user_id]
                    await message.answer(
                        _("There was an issue stopping the chat.",
                          lang_code=user_language))
                    search_queue[:] = [(uid, ts, gen)
                                       for uid, ts, gen in search_queue
                                       if uid != user_id]

                return

            # Check if user is searching (but not matched yet)
            was_searching = any(uid == user_id for uid, _, _ in search_queue)

            # Remove from search queue if there
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue
                               if uid != user_id]

            if was_searching:
                await message.answer(_(
                    "✅ Your search has been stopped. /search to start again.",
                    lang_code=user_language),
                                     reply_markup=search_menu_reply_keyboard(
                                         user_language))
            else:
                await message.answer(
                    _("🤔 You are not in an active chat or searching. /search to find a partner.",
                      lang_code=user_language))

    except Exception as e:
        logger.error(
            f"Unhandled error in stop_command for user {user_id}: {e}",
            exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


@router.message(Command("settings"))
async def settings_command(message: types.Message):
    user_id = message.from_user.id
    user_language = 'en'  # Default fallback

    try:
        async with db_pool.acquire() as conn:
            # Fetch user language (safe fallback inside helper)
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text=_("👱‍♂️Change Gender👩‍🦰",
                                                lang_code=user_language),
                                         callback_data="set_gender")
                ],
                [
                    InlineKeyboardButton(text=_("📍 Set Location",
                                                lang_code=user_language),
                                         callback_data="set_location")
                ],
                [
                    InlineKeyboardButton(text=_("🎂 Set Age",
                                                lang_code=user_language),
                                         callback_data="set_age")
                ],
                [
                    InlineKeyboardButton(text=_("🌐 Set Language",
                                                lang_code=user_language),
                                         callback_data="set_language")
                ]
            ])

            await message.answer(_("⚙️ Choose what you want to update:",
                                   lang_code=user_language),
                                 reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Error in settings_command for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred while loading settings. Please try again later.",
              lang_code=user_language))


#def gender_search_keyboard():
#"""Creates an inline keyboard for gender search."""
# keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
# [types.InlineKeyboardButton(text="♂️ Male", callback_data="search_gender:male")],
#[types.InlineKeyboardButton(text="♀️ Female", callback_data="search_gender:female")],
#[types.InlineKeyboardButton(text="Any", callback_data="search_gender:any")],
# ])
#return keyboard

#def city_gender_search_keyboard():
# """Creates an inline keyboard for city and gender search."""
### [types.InlineKeyboardButton(text="♀️ Female", callback_data="search_gender:female")],
# [types.InlineKeyboardButton(text="Any", callback_data="search_gender:any")],
#])
#return keyboard


async def get_user_credits(user_id: int, conn: asyncpg.Connection = None):
    """
    Retrieves user credits, search data, and language from the database.
    Uses an existing connection if provided, otherwise acquires one from the pool.
    """
    if conn:
        result = await conn.fetchrow(
            "SELECT credit, last_search_date, search_count, language FROM users WHERE user_id = $1",
            user_id)
    else:
        async with db_pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT credit, last_search_date, search_count, language FROM users WHERE user_id = $1",
                user_id)

    if result:
        return {
            "credits": result['credit'],
            "last_search_date": result['last_search_date'],
            "search_count": result['search_count'],
            "language": result['language']
        }
    else:
        return {
            "credits": 0,
            "last_search_date": None,
            "search_count": 0,
            "language": 'en'
        }


async def update_user_credits(user_id: int,
                              credits: int,
                              last_search_date: date,
                              search_count: int,
                              conn: asyncpg.Connection = None):
    """
    Updates user credits and search data in the database.
    Uses a provided connection or acquires one from the pool.
    """
    if conn:
        await conn.execute(
            """
            UPDATE users
            SET credit = $1, last_search_date = $2, search_count = $3
            WHERE user_id = $4
            """, credits, last_search_date, search_count, user_id)
    else:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users
                SET credit = $1, last_search_date = $2, search_count = $3
                WHERE user_id = $4
                """, credits, last_search_date, search_count, user_id)
    logger.info(
        f"User {user_id} credits updated to {credits}, last_search_date to {last_search_date}, search_count to {search_count}."
    )


@router.message(Command("credit"))
async def credit_command(message: types.Message):
    """Handles the /credit command."""
    user_id = message.from_user.id

    try:
        async with db_pool.acquire() as conn:
            user_data = await get_user_credits(user_id, conn=conn)
            user_language = user_data.get('language', 'en')

            new_credits = user_data['credits'] + 20

            await update_user_credits(user_id,
                                      new_credits,
                                      user_data['last_search_date'],
                                      user_data['search_count'],
                                      conn=conn)

            photo = FSInputFile("media/download.png")
            await message.answer_photo(photo=photo)

            await message.answer(
                _("💰 You earned 20 credits!\nYour total credits: {new_credits}",
                  lang_code=user_language).format(new_credits=new_credits))

            logger.info(
                f"User {user_id} added 10 credits. Total: {new_credits}")

    except Exception as e:
        logger.error(
            f"Error processing /credit command for user {user_id}: {e}",
            exc_info=True)
        await message.answer(
            _("❌ An error occurred while adding credits. Please try again later.",
              lang_code=user_language if 'user_language' in locals() else 'en')
        )


# Initialize global variables at module level (as provided by you)
search_queue = []
non_vip_search_locks = defaultdict(bool)


# Assume get_user_credits, update_user_credits, and find_match are defined and corrected elsewhere
# Example placeholders if they are in other files:
# from .db_operations import get_user_credits, update_user_credits
# from .matchmaking import find_match
# Initialize logger
# In webhook/handlers.py (inside handle_non_vip_search)

# In webhook/handlers.py (inside handle_non_vip_search)
# Example improvements in the search handling logic
async def handle_non_vip_search(message: types.Message, bot: Bot):
    global search_queue, non_vip_search_locks, current_chats
    user_id = message.from_user.id
    today = date.today()
    user_language = 'en'

    if user_id not in non_vip_search_locks:
        non_vip_search_locks[user_id] = False

    try:
        # Using async with for connection pool management
        async with db_pool.acquire() as conn:
            user_data = await get_user_credits(user_id, conn=conn)
            user_language = user_data.get('language', 'en')

            if non_vip_search_locks[user_id]:
                await message.answer(
                    _("Please wait for your previous search request to finish, or /stop to cancel.",
                      lang_code=user_language))
                return

            non_vip_search_locks[user_id] = True

            if user_data.get('last_search_date') != today:
                user_data['search_count'] = 0
                await update_user_credits(user_id,
                                          user_data.get('credits', 0),
                                          today,
                                          0,
                                          conn=conn)

            current_search_count = user_data.get('search_count', 0)
            current_credits = user_data.get('credits', 0)
            needs_credit = current_search_count >= 10

            if needs_credit and current_credits <= 0:
                await message.answer(
                    _("You have reached your daily search limit or have no credits. Use /credit to get more searches.",
                      lang_code=user_language))
                return

            # Disconnect from active chat if any
            if user_id in current_chats:
                partner_id = current_chats.pop(user_id, None)
                if partner_id:
                    current_chats.pop(partner_id, None)
                    partner_language = 'en'
                    try:
                        partner_row = await conn.fetchrow(
                            "SELECT language FROM users WHERE user_id = $1",
                            partner_id)
                        if partner_row and partner_row['language']:
                            partner_language = partner_row['language']
                    except Exception as db_e:
                        logger.error(
                            f"Error fetching partner language for {partner_id}: {db_e}"
                        )

                    try:
                        await bot.send_message(
                            partner_id,
                            _("Your partner has disconnected to /search for someone new.",
                              lang_code=partner_language))
                    except Exception as e:
                        logger.error(
                            f"Failed to send disconnect message to {partner_id}: {e}"
                        )

                await message.answer(
                    _("You have been disconnected from your previous chat. Searching for a new partner.",
                      lang_code=user_language))

            # Update search credits
            new_search_count = current_search_count + 1
            new_credits = current_credits - 1 if needs_credit else current_credits
            await update_user_credits(user_id,
                                      new_credits,
                                      today,
                                      new_search_count,
                                      conn=conn)

            # Add user to queue
            async with find_match_lock:
                search_queue[:] = [(uid, ts, gen)
                                   for uid, ts, gen in search_queue
                                   if uid != user_id]
                search_queue.append((user_id, time.time(), "any"))

            searching_message = await message.answer(
                _("🔍 Searching for a partner...", lang_code=user_language))

            partner_id = None
            is_partner_vip = False
            match_found = False

            timeout_seconds = 30  # Increased timeout
            sleep_interval = 3  # Increased sleep interval
            current_attempts = 0
            while current_attempts * sleep_interval < timeout_seconds:
                found_partner_id, found_is_partner_vip = await find_match(
                    user_id, "any", False)
                if found_partner_id:
                    partner_id = found_partner_id
                    is_partner_vip = found_is_partner_vip
                    match_found = True
                    break
                await asyncio.sleep(sleep_interval)
                current_attempts += 1

            # Remove "searching..." message
            try:
                await bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=searching_message.message_id)
            except Exception as e:
                logger.error(
                    f"Failed to delete search message for user {user_id}: {e}")

            if match_found and partner_id:
                search_queue[:] = [(uid, ts, gen)
                                   for uid, ts, gen in search_queue
                                   if uid != user_id]
                current_chats[user_id] = partner_id
                current_chats[partner_id] = user_id

                partner_language = 'en'
                try:
                    partner_row = await conn.fetchrow(
                        "SELECT language FROM users WHERE user_id = $1",
                        partner_id)
                    if partner_row and partner_row['language']:
                        partner_language = partner_row['language']
                except Exception as db_e:
                    logger.error(
                        f"Error fetching partner language for {partner_id} after match: {db_e}"
                    )

                # Message for user
                if is_partner_vip:
                    await message.answer(
                        _("💎 VIP partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat",
                          lang_code=user_language))
                else:
                    await message.answer(
                        _("☺️ Partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat",
                          lang_code=user_language))

                # Message for partner
                try:
                    await bot.send_message(
                        partner_id,
                        _("☺️ Partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat",
                          lang_code=partner_language))
                except Exception as e:
                    logger.error(
                        f"Failed to send match message to partner {partner_id}: {e}"
                    )

    except Exception as e:
        logger.error(
            f"Unhandled error in handle_non_vip_search for user {user_id}: {e}",
            exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))
    finally:
        if user_id in non_vip_search_locks and non_vip_search_locks[user_id]:
            non_vip_search_locks[user_id] = False


@router.callback_query(F.data.startswith("gender:"))
async def gender_callback(query: types.CallbackQuery, bot: Bot,
                          state: FSMContext):
    """Handles gender selection callback."""
    # Assuming the format is "gender:context:gender_value"
    parts = query.data.split(":")
    context = parts[1] if len(
        parts) > 2 else "start"  # Default context to "start"
    gender = parts[-1]  # Always take the last part as gender

    user_id = query.from_user.id
    user_language = 'en'  # Initialize with a default language

    # Always answer the callback query to dismiss the loading state
    await query.answer()

    try:
        async with db_pool.acquire() as conn:

            # Fetch user's language using the established connection
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            # Update gender in the database
            await conn.execute(
                "UPDATE users SET gender = $1 WHERE user_id = $2", gender,
                user_id)
            logger.info(f"User {user_id} gender updated to {gender}.")

            if context == "change":
                await query.message.answer(
                    _("✅ Gender updated!", lang_code=user_language))

            # This delete ensures the inline keyboard is removed after selection
            await bot.delete_message(chat_id=query.message.chat.id,
                                     message_id=query.message.message_id)

            if context == "start":
                await query.message.answer(
                    _("🔢 Please enter your age:", lang_code=user_language))
                # If using FSM, set the state here to await age input
                # await state.set_state(UserStates.waiting_for_age)

    except Exception as e:
        logger.error(f"Database error updating gender for user {user_id}: {e}",
                     exc_info=True)
        await query.message.answer(
            _("❌ An unexpected error occurred while saving your gender. Please try again later.",
              lang_code=user_language))


@router.message(Command("next"))
async def next_command(message: types.Message, bot: Bot):
    """Handle /next by disconnecting and routing based on VIP status."""
    global search_queue, current_chats, non_vip_search_locks

    user_id = message.from_user.id
    logger.info(f"Next command from {user_id}.")

    user_language = 'en'  # Default fallback

    try:
        async with db_pool.acquire() as conn:
            if not conn:
                logger.error(
                    "Failed to establish DB connection in next_command for user %s.",
                    user_id)
                await message.answer(
                    _("An internal error occurred. Please try again later.",
                      lang_code='en'))
                return

            # Fetch user's language for localization throughout the command
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Defaulting to 'en'."
                )

            # 1. Check ban status
            banned_info = await conn.fetchrow(
                """
                SELECT banned_until FROM banned_users
                WHERE user_id = $1 AND banned_until > CURRENT_TIMESTAMP
                """, user_id)

            if banned_info:
                banned_until: datetime = banned_info['banned_until']
                await message.answer(
                    _("🚫 You are banned until {banned_time}.",
                      lang_code=user_language).format(
                          banned_time=banned_until.strftime(
                              '%Y-%m-%d %H:%M:%S')))
                logger.info(f"User {user_id} is banned until {banned_until}.")
                return

            # 2. Disconnect from current chat (both users)
            if user_id in current_chats:
                partner_id = current_chats.pop(user_id)
                if partner_id in current_chats:
                    current_chats.pop(partner_id)
                logger.info(f"User {user_id} disconnected from {partner_id}.")

                partner_language = 'en'
                try:
                    partner_row = await conn.fetchrow(
                        "SELECT language FROM users WHERE user_id = $1",
                        partner_id)
                    if partner_row and partner_row['language']:
                        partner_language = partner_row['language']
                except Exception as db_e:
                    logger.error(
                        f"Error fetching partner language for {partner_id}: {db_e}",
                        exc_info=True)

                try:
                    await bot.send_message(
                        partner_id,
                        _("Your partner ended the chat. /search to find a new partner",
                          lang_code=partner_language),
                        reply_markup=search_menu_reply_keyboard(
                            partner_language))
                    await bot.send_message(
                        partner_id,
                        _("How was your experience with your last partner?",
                          lang_code=partner_language),
                        reply_markup=await feedback_keyboard(partner_id))
                except Exception as e:
                    logger.error(
                        f"Failed to notify partner {partner_id} about /next: {e}",
                        exc_info=True)

                await message.answer(
                    _("How was your experience with your last partner?",
                      lang_code=user_language),
                    reply_markup=await feedback_keyboard(user_id))
            else:
                await message.answer(
                    _("You're not currently in a chat.",
                      lang_code=user_language),
                    reply_markup=search_menu_reply_keyboard(user_language))
                logger.info(
                    f"User {user_id} used /next but was not in a chat.")

            # Remove user from search queue
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue
                               if uid != user_id]

            # 3. Check VIP status
            user_vip_info = await conn.fetchrow(
                "SELECT is_vip FROM users WHERE user_id = $1", user_id)
            is_vip = user_vip_info and user_vip_info["is_vip"]
            logger.info(f"User {user_id} VIP status: {is_vip}.")

            # 4. Route accordingly
            if is_vip:
                await quick_vip_search(message)
            else:
                await handle_non_vip_search(message, bot)

    except Exception as e:
        logger.error(f"Error in /next command for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


#async def show_vip_options(message: types.Message):
#await message.answer("Choose your VIP plan:", reply_markup=payment_method_keyboard)
PRIVACY_POLICY_CONTENT_EN = """
🔒 *Privacy Policy & Terms - Anonymous Chat Bot*
*Effective Date:* July 10, 2025

This document explains how *Anonymous Chat Bot* ("we", "our", "the Bot") collects, uses, and protects your data, and the terms you agree to by using the Bot.

_By using this bot, you confirm that you are 18+ and agree to all of the following terms and privacy practices. If you do not agree, please stop using the bot._

━━━━━━━━━━━━━━━
*1. Eligibility*
• You must be *at least 18 years old* to use this bot.
• If you're under 18, do not use the bot.

━━━━━━━━━━━━━━━
*2. Information We Collect*
We may collect the following data:
- 📍 Location (if shared)
- 🚻 Gender
- 🎂 Age or age range
- 💎 VIP status and payments (if applicable)
- 🆔 Telegram user ID and username
- ⚙️ Match preferences
- 🕒 Basic usage metadata (e.g., timestamps, match attempts)

We *do NOT store* or read chat messages between users.

━━━━━━━━━━━━━━━
*3. How Your Data is Used*
Your data is used to:
- Match users based on preferences
- Provide and manage VIP features
- Prevent spam and abuse
- Improve bot quality and safety

━━━━━━━━━━━━━━━
*4. Data Security*
- Data is stored securely on [your hosting provider]
- Access is restricted to authorized staff only
- We implement technical safeguards against misuse

━━━━━━━━━━━━━━━
*5. User Rules (Terms)*
• No harassment, spam, threats, illegal content
• Do not impersonate others or violate Telegram rules
• Violators may be banned without warning

━━━━━━━━━━━━━━━
*6. Data Sharing*
We do *not* sell or share your data, except:
- If required by law
- To prevent fraud or threats

━━━━━━━━━━━━━━━
*7. International Users*
Your data may be stored in or transferred to countries with different data protection laws.

━━━━━━━━━━━━━━━
*8. Your Rights*
You can:
- View or correct your data
- Delete your data at any time with `/delete`
- Stop using the bot any time

━━━━━━━━━━━━━━━
*9. Consent*
By using this bot, you confirm:
- You are 18 or older
- You consent to this Privacy Policy and Terms
- You understand how your data is handled

━━━━━━━━━━━━━━━
*10. Changes to Policy*
We may update this policy. Material changes will be announced via the bot. Continued use = acceptance.

━━━━━━━━━━━━━━━
*11. Contact*
For privacy or legal concerns:
📧 Email: your@
📩 Telegram: @
"""


@router.message(Command("privacy"))
async def send_privacy(message: types.Message):
    user_id = message.from_user.id
    user_language = 'en'  # Initialize with a default language

    try:
        async with db_pool.acquire() as conn:
            # Fetch user's data to get their language
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            # Localize the entire privacy policy content using the fetched language
            localized_policy = _(PRIVACY_POLICY_CONTENT_EN,
                                 lang_code=user_language)
            await message.answer(localized_policy, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error sending privacy policy to user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred while retrieving the privacy policy. Please try again later.",
              lang_code=user_language))


# Use Markdown for formatting


@router.message(Command("vip"))
async def vip_command(message: Message):
    user_id = message.from_user.id
    user_language = 'en'  # Default fallback

    try:
        async with db_pool.acquire() as conn:

            # Fetch user data for language
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            result = await conn.fetchrow(
                "SELECT is_vip FROM users WHERE user_id = $1", user_id)

            if result and result["is_vip"]:
                await message.answer(_(
                    "🎉 You already have 💎 **VIP access**!\nEnjoy all premium features.",
                    lang_code=user_language),
                                     parse_mode="Markdown")
                logger.info(
                    f"User {user_id} tried to become VIP but already has access."
                )
                return

            gif = FSInputFile("media/Unlock VIP Access.gif")

            await message.answer_animation(animation=gif)

            text = _(
                "<b>💎 Become a VIP User</b>\n\n"
                "<b>🧭 Search in Your Own City</b> – Meet people nearby and spark local connections.\n"
                "<b>🚻 Gender-Based Searching</b> – Choose who you chat with.\n"
                "<b>⏳ Unlimited Searches</b> – No limits, keep exploring as much as you want.\n"
                "<b>🚫 Ad-Free Experience</b> – Enjoy chatting without any interruptions.\n"
                "<b>💎 Visible VIP Status</b> – Stand out with a special badge.\n"
                "<b>🤍 Support the Bot</b> – Help us grow and keep the service running.\n"
                "👨🏻‍💻 If you have any questions, do not hesitate to get in touch @YourBotSupport\n\n"
                "<b>Choose your preferred payment method:</b>",
                lang_code=user_language)

            builder = InlineKeyboardBuilder()
            builder.button(text=_("🧾 Telegram Payments",
                                  lang_code=user_language),
                           callback_data="pay_telegram")
            builder.button(text=_("💳 Chapa Payments", lang_code=user_language),
                           callback_data="pay_chapa")
            # Ensure vertical layout

            await message.answer(text,
                                 reply_markup=builder.as_markup(),
                                 parse_mode="HTML")
            logger.info(f"User {user_id} was shown VIP payment options.")

    except Exception as e:
        logger.error(f"Error in vip_command for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


@router.message(Command("userid"))
async def userid_command(message: Message):
    """Handles the /userid command."""
    user_id = message.from_user.id
    user_language = 'en'  # Default fallback

    try:
        async with db_pool.acquire() as conn:
            # Fetch user's language
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            await message.answer(
                _("Your User ID is: `{user_id}`",
                  lang_code=user_language).format(user_id=user_id),
                parse_mode="Markdown")
            logger.info(f"User {user_id} requested their user ID.")

    except Exception as e:
        logger.error(f"Error in userid_command for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred while retrieving your User ID. Please try again later.",
              lang_code=user_language))


class SettingsStates(StatesGroup):
    waiting_for_age = State()  # This line needs to be indented


@router.callback_query(F.data == "set_age")
async def ask_age(query: types.CallbackQuery, state: FSMContext):
    """
    Handles the 'set_age' callback, prompts the user for their age,
    and sets the FSM state to 'waiting_for_age'.
    """

    user_id = query.from_user.id
    user_language = 'en'  # Fallback default

    await query.answer()  # Always answer the callback query

    try:
        async with db_pool.acquire() as conn:
            # Fetch user's language from DB
            user_data = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            user_language = user_data[
                'language'] if user_data and user_data['language'] else 'en'
            logger.info(f"User {user_id} language: {user_language}")

        await query.message.answer(
            _("🔢 Please enter your age:", lang_code=user_language))
        await state.set_state(SettingsStates.waiting_for_age)
        logger.info(
            f"User {user_id} initiated age setting and state set to waiting_for_age."
        )

    except Exception as e:
        logger.error(f"Error in ask_age for user {user_id}: {e}",
                     exc_info=True)
        await query.message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code='en'))

    @router.message(SettingsStates.waiting_for_age)
    async def age_input_handler(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        text = message.text.strip()

        if text.isdigit():
            age = int(text)
            if 10 <= age <= 100:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE users SET age = $1 WHERE user_id = $2",
                            age, user_id)
                    await message.answer(_(
                        "✅ Your age has been set to: **{age}**",
                        user_id).format(age=age),
                                         parse_mode="Markdown")
                    logger.info(
                        f"User {user_id} successfully set age to {age}.")
                except Exception as e:
                    logger.error(f"Error updating age for user {user_id}: {e}",
                                 exc_info=True)
                    await message.answer(
                        _(
                            "❌ An error occurred while saving your age. Please try again later.",
                            user_id))
                await state.clear()
            else:
                await message.answer(
                    _("❌ Please enter a valid age between 10 and 100.",
                      user_id))
                logger.warning(f"User {user_id} entered invalid age: {age}.")
        else:
            await message.answer(
                _("❌ Please enter a valid numeric age.", user_id))
            logger.warning(
                f"User {user_id} entered non-numeric age: '{text}'.")


# Get the logger from the main application

# Helper function to convert date to timezone-aware datetime for comparison


# Globals
@router.message(lambda message: message.text == "🏙️ Search by City")
async def search_by_city_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    user_language = "en"  # default language

    try:
        async with db_pool.acquire() as conn:
            if not conn:
                logger.error(
                    "Failed to connect to DB in search_by_city_handler.")
                await message.answer(
                    _("An internal error occurred. Please try again later.",
                      lang_code=user_language))
                return

            user_row = await conn.fetchrow(
                "SELECT is_vip, vip_expires_at, location, language FROM users WHERE user_id = $1",
                user_id)

            if user_row and user_row.get("language"):
                user_language = user_row["language"]

            if not user_row or not user_row['is_vip'] or \
               (user_row['vip_expires_at'] and user_row['vip_expires_at'] < datetime.now(timezone.utc)):
                await message.answer(
                    _("💎 City-based matching is a **VIP-only feature**.\nBecome a /vip member to unlock it!",
                      lang_code=user_language))
                logger.info(
                    f"User {user_id} tried city search without active VIP.")
                return

            user_location = user_row['location']
            if not user_location:
                await message.answer(
                    _("📍 Please share your location first using the /setlocation command.",
                      lang_code=user_language))
                logger.info(
                    f"User {user_id} tried city search but has no location set."
                )
                return

            if user_id in current_chats:
                await message.answer(
                    _("⚠️ You're already in a chat. Use /stop to end it first before searching.",
                      lang_code=user_language))
                logger.info(
                    f"User {user_id} tried city search while in an active chat."
                )
                return

            if any(user_id == uid for uid, _, _ in search_queue):
                await message.answer(
                    _("⏳ You're already searching. Please wait or use /stop to cancel.",
                      lang_code=user_language))
                logger.info(
                    f"User {user_id} tried city search but is already in the queue."
                )
                return

            city = user_location.strip()

            # Remove any previous presence in queue
            search_queue[:] = [(uid, ts, loc) for uid, ts, loc in search_queue
                               if uid != user_id]
            search_queue.append((user_id, time.time(), city))
            logger.info(
                f"User {user_id} added to city search queue for city: {city}.")

            searching_msg = await message.answer(
                _("🔍 Searching for a partner in your city...",
                  lang_code=user_language))

            match_found = False
            partner_id = None
            partner_is_vip = False

            shuffled_queue = list(search_queue)
            random.shuffle(shuffled_queue)

            # 1. First try matching with a VIP user
            for p_id, _ignored_timestamp, p_city in shuffled_queue:
                if p_id != user_id and p_city == city and p_id not in current_chats:
                    partner_row = await conn.fetchrow(
                        "SELECT is_vip, vip_expires_at FROM users WHERE user_id = $1",
                        p_id)
                    if partner_row and partner_row['is_vip'] and \
                       (partner_row['vip_expires_at'] and partner_row['vip_expires_at'] > datetime.now(timezone.utc)):
                        partner_id = p_id
                        partner_is_vip = True
                        match_found = True
                        break

            # 2. If no VIP found, try matching with a non-VIP user
            if not match_found:
                for p_id, _ignored_timestamp, p_city in shuffled_queue:
                    if p_id != user_id and p_city == city and p_id not in current_chats:
                        partner_row = await conn.fetchrow(
                            "SELECT is_vip FROM users WHERE user_id = $1",
                            p_id)
                        if partner_row and not partner_row['is_vip']:
                            partner_id = p_id
                            partner_is_vip = False
                            match_found = True
                            break

            if match_found:
                current_chats[user_id] = partner_id
                current_chats[partner_id] = user_id
                logger.info(
                    f"City match: {user_id} matched with {partner_id} in {city}. Partner VIP: {partner_is_vip}"
                )

                try:
                    await bot.delete_message(
                        chat_id=user_id, message_id=searching_msg.message_id)
                except Exception as e:
                    logger.error(
                        f"Failed to delete search message for user {user_id}: {e}"
                    )

                message_text = (_(
                    "💎 **VIP City Match Found!** You're now chatting with another **VIP** member in your city.\n\n",
                    lang_code=user_language
                ) if partner_is_vip else _(
                    "🏙️ **City Match Found!** You're now chatting with someone in your city.\n\n",
                    lang_code=user_language))

                await bot.send_message(
                    partner_id,
                    message_text +
                    "/next — find a new partner\n/stop — end chat",
                    parse_mode=ParseMode.HTML)
                await message.answer(
                    message_text +
                    "/next — find a new partner\n/stop — end chat",
                    parse_mode=ParseMode.HTML)

                search_queue[:] = [(uid, ts, loc)
                                   for uid, ts, loc in search_queue
                                   if uid not in (user_id, partner_id)]
                return

            else:
                try:
                    await bot.delete_message(
                        chat_id=user_id, message_id=searching_msg.message_id)
                except Exception as e:
                    logger.error(
                        f"Failed to delete search message for user {user_id} (no match): {e}"
                    )

                await message.answer(
                    _("😔 No active users are available in your city right now. You'll stay in the search queue and be matched as soon as someone nearby becomes available.",
                      lang_code=user_language))
                logger.info(
                    f"No match found for user {user_id} in {city}. Remaining in queue."
                )

    except Exception as e:
        logger.error(
            f"An unexpected error occurred in search_by_city_handler for user {user_id}: {e}",
            exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred while searching for a city partner. Please try again later.",
              lang_code=user_language))


# Common handler logic
async def handle_fallback(message: Message):
    """
    Handles messages from users who are not currently in a chat,
    informing them to start a new chat.
    """
    user_id = message.from_user.id
    user_language = 'en'  # Initialize with a default language

    try:
        async with db_pool.acquire() as conn:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            if user_id not in current_chats:
                await message.answer(
                    _("🤖 You're not in a chat right now.\n\nTap /Search to start chatting.",
                      lang_code=user_language))
                logger.info(
                    f"User {user_id} sent a message but was not in a chat; fallback message sent."
                )

    except Exception as e:
        logger.error(f"Error in handle_fallback for user {user_id}: {e}",
                     exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


@router.message(F.text)
async def chat_handler(message: types.Message, bot: Bot):
    """Handles chat messages."""
    user_id = message.from_user.id
    if user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_message(partner_id, message.text)


@router.message(F.photo)
async def photo_handler(message: types.Message, bot: Bot):
    """Handles photo messages."""
    user_id = message.from_user.id
    if user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_photo(partner_id, message.photo[-1].file_id)


@router.message(F.video)
async def video_handler(message: types.Message, bot: Bot):
    """Handles video messages."""
    user_id = message.from_user.id
    if user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_video(partner_id, message.video.file_id)


@router.message(F.voice)
async def voice_handler(message: types.Message, bot: Bot):
    """Handles voice messages."""
    user_id = message.from_user.id
    if user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_voice(partner_id, message.voice.file_id)


@router.message(F.document)
async def document_handler(message: types.Message, bot: Bot):
    """Handles document messages."""
    user_id = message.from_user.id
    if user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_document(partner_id, message.document.file_id)


@router.message(F.animation)
async def animation_handler(message: types.Message, bot: Bot):
    """Handles GIF messages."""
    user_id = message.from_user.id
    if user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_animation(partner_id, message.animation.file_id)


@router.message(F.photo)
async def payment_proof_handler(message: types.Message, bot: Bot):
    """Handles payment proof photo."""
    user_id = message.from_user.id
    admin_id = config.ADMIN_USER_ID

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO subscription_requests (user_id, payment_proof, request_date, status)
            VALUES ($1, $2, now(), $3)
            """, user_id, message.photo[-1].file_id, "pending")

    await bot.send_photo(admin_id,
                         message.photo[-1].file_id,
                         caption=f"User {user_id} requests VIP.")
    await message.answer("Your request has been sent to the admin.")


@router.message(Command("approve_vip"))
async def approve_vip_command(message: types.Message, bot: Bot):
    """Handles the /approve_vip command."""
    if message.from_user.id != config.ADMIN_USER_ID:
        await message.answer("You are not authorized to use this command.")
        return

    try:
        user_id = int(message.text.split()[1])
    except (ValueError, IndexError):
        await message.answer("Usage: /approve_vip <user_id>")
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET is_vip = TRUE,
                subscription_expiry = now() + interval '30 days'
            WHERE user_id = $1
            """, user_id)
        await conn.execute(
            """
            UPDATE subscription_requests
            SET status = 'approved'
            WHERE user_id = $1
            """, user_id)

    await message.answer(f"User {user_id} VIP approved.")
    await bot.send_message(user_id, "Your VIP status has been approved!")


@router.message(Command("reject_vip"))
async def reject_vip_command(message: types.Message, bot: Bot):
    """Handles the /reject_vip command."""
    if message.from_user.id != config.ADMIN_USER_ID:
        await message.answer("You are not authorized to use this command.")
        return

    try:
        user_id = int(message.text.split()[1])
    except (ValueError, IndexError):
        await message.answer("Usage: /reject_vip <user_id>")
        return

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE subscription_requests
            SET status = 'rejected'
            WHERE user_id = $1
            """, user_id)

    await message.answer(f"User {user_id} VIP rejected.")
    await bot.send_message(user_id, "Your VIP request has been rejected.")


@router.message(F.voice)
async def vip_voice_handler(message: types.Message, bot: Bot):
    """Handles VIP voice messages."""
    user_id = message.from_user.id

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT is_vip FROM users WHERE user_id = $1", user_id)

    is_vip = row and row["is_vip"]

    if is_vip and user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_voice(partner_id, message.voice.file_id)
    elif not is_vip:
        await message.answer(
            "This is a VIP feature. Become a /VIP to use voice messages.")


@router.message(Command("voicecall"))
async def voice_call_command(message: types.Message, bot: Bot):
    """Handles the /voicecall command (simulated)."""
    user_id = message.from_user.id

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT is_vip FROM users WHERE user_id = $1", user_id)

    is_vip = row and row["is_vip"]

    if is_vip and user_id in current_chats:
        partner_id = current_chats[user_id]
        await message.answer("📞 Initiating voice call (simulated).")
        await bot.send_message(partner_id,
                               "📞 Incoming voice call (simulated).")
    elif not is_vip:
        await message.answer(
            "This is a /VIP feature. Become a VIP to use voice calls.")
    else:
        await message.answer("You are not currently in a chat.")


# In your webhook/handlers.py file


# In your webhook/handlers.py file
async def create_tables():
    """
    Creates necessary database tables if they don't exist.
    """

    try:
        async with db_pool.acquire() as conn:
            # One execute per statement
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    gender TEXT,
                    age INTEGER,
                    location TEXT,
                    is_vip BOOLEAN DEFAULT FALSE,
                    subscription_expiry TIMESTAMP,
                    pending_vip BOOLEAN DEFAULT FALSE,
                    credit INTEGER DEFAULT 0,
                    vip_expires_at TIMESTAMP WITH TIME ZONE,
                    last_search_date DATE,
                    search_count INTEGER DEFAULT 0,
                    vip_plan TEXT,
                    notified_before_expiry BOOLEAN DEFAULT FALSE,
                    language TEXT DEFAULT 'en'
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS subscription_requests (
                    request_id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id),
                    payment_proof TEXT,
                    request_date TIMESTAMP,
                    status TEXT
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chapa_payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    tx_ref TEXT NOT NULL UNIQUE,
                    plan TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    amount NUMERIC(10, 2) NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id BIGINT PRIMARY KEY,
                    banned_until TIMESTAMP WITH TIME ZONE NOT NULL,
                    reason TEXT
                );
            """)

            logger.info("✅ Database tables created or already exist.")

    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        raise


async def feedback_keyboard(user_id: int) -> InlineKeyboardMarkup:
    user_language = 'en'

    async with db_pool.acquire() as conn:
        try:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
        except Exception as e:
            logger.error(f"Error fetching language for {user_id}: {e}",
                         exc_info=True)

    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text=_("👍", lang_code=user_language),
                                 callback_data="feedback_good"),
            InlineKeyboardButton(text=_("👎", lang_code=user_language),
                                 callback_data="feedback_bad")
        ],
                         [
                             InlineKeyboardButton(
                                 text=_("⚠️ Report", lang_code=user_language),
                                 callback_data="feedback_report")
                         ]])


@router.callback_query(F.data == "feedback_good")
async def feedback_good(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = 'en'

    async with db_pool.acquire() as conn:
        try:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            await callback.answer(_(
                "Your feedback has been submitted successfully.",
                lang_code=user_language),
                                  show_alert=True)

            logger.info(f"User {user_id} submitted 'good' feedback.")

            try:
                await callback.message.delete()
            except Exception as e:
                logger.error(
                    f"Failed to delete feedback message for user {user_id}: {e}"
                )

        except Exception as e:
            logger.error(f"Error in feedback_good for user {user_id}: {e}",
                         exc_info=True)
            await callback.answer(_(
                "❌ An unexpected error occurred while submitting your feedback. Please try again later.",
                lang_code=user_language),
                                  show_alert=True)


@router.callback_query(F.data == "feedback_bad")
async def feedback_bad(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = 'en'

    async with db_pool.acquire() as conn:
        try:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            await callback.answer(_(
                "Your feedback has been submitted successfully.",
                lang_code=user_language),
                                  show_alert=True)

            logger.info(f"User {user_id} submitted 'bad' feedback.")

            try:
                await callback.message.delete()
            except Exception as e:
                logger.error(
                    f"Failed to delete feedback message for user {user_id}: {e}"
                )

        except Exception as e:
            logger.error(f"Error in feedback_bad for user {user_id}: {e}",
                         exc_info=True)
            await callback.answer(_(
                "❌ An unexpected error occurred while submitting your feedback. Please try again later.",
                lang_code=user_language),
                                  show_alert=True)


async def get_report_reasons_keyboard(user_id: int) -> InlineKeyboardMarkup:
    user_language = 'en'

    try:
        async with db_pool.acquire() as conn:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
    except Exception as e:
        logger.error(
            f"Error generating report reasons keyboard for user {user_id}: {e}",
            exc_info=True)

    # Define the inline keyboard with one button per row
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=_("📢 Advertising",
                                        lang_code=user_language),
                                 callback_data="report_advertising")
        ],
        [
            InlineKeyboardButton(text=_("💰 Selling", lang_code=user_language),
                                 callback_data="report_selling")
        ],
        [
            InlineKeyboardButton(text=_("🔞 Child Pornography",
                                        lang_code=user_language),
                                 callback_data="report_childporn")
        ],
        [
            InlineKeyboardButton(text=_("🤲 Begging", lang_code=user_language),
                                 callback_data="report_begging")
        ],
        [
            InlineKeyboardButton(text=_("😡 Insult", lang_code=user_language),
                                 callback_data="report_insult")
        ],
        [
            InlineKeyboardButton(text=_("🪓 Violence", lang_code=user_language),
                                 callback_data="report_violence")
        ],
        [
            InlineKeyboardButton(text=_("🌍 Racism", lang_code=user_language),
                                 callback_data="report_racism")
        ],
        [
            InlineKeyboardButton(text=_("🤬 Vulgar Partner",
                                        lang_code=user_language),
                                 callback_data="report_vulgar")
        ],
        [
            InlineKeyboardButton(text=_("🔙 Back", lang_code=user_language),
                                 callback_data="feedback_keyboard")
        ]
    ])


@router.callback_query(F.data == "feedback_report")
async def feedback_report(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = 'en'

    await callback.answer()

    async with db_pool.acquire() as conn:
        try:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            await callback.message.edit_text(
                text=_("⚠️ Please select a reason to report your partner:",
                       lang_code=user_language),
                reply_markup=await get_report_reasons_keyboard(user_id))
            logger.info(f"User {user_id} requested report reasons.")

        except Exception as e:
            logger.error(
                f"Failed to update message with report reasons for user {user_id}: {e}",
                exc_info=True)
            await callback.answer(_(
                "❌ An unexpected error occurred while showing report options. Please try again later.",
                lang_code=user_language),
                                  show_alert=True)


@router.callback_query(F.data == "feedback_keyboard")
async def handle_feedback_main(callback: CallbackQuery):
    user_id = callback.from_user.id
    user_language = 'en'

    await callback.answer()

    async with db_pool.acquire() as conn:
        try:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            await callback.message.edit_reply_markup(
                reply_markup=await feedback_keyboard(user_id))
            logger.info(f"User {user_id} returned to main feedback keyboard.")

        except Exception as e:
            logger.error(
                f"Failed to edit message to main feedback keyboard for user {user_id}: {e}",
                exc_info=True)
            await callback.answer(_(
                "❌ An unexpected error occurred. Please try again later.",
                lang_code=user_language),
                                  show_alert=True)


@router.callback_query(F.data.startswith("report_"))
async def handle_report_reason(callback: CallbackQuery):
    user_id = callback.from_user.id
    reason = callback.data.replace("report_", "")
    reported_id = current_chats.get(user_id)
    user_language = 'en'

    await callback.answer()

    async with db_pool.acquire() as conn:
        try:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

            if reported_id:
                logger.info(
                    f"User {user_id} reported partner {reported_id} for reason: {reason}"
                )
                # Optionally save report to DB:
                # await conn.execute(
                #     "INSERT INTO reports (reporter_id, reported_id, reason, timestamp) VALUES ($1, $2, $3, NOW())",
                #     user_id, reported_id, reason
                # )
            else:
                logger.warning(
                    f"User {user_id} tried to report but no active partner found. Reason: {reason}"
                )

            await callback.message.edit_text(
                _("✅ Your report has been submitted. Thank you!",
                  lang_code=user_language))

        except Exception as e:
            logger.error(
                f"Error handling report reason for user {user_id}: {e}",
                exc_info=True)
            await callback.message.answer(
                _("❌ An unexpected error occurred while submitting your report. Please try again later.",
                  lang_code=user_language))


async def get_telegram_plans_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """
    Generates a localized inline keyboard for Telegram Stars payment plans.
    Fetches the user's language from the database using the connection pool.
    """
    builder = InlineKeyboardBuilder()
    user_language = 'en'  # default fallback

    try:
        async with db_pool.acquire() as conn:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get("language"):
                user_language = user_data["language"]
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Using default 'en'."
                )

    except Exception as e:
        logger.error(
            f"Error fetching language for Telegram plans keyboard for user {user_id}: {e}",
            exc_info=True)

    # Generate the keyboard, whether we have a valid language or not
    builder.button(text=_("100 ⭐ / $1.99 a week", lang_code=user_language),
                   callback_data="tgpay_week")
    builder.button(text=_("250 ⭐ / $3.99 a month", lang_code=user_language),
                   callback_data="tgpay_1m")
    builder.button(text=_("1000 ⭐ / $19.99 a year", lang_code=user_language),
                   callback_data="tgpay_1y")
    builder.adjust(1)

    return builder.as_markup()


@router.callback_query(F.data == "pay_telegram")
async def choose_telegram_plan(callback: CallbackQuery):
    user_id = callback.from_user.id
    message = callback.message
    await callback.answer()

    user_language = 'en'

    try:
        async with db_pool.acquire() as conn:
            user_data = await get_user_credits(user_id, conn=conn)
            if user_data and user_data.get('language'):
                user_language = user_data['language']
            else:
                logger.warning(
                    f"Could not retrieve language for user {user_id}. Defaulting to 'en'."
                )

        # Generate the keyboard once, passing the known language
        keyboard = await get_telegram_plans_keyboard(user_id)

        await message.edit_text(
            text=_("💫 Choose your plan with Telegram Stars:",
                   lang_code=user_language),
            reply_markup=keyboard)
        logger.info(f"User {user_id} was shown Telegram Stars payment plans.")

    except Exception as e:
        logger.error(
            f"Failed to show Telegram Stars plans to user {user_id}: {e}",
            exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


# Inside your chapa_payment_callback function:
def get_chapa_plans_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """
    Generates a localized inline keyboard for Chapa payment plans.
    """
    builder = InlineKeyboardBuilder()
    builder.button(
        text=_("1 Month - 400 ETB", user_id),  # Localized button text
        callback_data="chapa_1m")
    builder.button(
        text=_("6 Months - 1500 ETB", user_id),  # Localized button text
        callback_data="chapa_6m")
    builder.button(
        text=_("1 Year - 2500 ETB", user_id),  # Localized button text
        callback_data="chapa_1y")
    # Arrange buttons in a column
    builder.adjust(1)
    return builder.as_markup()


# --- Corrected choose_chapa_plan ---
@router.callback_query(F.data == "pay_chapa")
async def choose_chapa_plan(callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer()

    user_language = 'en'  # Default fallback

    try:
        # Use the connection pool
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT language FROM users WHERE user_id = $1", user_id)
            if row and row["language"]:
                user_language = row["language"]
            else:
                logger.warning(
                    f"No language found for user {user_id}, defaulting to 'en'."
                )

        # Show Chapa plans
        await callback.message.edit_text(
            text=_("Choose your Chapa plan:", lang_code=user_language),
            reply_markup=get_chapa_plans_keyboard(user_language))
        logger.info(
            f"User {user_id} was shown Chapa plans in '{user_language}'.")

    except Exception as e:
        logger.error(f"Failed to show Chapa plans to user {user_id}: {e}",
                     exc_info=True)
        await callback.message.answer(
            _("❌ An unexpected error occurred. Please try again later.",
              lang_code=user_language))


# --- Corrected handle_chapa_plan ---
@router.callback_query(F.data.startswith("chapa_"))
async def handle_chapa_plan(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_callback_data = callback.data
    tx_ref = str(uuid.uuid4())

    # Define available plans
    prices = {
        "chapa_1m": {
            "amount": 400.00,
            "name": "1 Month VIP"
        },
        "chapa_6m": {
            "amount": 1500.00,
            "name": "6 Months VIP"
        },
        "chapa_1y": {
            "amount": 2500.00,
            "name": "1 Year VIP"
        },
    }

    plan_info = prices.get(selected_callback_data)
    if not plan_info:
        await callback.answer(_("Invalid plan selected.", lang_code="en"),
                              show_alert=True)
        logger.warning(
            f"User {user_id} selected invalid Chapa plan: {selected_callback_data}"
        )
        return

    vip_amount = plan_info["amount"]
    vip_plan_name = plan_info["name"]

    await callback.answer(_("Preparing Chapa payment...", lang_code="en"))

    # Get user language
    user_language = "en"
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT language FROM users WHERE user_id = $1", user_id)
        if row and row["language"]:
            user_language = row["language"]

    # Create payment session
    async with aiohttp.ClientSession() as session:
        headers = {
            "Authorization": f"Bearer {CHAPA_SECRET_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "amount": str(vip_amount),
            "currency": "ETB",
            "email": "salahadinshemsu0@gmail.com",
            "first_name": f"user_{user_id}",
            "tx_ref": tx_ref,
            "callback_url": CHAPA_CALLBACK_URL,
            "return_url": "https://t.me/Selameselambot",
            "customization": {
                "title":
                _("VIP Subscription", lang_code=user_language),
                "description":
                _("Unlock VIP features in the bot", lang_code=user_language)
            }
        }

        try:
            async with session.post(CHAPA_BASE_URL,
                                    json=payload,
                                    headers=headers) as resp:
                text_response = await resp.text()

                try:
                    data = await resp.json()
                except Exception as e_json:
                    logger.error(
                        f"Invalid JSON from Chapa for user {user_id}: {text_response}",
                        exc_info=True)
                    await callback.message.answer(
                        _("❌ Chapa's response was unreadable. Please try again later.",
                          lang_code=user_language))
                    return

                if resp.status == 200 and data.get("status") == "success":
                    payment_url = data["data"]["checkout_url"]

                    # Save payment record
                    try:
                        async with db_pool.acquire() as conn:
                            await conn.execute(
                                """
                                INSERT INTO chapa_payments (user_id, tx_ref, plan, amount, status)
                                VALUES ($1, $2, $3, $4::NUMERIC, $5);
                                """, user_id, tx_ref, vip_plan_name,
                                vip_amount, 'pending')
                            logger.info(
                                f"Chapa payment saved for {user_id}: {vip_plan_name}, tx_ref={tx_ref}"
                            )
                    except Exception as db_error:
                        logger.error(
                            f"DB error saving Chapa payment for {user_id}: {db_error}",
                            exc_info=True)
                        await callback.message.answer(
                            _("⚠ Payment prepared, but failed to save record. Please contact support.",
                              lang_code=user_language))
                        return

                    # Show payment button
                    builder = InlineKeyboardBuilder()
                    builder.button(text=_("✅ Pay with Chapa",
                                          lang_code=user_language),
                                   url=payment_url)
                    await callback.message.edit_text(
                        text=_(
                            "💳 Click below to complete your payment securely:",
                            lang_code=user_language),
                        reply_markup=builder.as_markup())
                else:
                    error_msg = data.get(
                        "message",
                        "Unknown error") if data else "No response body"
                    logger.error(
                        f"Chapa error for {user_id}. Status: {resp.status}, Body: {text_response}"
                    )
                    await callback.message.answer(
                        _("❌ Failed to create payment. Please try again later.",
                          lang_code=user_language))

        except aiohttp.ClientError as e_http:
            logger.error(f"HTTP error with Chapa for user {user_id}: {e_http}",
                         exc_info=True)
            await callback.message.answer(
                _("❌ Network error during payment initiation. Please check your connection and try again.",
                  lang_code=user_language))
        except Exception as e:
            logger.error(
                f"Unexpected error during Chapa payment for user {user_id}: {e}",
                exc_info=True)
            await callback.message.answer(
                _("❌ An unexpected error occurred while processing your payment. Please try again later.",
                  lang_code=user_language))


# Assuming this is in db_utils.py or handlers.py
# You'll need your database connection function here
# from .db_utils import create_database_connection # Example import if in a separate file

# ... other Chapa related settings ...

# Define your Chapa plan details


# --- Your existing calculate_expiry_date function ---
def calculate_expiry_date(plan: str) -> datetime:
    """
    Calculates the VIP subscription expiry date based on the plan name.
    """
    now = datetime.now(timezone.utc)  # Use UTC for consistency

    if "1 Week VIP" in plan or "7 Days VIP" in plan:  # Added 7 Days VIP for Chapa consistency
        return now + timedelta(days=7)
    elif "1 Month VIP" in plan or "30 Days VIP" in plan:  # Added 30 Days VIP
        return now + timedelta(days=30)
    elif "3 Months VIP" in plan or "90 Days VIP" in plan:
        return now + timedelta(days=90)
    elif "6 Months VIP" in plan or "180 Days VIP" in plan:
        return now + timedelta(days=180)
    elif "1 Year VIP" in plan or "365 Days VIP" in plan:  # Added 365 Days VIP
        return now + timedelta(days=365)
    logger.warning(f"Unknown VIP plan '{plan}'. Defaulting to 7 days expiry.")
    return now + timedelta(days=7)


async def grant_vip_access(user_id: int, source_type: str,
                           payment_detail: str) -> bool:
    """
    Grants VIP access to a user based on the payment source
    (Chapa or Telegram Stars) and the relevant payment detail.
    """
    logger.info(
        f"Attempting to grant VIP to user {user_id} via {source_type}.")

    # Maps for different payment systems
    CHAPA_PLANS = {
        7: ("7 Days VIP", timedelta(days=7)),
        30: ("30 Days VIP", timedelta(days=30)),
        90: ("90 Days VIP", timedelta(days=90)),
        180: ("180 Days VIP", timedelta(days=180)),
        365: ("365 Days VIP", timedelta(days=365)),
    }

    TELEGRAM_PLANS = {
        "premium_week_sub": ("1 Week VIP", timedelta(weeks=1)),
        "premium_month_sub": ("1 Month VIP", timedelta(days=30)),
        "premium_year_sub": ("1 Year VIP", timedelta(days=365)),
    }

    plan_name = None
    duration_delta = None

    if source_type == 'chapa':
        try:
            duration_days = int(payment_detail)
            if duration_days in CHAPA_PLANS:
                plan_name, duration_delta = CHAPA_PLANS[duration_days]
            else:
                logger.error(
                    f"Unknown Chapa duration: '{duration_days}' for user {user_id}"
                )
                return False
        except ValueError:
            logger.error(
                f"Invalid Chapa payment_detail for user {user_id}: '{payment_detail}'"
            )
            return False

    elif source_type == 'telegram_stars':
        if payment_detail in TELEGRAM_PLANS:
            plan_name, duration_delta = TELEGRAM_PLANS[payment_detail]
        else:
            logger.error(
                f"Unknown Telegram Stars payload: '{payment_detail}' for user {user_id}"
            )
            return False
    else:
        logger.error(
            f"Unknown payment source: '{source_type}' for user {user_id}")
        return False

    # Calculate expiry date from now
    now = datetime.now(timezone.utc)
    expiry_date = now + duration_delta

    async with db_pool.acquire() as conn:
        # Fetch current VIP expiry if exists
        row = await conn.fetchrow(
            "SELECT vip_expires_at FROM users WHERE user_id = $1", user_id)
        current_vip_expiry = row[
            "vip_expires_at"] if row and row["vip_expires_at"] else None

        final_expiry_date = expiry_date

        if current_vip_expiry and current_vip_expiry > now:
            # Extend from current expiry
            remaining_duration = expiry_date - now
            final_expiry_date = current_vip_expiry + remaining_duration
            logger.info(
                f"Extending VIP for user {user_id}: "
                f"Old expiry={current_vip_expiry}, Adding={remaining_duration}, New expiry={final_expiry_date}"
            )
        else:
            logger.info(
                f"Setting new VIP for user {user_id}: New expiry={expiry_date}"
            )

        # Update user record
        await conn.execute(
            """
            UPDATE users
            SET
                is_vip = TRUE,
                vip_plan = $1,
                vip_expires_at = $2,
                notified_before_expiry = FALSE
            WHERE user_id = $3
            """, plan_name, final_expiry_date, user_id)

        logger.info(f"VIP access granted to user {user_id}. "
                    f"Plan: {plan_name}, Expires: {final_expiry_date}")
        return True

    return False


async def check_and_deactivate_expired_vip(bot):
    """
    Checks for expired VIP subscriptions in the database and deactivates them.
    Also sends expiry notifications if 'notified_before_expiry' is FALSE.
    """

    try:
        async with db_pool.acquire() as conn:

            now_utc = datetime.now(timezone.utc)
            logger.info(f"Running VIP expiry check at {now_utc}.")

            # --- Phase 1: Notify users before expiry (e.g., 24 hours before) ---
            expiring_soon_threshold = now_utc + timedelta(hours=24)

            users_to_notify = await conn.fetch(
                """
                SELECT user_id, vip_expires_at, language
                FROM users
                WHERE is_vip = TRUE
                  AND notified_before_expiry = FALSE
                  AND vip_expires_at <= $1
                  AND vip_expires_at > $2
                """, expiring_soon_threshold, now_utc)

            for user_data in users_to_notify:
                user_id = user_data["user_id"]
                expires_at = user_data["vip_expires_at"]
                user_language = user_data["language"] or "en"

                time_until_expiry_hours = int(
                    (expires_at - now_utc).total_seconds() / 3600)

                try:
                    message_text = _(
                        "⏰ Your VIP subscription will expire in less than {hours} hours ({expiry_date})!\n\n"
                        "Don't lose access to exclusive features like city-based matching. Renew your VIP status now: /vip",
                        lang_code=user_language).format(
                            hours=time_until_expiry_hours,
                            expiry_date=expires_at.strftime(
                                "%Y-%m-%d %H:%M UTC"))

                    await bot.send_message(chat_id=user_id,
                                           text=message_text,
                                           parse_mode=ParseMode.HTML)

                    await conn.execute(
                        "UPDATE users SET notified_before_expiry = TRUE WHERE user_id = $1",
                        user_id)

                    logger.info(
                        f"Sent VIP expiry notification to user {user_id}. Expires at {expires_at}."
                    )

                except Exception as e:
                    logger.warning(
                        f"Could not send VIP expiry notification to user {user_id}: {e}",
                        exc_info=True)
                    # Don’t mark as notified if sending message failed

            # --- Phase 2: Deactivate expired VIPs ---
            expired_users = await conn.fetch(
                """
                SELECT user_id, vip_plan, language
                FROM users
                WHERE is_vip = TRUE AND vip_expires_at <= $1
                """, now_utc)

            for user_data in expired_users:
                user_id = user_data["user_id"]
                vip_plan = user_data["vip_plan"]
                user_language = user_data["language"] or "en"

                logger.info(
                    f"Deactivating VIP for user {user_id}. Plan: {vip_plan}. Expiry was in the past."
                )

                await conn.execute(
                    """
                    UPDATE users
                    SET is_vip = FALSE,
                        vip_expires_at = NULL,
                        vip_plan = NULL,
                        notified_before_expiry = FALSE
                    WHERE user_id = $1
                    """, user_id)

                try:
                    message_text = _(
                        "😔 Your VIP subscription has expired. You no longer have access to exclusive features.\n\n"
                        "Renew your VIP access anytime to unlock all premium benefits: /vip",
                        lang_code=user_language)

                    await bot.send_message(chat_id=user_id,
                                           text=message_text,
                                           parse_mode=ParseMode.HTML)

                    logger.info(f"Sent VIP expired message to user {user_id}.")

                except Exception as e:
                    logger.warning(
                        f"Could not send VIP expired message to user {user_id}: {e}",
                        exc_info=True)

    except Exception as e:
        logger.error(f"Error in check_and_deactivate_expired_vip task: {e}",
                     exc_info=True)


# handlers.py (Add this new function)
# In webhook/handlers.py (at the very top, with other imports)

# --- Chapa Webhook Handler ---


async def chapa_webhook_handler(request: web.Request):
    """
    Handles incoming webhook notifications from Chapa.
    """
    user_language = 'en'  # Default language

    try:
        data = await request.json()
        logger.info(f"Received Chapa webhook: {data}")
    except Exception as e:
        logger.error(f"Failed to parse Chapa webhook JSON: {e}")
        return web.Response(status=400,
                            text=_("Bad Request: Invalid JSON",
                                   lang_code=user_language))

    tx_ref = data.get("tx_ref")
    if not tx_ref:
        logger.warning("Chapa webhook received without tx_ref.")
        return web.Response(status=400,
                            text=_("Bad Request: Missing tx_ref",
                                   lang_code=user_language))

    try:
        async with db_pool.acquire() as conn:
            if not conn:
                logger.error(
                    "Failed to connect to DB for Chapa webhook verification.")
                return web.Response(status=500,
                                    text=_("Internal Server Error",
                                           lang_code=user_language))

            async with ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {config.CHAPA_SECRET_KEY}",
                    "Content-Type": "application/json"
                }
                async with session.get(f"{config.CHAPA_VERIFY_URL}{tx_ref}",
                                       headers=headers) as resp:
                    verify_data = await resp.json()
                    logger.info(
                        f"Chapa verification response for {tx_ref}: {verify_data}"
                    )

                    if resp.status == 200 and \
                       verify_data.get("status") == "success" and \
                       verify_data["data"]["status"] == "success":

                        user_id = None
                        try:
                            original_record = await conn.fetchrow(
                                "SELECT user_id, status, plan FROM chapa_payments WHERE tx_ref = $1",
                                tx_ref)

                            if original_record:
                                user_id = original_record['user_id']
                                current_status = original_record['status']
                                plan_name = original_record['plan']

                                if current_status != 'success':
                                    await conn.execute(
                                        "UPDATE chapa_payments SET status = $1 WHERE tx_ref = $2",
                                        'success', tx_ref)
                                    logger.info(
                                        f"Chapa payment for {tx_ref} (user {user_id}) confirmed as SUCCESS and DB chapa_payments updated."
                                    )

                                    duration_map = {
                                        "1 Week VIP": 7,
                                        "1 Month VIP": 30,
                                        "3 Months VIP": 90,
                                        "6 Months VIP": 180,
                                        "1 Year VIP": 365
                                    }
                                    chapa_duration_days = duration_map.get(
                                        plan_name, 7)

                                    vip_granted = await grant_vip_access(
                                        user_id, 'chapa',
                                        str(chapa_duration_days))

                                    if vip_granted:
                                        bot_instance = request.app["bot"]
                                        expiry_date_display = calculate_expiry_date(
                                            plan_name).strftime(
                                                '%Y-%m-%d %H:%M UTC')

                                        try:
                                            message_text = _(
                                                "🎉 Congratulations! Your 💎{plan_name}💎 VIP subscription has been activated! "
                                                "It will expire on **{expiry_date}**.",
                                                lang_code=user_language
                                            ).format(
                                                plan_name=plan_name,
                                                expiry_date=expiry_date_display
                                            )

                                            await bot_instance.send_message(
                                                chat_id=user_id,
                                                text=message_text,
                                                parse_mode=ParseMode.HTML)
                                            logger.info(
                                                f"VIP activation message sent to user {user_id}."
                                            )
                                        except Exception as send_err:
                                            logger.error(
                                                f"Failed to send VIP activation message to {user_id}: {send_err}",
                                                exc_info=True)
                                    else:
                                        logger.error(
                                            f"Failed to grant VIP access via grant_vip_access for user {user_id} after Chapa success."
                                        )
                                else:
                                    logger.info(
                                        f"Chapa payment {tx_ref} already marked as success. Skipping update."
                                    )
                            else:
                                logger.warning(
                                    f"Chapa payment for {tx_ref} not found in DB. Cannot update or activate VIP."
                                )

                        except Exception as db_update_err:
                            logger.error(
                                f"DB/VIP update error for Chapa webhook {tx_ref}: {db_update_err}",
                                exc_info=True)
                            return web.Response(
                                status=200,
                                text=
                                _("Webhook received, but internal DB/VIP update failed.",
                                  lang_code=user_language))

                    else:
                        chapa_data_status = verify_data.get('data', {}).get(
                            'status', 'N/A')
                        logger.warning(
                            f"Chapa verification failed for {tx_ref}. Status: {chapa_data_status}. Response: {verify_data}"
                        )
                        if chapa_data_status == 'failed':
                            await conn.execute(
                                "UPDATE chapa_payments SET status = $1 WHERE tx_ref = $2",
                                'failed', tx_ref)
                            logger.info(
                                f"Chapa payment for {tx_ref} marked as FAILED in DB."
                            )

                        return web.Response(
                            status=200,
                            text=
                            _(f"Verification failed or not success: {chapa_data_status}",
                              lang_code=user_language))

    except ClientError as ce:
        logger.error(
            f"Network error during Chapa verification for {tx_ref}: {ce}",
            exc_info=True)
        return web.Response(
            status=500,
            text=_("Internal Server Error: Network issue with Chapa API",
                   lang_code=user_language))
    except Exception as general_err:
        logger.error(
            f"Unexpected error in Chapa webhook handler for {tx_ref}: {general_err}",
            exc_info=True)
        return web.Response(status=500,
                            text=_("Internal Server Error: Unexpected issue",
                                   lang_code=user_language))

    return web.Response(status=200,
                        text=_("Webhook received and processed.",
                               lang_code=user_language))


# ... (other imports and code) ...


@router.callback_query(F.data.startswith("tgpay_"))
async def handle_tgpay_plan_selection(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    user_language = 'en'  # Default language for localization
    plan_callback_data = callback.data  # Initialize here to prevent UnboundLocalError in except block

    try:
        # Acquire DB connection from pool
        async with db_pool.acquire() as conn:
            if conn:
                user_language = await get_user_language_from_db(user_id, conn)
            else:
                logger.error(
                    f"Failed to get DB connection for user {user_id} in tgpay_plan_selection. Using default language."
                )
                # We'll proceed with the default 'en' if DB connection fails,
                # and send a generic error message later if sending the invoice fails.

        # 1. Acknowledge the callback immediately with a localized message
        await callback.answer(_("Preparing your invoice...",
                                lang_code=user_language),
                              show_alert=False)

        plan_details = PLAN_DETAILS.get(plan_callback_data)
        if not plan_details:
            logger.error(
                f"User {user_id} selected unknown Telegram Stars plan callback: {plan_callback_data}"
            )
            await callback.message.answer(
                _("An error occurred: Invalid plan selected. Please try again.",
                  lang_code=user_language))
            return

        amount = plan_details["amount"]
        title = _(plan_details["title_key"], lang_code=user_language)
        description = _(plan_details["description_key"],
                        lang_code=user_language)
        payload = plan_details["payload"]

        try:
            await callback.message.delete()
            logger.info(
                f"Deleted previous plan selection message for user {user_id}.")
        except Exception as e:
            logger.warning(
                f"Could not delete previous plan selection message for user {user_id}: {e}"
            )
            # Log the warning but continue

        await bot.send_message(
            chat_id=user_id,
            text=_("💫 Your invoice for **{title}** is ready!",
                   lang_code=user_language).format(title=title),
            parse_mode=ParseMode.HTML)
        logger.info(f"Sent 'invoice ready' message to user {user_id}.")

        await bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=description,
            payload=payload,
            currency="XTR",  # Correct currency code for Telegram Stars
            prices=[types.LabeledPrice(label=title, amount=amount)],
            provider_token=
            "",  # For Telegram Stars, the provider_token is an empty string
            is_flexible=False,
        )
        logger.info(
            f"Invoice for {title} ({amount} Stars) sent to user {user_id}.")

    except Exception as e:
        logger.error(
            f"Failed to send Stars invoice to {user_id} for {plan_callback_data}: {e}",
            exc_info=True)
        await bot.send_message(
            chat_id=user_id,
            text=
            _("Sorry, something went wrong while creating your invoice. Please try again later.",
              lang_code=user_language))


PLAN_DETAILS = {
    "tgpay_week": {
        "amount": 100,  # This is the amount in Telegram Stars
        "title_key":
        "weekly plan with telegram stars",  # This should be a key in your localization files
        "description_key":
        "Unlock weekly Premium",  # This should be a key in your localization files
        "payload": "vip_week_plan"
    },
    "tgpay_1m": {
        "amount": 250,
        "title_key": "Monthly plan with telegram stars",
        "description_key": "Unlock monthly Premium",
        "payload": "vip_month_plan"
    },
    "tgpay_1y": {
        "amount": 1000,
        "title_key": "Yearly plan with telegram stars",
        "description_key": "Unlock yearly Premium",
        "payload": "vip_year_plan"
    },
}


@router.callback_query(F.data.startswith("tgpay_"))
async def handle_tgpay_plan_selection(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    conn = None  # Initialize conn to None for finally block
    user_language = 'en'  # Default language for localization
    plan_callback_data = callback.data  # Initialize here to prevent UnboundLocalError in except block

    try:
        # Establish database connection to fetch user's language
        async with db_pool.acquire() as conn:
            user_language = await get_user_language_from_db(user_id, conn)

        # 1. Acknowledge the callback immediately with a localized message
        await callback.answer(_("Preparing your invoice...",
                                lang_code=user_language),
                              show_alert=False)

        plan_details = PLAN_DETAILS.get(plan_callback_data)
        if not plan_details:
            logger.error(
                f"User {user_id} selected unknown Telegram Stars plan callback: {plan_callback_data}"
            )
            await callback.message.answer(
                _("An error occurred: Invalid plan selected. Please try again.",
                  lang_code=user_language))
            return

        amount = plan_details["amount"]
        title = _(plan_details["title_key"], lang_code=user_language)
        description = _(plan_details["description_key"],
                        lang_code=user_language)
        payload = plan_details["payload"]

        try:
            # 2. Delete the message that contained the plan selection keyboard
            await callback.message.delete()
            logger.info(
                f"Deleted previous plan selection message for user {user_id}.")
        except Exception as e:
            logger.warning(
                f"Could not delete previous plan selection message for user {user_id}: {e}"
            )

        # 3. Send a *new* message confirming invoice readiness (localized)
        await bot.send_message(
            chat_id=user_id,
            text=_("💫 Your invoice for **{title}** is ready!",
                   lang_code=user_language).format(title=title),
            parse_mode=ParseMode.HTML)
        logger.info(f"Sent 'invoice ready' message to user {user_id}.")

        # 4. Send the actual invoice
        await bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=description,
            payload=payload,
            currency="XTR",
            prices=[types.LabeledPrice(label=title, amount=amount)],
            provider_token=
            "",  # For Telegram Stars, the provider_token is an empty string
            is_flexible=False,
        )
        logger.info(
            f"Invoice for {title} ({amount} Stars) sent to user {user_id}.")

    except Exception as e:
        logger.error(
            f"Failed to send Stars invoice to {user_id} for {plan_callback_data}: {e}",
            exc_info=True)
        await bot.send_message(
            chat_id=user_id,
            text=
            _("Sorry, something went wrong while creating your invoice. Please try again later.",
              lang_code=user_language))

    finally:
        # Ensure the database connection is closed
        if conn:
            await conn.close()
            logger.info(
                "Database connection closed for handle_tgpay_plan_selection.")


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    user_id = message.from_user.id
    payment_info = message.successful_payment
    invoice_payload = payment_info.invoice_payload

    logger.info(f"Successful payment received from {user_id}: {payment_info}")

    conn = None  # Initialize connection for finally block
    user_language = 'en'  # Default language

    try:
        # Fetch user's language from the database
        async with db_pool.acquire() as conn:
            user_language = await get_user_language_from_db(user_id, conn)

        # Call grant_vip_access for Telegram Stars payment
        if await grant_vip_access(user_id, 'telegram_stars', invoice_payload):
            plan_name_display = _(
                "VIP", lang_code=user_language)  # Default to localized "VIP"

            for key, details in PLAN_DETAILS.items():
                if details["payload"] == invoice_payload:
                    plan_name_display = _(details["title_key"],
                                          lang_code=user_language)
                    break

            # Calculate expiry date to show to user
            expiry_date_for_display = calculate_expiry_date(
                invoice_payload).strftime('%Y-%m-%d %H:%M UTC')

            # Send a localized success message
            await message.answer(_(
                "🎉 Congratulations! Your 💎**{plan_name_display}**💎 VIP subscription has been activated! It will expire on **{expiry_date_for_display}**.",
                lang_code=user_language).format(
                    plan_name_display=plan_name_display,
                    expiry_date_for_display=expiry_date_for_display),
                                 parse_mode=ParseMode.HTML)
            logger.info(
                f"User {user_id} successfully bought VIP with Stars via payload '{invoice_payload}'."
            )

        else:
            await message.answer(
                _("Thank you for your payment, but there was an issue granting your VIP access. Please contact support.",
                  lang_code=user_language))
            logger.error(
                f"Failed to grant VIP access for user {user_id} with payload '{invoice_payload}'."
            )

    except Exception as e:
        logger.error(
            f"An unexpected error occurred in successful_payment_handler for user {user_id}: {e}",
            exc_info=True)
        await message.answer(
            _("An unexpected error occurred. Please try again later or contact support.",
              lang_code=user_language))

    finally:
        if conn:
            await conn.close()
            logger.info(
                "Database connection closed for successful_payment_handler.")
