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
from utils.localization import _
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


async def create_database_connection():
    """Creates and returns an asynchronous database connection using asyncpg."""
    try:
        conn = await asyncpg.connect(config.DATABASE_URL)
        return conn
    except Exception as e:
        # It's good to log this error
        logger.error(f"Failed to connect to PostgreSQL database: {e}",
                     exc_info=True)
        raise  # Re-raise the exception so the calling code knows it failed


async def create_pool():
    return await asyncpg.create_pool(
        dsn=config.
        DATABASE_URL,  # Example: "postgresql://user:password@host/db"
        min_size=1,
        max_size=5)


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
        types.BotCommand(command="privacy", description="📜 View privacy,Terms and Conditions"),                 
    ]
    await bot.set_my_commands(commands)


def location_keyboard(user_id: int):
    """
    Creates a reply keyboard for location sharing, localized for the given user_id.
    """
    return types.ReplyKeyboardMarkup(keyboard=[[
        types.KeyboardButton(text=_("📍 Share Location", user_id), request_location=True)
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


@router.message(F.location)
async def location_handler(message: types.Message, bot: Bot):
    """Handles location sharing and saves city name."""
    user_id = message.from_user.id
    location = message.location
    lat, lon = location.latitude, location.longitude
    city = await get_city_from_coords(lat, lon)

    if not city:
        await message.answer(
            _("⚠️ Could not detect your city. Please try again later.", user_id),
            reply_markup=ReplyKeyboardRemove()
        )
        return

    conn = None
    try:
        conn = await create_database_connection()

        await conn.execute("UPDATE users SET location = $1 WHERE user_id = $2",
                           city, user_id) # Use user_id here

        logger.info(f"User {user_id} location updated to {city}.")

    except Exception as e:
        logger.error(
            f"Database error updating user location for {user_id}: {e}",
            exc_info=True
        )
        await message.answer(
            _("❌ An internal database error occurred while saving your location. Please try again.", user_id)
        )
        return
    finally:
        if conn:
            await conn.close()

    await message.answer(_(f"✅ Location set to: {city}", user_id),
                         reply_markup=ReplyKeyboardRemove())

    await set_commands(bot)

current_chats = {}


def gender_keyboard(user_id: int, context: str = "start"):
    """
    Creates an inline keyboard for gender selection, localized for the given user_id.
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=_("♂️ Male", user_id),
                callback_data=f"gender:{context}:male"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("♀️ Female", user_id),
                callback_data=f"gender:{context}:female"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("Any", user_id), # Changed 'any' to 'Any' for better capitalization
                callback_data=f"gender:{context}:any"
            )
        ],
    ])
    return keyboard


def location_keyboard(user_id: int):
    """
    Creates a reply keyboard for location sharing, localized for the given user_id.
    """
    keyboard = ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text=_("📍 Share Location", user_id), request_location=True)
    ]],
                                      resize_keyboard=True,
                                      one_time_keyboard=True)
    return keyboard

# Placeholder for set_commands and logger if not already imported/defined
async def set_commands(bot: Bot):
    # This is a placeholder. Replace with your actual implementation.
    logging.info("Setting commands for bot.")


logger = logging.getLogger(__name__)  # Use the logger for consistent logging


@router.message(CommandStart())
async def cmd_start(message: types.Message, bot: Bot):
    """Handles the /start command."""
    user_id = message.from_user.id
    logger.info(f"Received /start from user {user_id}")
    conn = None

    try:
        conn = await create_database_connection()

        user = await conn.fetchrow(
            "SELECT user_id, gender, age, location, language FROM users WHERE user_id = $1",
            user_id
        )
        logger.info(f"User data for {user_id}: {user}")

        # --- NEW LOGIC FOR LANGUAGE SELECTION ---
        if not user:
            # BRAND NEW USER: Ask for language first
            logger.info(f"New user {user_id}. Prompting for language selection.")
            # Insert user with a temporary language (or null) if your DB allows.
            # We will update it after language selection.
            # For simplicity, we'll insert with default 'en' and then update.
            await conn.execute("INSERT INTO users (user_id, language) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING",
                               user_id, 'en') # Insert with 'en' initially

            await message.answer(
                "👋 Welcome to the Anonymous Chat Bot!\n\n"
                "Please choose your preferred language:",
                reply_markup=language_keyboard() # Send the language selection keyboard
            )
            logger.info(f"Sent language selection keyboard to new user {user_id}.")
            return # IMPORTANT: Exit here, don't proceed with profile setup yet.

        # --- EXISTING USER LOGIC (as before, but now using fetched language) ---
        user_language = user['language'] if user and user['language'] else 'en'
        logger.info(f"User {user_id} exists. Language: {user_language}")

        if user['gender'] is None or user['age'] is None:
            logger.info(f"User {user_id} profile incomplete. Asking for gender.")
            await message.answer(
                _("⚠️ Your profile is incomplete. Please finish the setup.\n\n"
                  "Select your gender:", lang_code=user_language),
                reply_markup=gender_keyboard(user_id, context="start", lang_code=user_language)
            )
            logger.info(f"Sent gender keyboard for incomplete profile to {user_id}.")

        elif user['location'] is None:
            logger.info(f"User {user_id} location is None. Asking for location.")
            await message.answer(
                _("📍 Would you like to share your location for better matches?\n\n"
                  "This is optional, but helps us find people near you. If not, use /search command to find a match.", lang_code=user_language),
                reply_markup=location_keyboard(user_id, lang_code=user_language)
            )
            logger.info(f"Sent location keyboard for missing location to {user_id}.")

        else:
            logger.info(f"User {user_id} profile is complete. Sending welcome back message.")
            await message.answer(_("🎉 Welcome back! You're all set.", lang_code=user_language))
            logger.info(f"Sent welcome back message to {user_id}.")

        # Set commands for all languages (assuming set_commands handles this globally)
        await set_commands(bot)
        logger.info("Set commands for bot.")

    except Exception as e:
        logger.error(
            f"Error in cmd_start for user {user_id}: {e}",
            exc_info=True
        )
        # Fallback to English if language not set or error occurs
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.", lang_code='en')
        )
    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed.")

# --- NEW CALLBACK QUERY HANDLER FOR LANGUAGE SELECTION ---
@router.callback_query(F.data.startswith("lang_select:"))
async def language_selection_callback(call: CallbackQuery):
    user_id = call.from_user.id
    selected_lang_code = call.data.split(":")[1]
    logger.info(f"User {user_id} selected language: {selected_lang_code}")
    conn = None

    try:
        conn = await create_database_connection()
        if not conn:
            logger.error(f"Failed to get DB connection for user {user_id} in language_selection_callback.")
            await call.answer(_("❌ An internal error occurred. Please try again.", lang_code='en'))
            await call.message.edit_text(_("❌ An internal error occurred. Please try again later.", lang_code='en'))
            return

        # Update user's language in the database
        await conn.execute(
            "UPDATE users SET language = $1 WHERE user_id = $2",
            selected_lang_code, user_id
        )
        logger.info(f"Updated language for user {user_id} to {selected_lang_code}.")

        # Acknowledge the callback query to remove the loading state
        await call.answer(_("Language set successfully!", lang_code=selected_lang_code))

        # Edit the original message to remove the language selection keyboard
        await call.message.edit_text(
            _("Language set to **{lang}**.".format(lang=selected_lang_code), lang_code=selected_lang_code),
            parse_mode=ParseMode.HTML
        )

        # Now, proceed with the rest of the /start flow (asking for gender)
        await call.message.answer(
            _("👋 Welcome to the Anonymous Chat Bot! Let's get you set up.\n\n"
              "By using this bot, you confirm you're 18+ and agree to our Terms and Conditions (/privacy).\n\n"
              "Please select your gender:", lang_code=selected_lang_code),
            reply_markup=gender_keyboard(user_id, context="start", lang_code=selected_lang_code)
        )
        logger.info(f"Sent gender keyboard to {user_id} after language selection.")

    except Exception as e:
        logger.error(
            f"Error in language_selection_callback for user {user_id}: {e}",
            exc_info=True
        )
        await call.answer(_("❌ An unexpected error occurred. Please try again.", lang_code='en'))
        await call.message.edit_text(_("❌ An unexpected error occurred. Please try again later.", lang_code='en'))
    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed.")


# This list defines your supported languages and their display names
# You can define this globally or load from config
AVAILABLE_LANGUAGES = {
    'en': 'English 🇬🇧',
    'am': 'Amharic 🇪🇹',
    'or': 'Oromo 🇪🇹',
    'ar': 'العربية 🇸🇦' # Assuming a generic Arabic flag or adjust as needed
}

def language_keyboard() -> InlineKeyboardMarkup:
    """
    Creates an inline keyboard for language selection.
    This keyboard does NOT need localization itself, as it's for choosing a language.
    """
    builder = InlineKeyboardBuilder()
    for lang_code, lang_name in AVAILABLE_LANGUAGES.items():
        # Callback data format: "lang_select:<code>"
        builder.button(text=lang_name, callback_data=f"lang_select:{lang_code}")
    builder.adjust(2) # Adjust layout as desired
    return builder.as_markup()
async def get_user_language_from_db(user_id: int, conn) -> str:
    """
    Fetches user's language from DB. Defaults to 'en' if not found.
    This function is now defined directly within handlers.py.
    """
    if not conn:
        logger.warning("No DB connection provided to get_user_language_from_db. Defaulting to 'en'.")
        return 'en'
    try:
        language_code = await conn.fetchval(
            "SELECT language FROM users WHERE user_id = $1", user_id
        )
        return language_code if language_code else 'en'
    except Exception as e:
        logger.error(f"Error fetching language for user {user_id}: {e}")
        return 'en'
@router.callback_query(F.data == "set_language")
async def show_language_options_from_settings(call: CallbackQuery):
    user_id = call.from_user.id
    conn = None

    try:
        conn = await create_database_connection()
        if not conn:
            logger.error(f"Failed to get DB connection for user {user_id} in show_language_options_from_settings.")
            await call.answer(_("❌ An internal error occurred. Please try again.", lang_code='en'))
            await call.message.edit_text(_("❌ An internal error occurred. Please try again later.", lang_code='en'))
            return

        # Fetch user's current language to use for _() function if needed for prompt
        # (Though the language_keyboard itself doesn't need localization)
        user_language = await get_user_language_from_db(user_id, conn)

        # Acknowledge the callback query to remove the loading state
        await call.answer() # No alert message needed, just dismiss the loading

        # Edit the original message (the settings menu) to show language options
        await call.message.edit_text(
            _("Please choose your preferred language:", lang_code=user_language), # Localized prompt
            reply_markup=language_keyboard() # Your existing function to generate language buttons
        )
        logger.info(f"User {user_id} requested language change from settings.")

    except Exception as e:
        logger.error(
            f"Error in show_language_options_from_settings for user {user_id}: {e}",
            exc_info=True
        )
        await call.answer(_("❌ An unexpected error occurred. Please try again.", lang_code='en'))
        await call.message.edit_text(_("❌ An unexpected error occurred. Please try again later.", lang_code='en'))
    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed.")

@router.callback_query(F.data.startswith("gender:"))
async def gender_callback(query: types.CallbackQuery, bot: Bot):
    """Handles gender selection callback."""
    # Always answer the callback query to dismiss the loading state on the client
    await query.answer()

    user_id = query.from_user.id # Get user_id early for localization
    # Use tuple unpacking for cleaner code
    _, context, gender = query.data.split(":") # context and gender are already strings here

    conn = None # Initialize conn to None for safe cleanup
    try:
        conn = await create_database_connection()

        # Update user's gender in the database
        await conn.execute("UPDATE users SET gender = $1 WHERE user_id = $2",
                           gender, user_id)

        logger.info(f"User {user_id} gender updated to {gender}.")

        # Localize confirmation messages based on context
        if context == "change":
            await query.message.answer(_("✅ Gender updated!", user_id))
        elif context == "start": # Using elif for clarity and distinct actions
            await query.message.answer(_("🔢 Please enter your age:", user_id))

        # IMPORTANT: Ensure set_commands handles localization for all languages or is called per language.
        # As discussed, if your `set_commands` iterates through languages, this call is fine.
        await set_commands(bot)

    except Exception as e:
        logger.error(f"Database error updating gender for user {user_id}: {e}",
                     exc_info=True)
        await query.message.answer(
            _("❌ An unexpected error occurred while saving your gender. Please try again later.", user_id)
        )
    finally:
        if conn:
            await conn.close() # Ensure connection is closed

@router.message(F.text.isdigit())
async def age_handler(message: types.Message, bot: Bot):
    """Handles age input."""
    age = int(message.text)
    user_id = message.from_user.id
    conn = None  # Initialize conn to None for safe cleanup
    try:
        conn = await create_database_connection()

        # Update user's age in the database
        await conn.execute("UPDATE users SET age = $1 WHERE user_id = $2", age, user_id)

        logger.info(f"User {user_id} age updated to {age}.")

        await message.answer(_("✅ Your profile is complete!", user_id))
        await message.answer(
            _("📍 Would you like to share your location for better matches?\n\n"
              "This is optional, but helps us find people near you. If not, use /search command to find a match.", user_id),
            reply_markup=location_keyboard(user_id) # Pass user_id to location_keyboard
        )

        # Assuming set_commands handles setting commands for all languages globally
        await set_commands(bot)

    except Exception as e:
        logger.error(f"Database error updating age for user {user_id}: {e}", exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred while saving your age. Please try again later.", user_id)
        )
    finally:
        if conn:
            await conn.close()  # Ensure connection is closed


# This function does not interact with the database, so no changes needed
@router.callback_query(F.data == "set_gender")
async def set_gender_handler(query: types.CallbackQuery):
    user_id = query.from_user.id # Get user_id for localization
    await query.message.answer(
        _("🔄 Select your new gender:", user_id), # Localize this message
        reply_markup=gender_keyboard(user_id, context="change") # Pass user_id to gender_keyboard
    )
    await query.answer()

# This function does not interact with the database, so no changes needed
current_chats = {
}  # Dictionary to store active chat pairs (user_id: partner_id)


# This function does not interact with the database, so no changes needed
def gender_selection_keyboard(user_id: int):
    """
    Creates an inline keyboard for gender selection (e.g., for partner preference),
    localized for the given user_id.
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=_("♂️ Male", user_id), callback_data="gender_pref:male")
        ],
        [
            InlineKeyboardButton(text=_("♀️ Female", user_id), callback_data="gender_pref:female")
        ],
        [
            InlineKeyboardButton(text=_("Any", user_id), callback_data="gender_pref:any") # Localized 'any'
        ],
    ])
    return keyboard


search_queue = [
]  # List to store searching users (user_id, timestamp, gender_pref)
current_chats = {
}  # Dictionary to store active chat pairs (user_id: partner_id)

find_match_lock = asyncio.Lock()  # Already defined


async def find_match(user_id, gender_pref, is_vip):
    global current_chats, search_queue

    logger.debug(
        f"find_match called for user {user_id}. Pref: {gender_pref}, VIP: {is_vip}"
    )

    conn = None
    try:
        async with find_match_lock:
            # Check if user is still in the queue (might have been matched already)
            if not any(uid == user_id for uid, _, _ in search_queue):
                logger.debug(
                    f"User {user_id} no longer in search_queue at start of find_match."
                )
                return None, None

            user_ids_in_queue = [uid for uid, _, _ in search_queue]
            logger.debug(f"Users currently in queue: {user_ids_in_queue}")

            conn = await create_database_connection()
            rows = await conn.fetch(
                "SELECT user_id, is_vip, gender FROM users WHERE user_id = ANY($1)",
                user_ids_in_queue)
            user_info_map = {row['user_id']: row for row in rows}
            logger.debug(f"User info map from DB: {user_info_map}")

            user_own_gender = user_info_map.get(user_id, {}).get('gender')
            if not user_own_gender:
                logger.warning(
                    f"User {user_id} has no gender set, cannot match.")
                # Remove user from queue since can't match properly
                search_queue[:] = [(uid, ts, gen)
                                   for uid, ts, gen in search_queue
                                   if uid != user_id]
                return None, None

            current_user_effective_pref = gender_pref if is_vip else "any"
            logger.debug(
                f"User {user_id} (VIP:{is_vip}) effective preference: {current_user_effective_pref}"
            )

            potential_partners = []

            for other_user_id, _, other_user_gender_pref_in_queue in search_queue:
                if other_user_id == user_id:
                    continue

                other_user_row = user_info_map.get(other_user_id)
                if not other_user_row:
                    logger.debug(
                        f"Skipping {other_user_id}: Not found in DB info map.")
                    continue

                other_user_is_vip = other_user_row['is_vip']
                other_user_gender = other_user_row['gender']

                other_user_effective_pref = other_user_gender_pref_in_queue if other_user_is_vip else "any"

                current_user_likes_other = (
                    current_user_effective_pref == "any"
                    or other_user_gender == current_user_effective_pref)
                other_user_likes_current = (other_user_effective_pref == "any"
                                            or user_own_gender
                                            == other_user_effective_pref)

                logger.debug(
                    f"  Checking pair: {user_id} (gender:{user_own_gender}, vip:{is_vip}) vs {other_user_id} (gender:{other_user_gender}, vip:{other_user_is_vip})"
                )
                logger.debug(
                    f"    {user_id} likes {other_user_id}: {current_user_likes_other}"
                )
                logger.debug(
                    f"    {other_user_id} likes {user_id}: {other_user_likes_current}"
                )

                # Matchmaking logic
                if is_vip and other_user_is_vip:
                    logger.debug(f"    Case: Both VIPs.")
                    if current_user_likes_other and other_user_likes_current:
                        potential_partners.append(
                            (other_user_id, other_user_is_vip))
                        logger.debug(
                            f"      -> Added {other_user_id} to potential partners (mutual VIP like)."
                        )
                elif is_vip and not other_user_is_vip:
                    logger.debug(
                        f"    Case: {user_id} is VIP, {other_user_id} is Non-VIP."
                    )
                    if current_user_likes_other:
                        potential_partners.append(
                            (other_user_id, other_user_is_vip))
                        logger.debug(
                            f"      -> Added {other_user_id} to potential partners (VIP likes Non-VIP)."
                        )
                elif not is_vip and other_user_is_vip:
                    logger.debug(
                        f"    Case: {user_id} is Non-VIP, {other_user_id} is VIP."
                    )
                    if other_user_likes_current:
                        potential_partners.append(
                            (other_user_id, other_user_is_vip))
                        logger.debug(
                            f"      -> Added {other_user_id} to potential partners (VIP likes Non-VIP)."
                        )
                else:  # Both are non-VIPs
                    logger.debug(f"    Case: Both Non-VIPs.")
                    potential_partners.append(
                        (other_user_id, other_user_is_vip)
                    )  # Non-VIPs always match if criteria met before this
                    logger.debug(
                        f"      -> Added {other_user_id} to potential partners (both Non-VIP)."
                    )

            logger.debug(
                f"Potential partners for {user_id} after loop: {potential_partners}"
            )

            if potential_partners:
                partner_id, partner_is_vip = random.choice(potential_partners)

                # Confirm both users are still in queue before finalizing match
                current_queue_user_ids = [uid for uid, _, _ in search_queue]
                if user_id not in current_queue_user_ids or partner_id not in current_queue_user_ids:
                    logger.warning(
                        f"One of the matched users is no longer in the queue: {user_id}, {partner_id}"
                    )
                    return None, None

                # Remove both users from search queue
                search_queue[:] = [(uid, ts, gen)
                                   for uid, ts, gen in search_queue
                                   if uid != user_id and uid != partner_id]

                # Add matched users to current chats
                current_chats[user_id] = partner_id
                current_chats[partner_id] = user_id

                logger.info(
                    f"MATCHED: {user_id} <-> {partner_id} (VIP:{is_vip} vs PartnerVIP:{partner_is_vip})"
                )
                return partner_id, partner_is_vip

            logger.debug(f"No potential partners found for {user_id}.")
            return None, None

    except Exception as e:
        logger.error(f"ERROR in find_match() for user {user_id}: {e}",
                     exc_info=True)
        return None, None
    finally:
        if conn:
            await conn.close()


# 🔧 Ensure tuple on error


async def handle_vip_search(message: types.Message, bot: Bot):
    """Handles /search for VIP users."""
    user_id = message.from_user.id # Get user_id for localization
    await message.answer(
        _("Choose the gender you want to chat with:", user_id), # Localize this message
        reply_markup=gender_selection_keyboard(user_id) # Pass user_id to keyboard function
    )


@router.callback_query(F.data.startswith("gender_pref:"))
async def gender_preference_callback(query: types.CallbackQuery, bot: Bot):
    user_id = query.from_user.id
    gender_pref = query.data.split(":")[1]
    current_time = time.time() # Not directly used for localization, but kept for context

    await query.answer() # Acknowledge the callback query

    # --- SIMPLIFIED COOLDOWN CHECK ---
    if any(uid == user_id for uid, _, _ in search_queue):
        await query.message.answer(
            _("⏳ You are already in the search queue. Please wait for your current search to complete, or you can /stop.", user_id)
        )
        try:
            await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)
        except Exception as e:
            logger.error(f"Could not delete gender preference message for {user_id}: {e}")
        return
    # --- END SIMPLIFIED COOLDOWN CHECK ---

    # Delete the gender preference message buttons after selection
    try:
        await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)
    except Exception as e:
        logger.error(f"Could not delete gender preference message for {user_id} after selection: {e}")

    # Prevent searching if already in a chat
    if user_id in current_chats:
        partner_id = current_chats.pop(user_id, None)
        if partner_id and partner_id in current_chats:
            del current_chats[partner_id]
            try:
                await bot.send_message(
                    partner_id,
                    _("Your partner has disconnected. Use /search to find a partner.", partner_id) # Localize for partner
                )
            except Exception as e:
                logger.error(f"Could not send disconnect message to {partner_id}: {e}")
        await query.message.answer(_("You were in a chat. Disconnected.", user_id)) # Localize for current user
        return

    # --- DB Fetch (for user's VIP status and gender) ---
    conn = None
    try:
        conn = await create_database_connection()
        user_row = await conn.fetchrow(
            "SELECT is_vip, gender FROM users WHERE user_id = $1", user_id)

        if not user_row:
            await query.message.answer(
                _("⚠️ Could not retrieve your user info. Please try again.", user_id)
            )
            logger.warning(f"User {user_id} not found in DB during gender_preference_callback.")
            return

        is_vip = user_row['is_vip']
        user_own_gender = user_row['gender']

        if not user_own_gender:
            await query.message.answer(
                _("⚠️ Please set your gender first using /setgender.", user_id)
            )
            logger.info(f"User {user_id} tried to search without setting gender.")
            return

        # Ensure only VIPs can use gender preferences
        if not is_vip:
            await query.message.answer(
                _("💎 Gender-based matching is a VIP-only feature.\nBecome a /vip member", user_id)
            )
            logger.info(f"Non-VIP user {user_id} tried to use gender preference for search.")
            return

        # Add user to queue
        # Ensure not duplicated. Using list comprehension to remove existing entry.
        search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
        search_queue.append((user_id, time.time(), gender_pref))
        logger.info(f"User {user_id} added to search queue with preference {gender_pref}.")

        searching_message = await query.message.answer(
            _("🔍 Searching for a partner...", user_id) # Localize this
        )
        searching_message_id = searching_message.message_id

        partner_id = None
        partner_is_vip = False
        for _ in range(20):  # Try for 20 seconds
            partner_id, partner_is_vip = await find_match(user_id, gender_pref, is_vip)
            if partner_id:
                break
            await asyncio.sleep(1)

        # --- Always remove user from search queue after search attempt ---
        search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
        logger.info(f"User {user_id} removed from search queue after attempt.")

        try:
            await bot.delete_message(chat_id=query.message.chat.id, message_id=searching_message_id)
        except Exception as e:
            logger.error(f"Could not delete searching message {searching_message_id} for user {user_id}: {e}")

        if partner_id:
            # Match found
            current_chats[user_id] = partner_id
            current_chats[partner_id] = user_id

            # Get partner's language to localize their message
            partner_conn = None
            partner_language = 'en' # Default fallback
            try:
                partner_conn = await create_database_connection()
                partner_row = await partner_conn.fetchrow("SELECT language FROM users WHERE user_id = $1", partner_id)
                if partner_row and partner_row['language']:
                    partner_language = partner_row['language']
            except Exception as db_e:
                logger.error(f"Error fetching partner language for {partner_id}: {db_e}")
            finally:
                if partner_conn:
                    await partner_conn.close()

            # Send messages to both users, localized for each
            if partner_is_vip:
                await query.message.answer(
                    _("💎 You found another VIP partner! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat", user_id),
                    parse_mode=ParseMode.HTML
                )
            else:
                await query.message.answer(
                    _("✅ Partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat", user_id)
                )

            try:
                # Use partner_language for the message sent to the partner
                await bot.send_message(
                    partner_id,
                    _("💎 VIP partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat", partner_id, partner_language), # Pass partner_id AND partner_language
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Could not send match message to partner {partner_id}: {e}")
            logger.info(f"Match found between {user_id} and {partner_id}. User is VIP, partner is VIP: {partner_is_vip}.")
        else:
            # No match found
            await query.message.answer(
                _("😔 No partners found at the moment. Please try again later.", user_id) # Localize this message
            )
            logger.info(f"No match found for user {user_id} after timeout.")

    except Exception as e:
        logger.error(
            f"Error in gender_preference_callback for user {user_id}: {e}",
            exc_info=True
        )
        await query.message.answer(
            _("❌ An unexpected error occurred during search. Please try again later.", user_id)
        )
    finally:
        if conn:
            await conn.close()

async def get_partner_searching_message_id(partner_id: int) -> int | None:
    """Retrieves the searching message ID for a given partner ID from the database."""
    conn = None  # Initialize conn to None for safe cleanup
    try:
        conn = await create_database_connection()
        # Use await conn.fetchrow() for a single row
        # Replace %s with $1
        result = await conn.fetchrow(
            "SELECT message_id FROM search_messages WHERE user_id = $1",
            partner_id)
        if result:
            return result['message_id']  # Access the 'message_id' key
        else:
            return None
    except Exception as e:
        logger.error(
            f"ERROR: Error in get_partner_searching_message_id for {partner_id}: {e}",
            exc_info=True)
        return None
    finally:
        if conn:
            await conn.close()


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


@router.message(lambda message: message.text == "🚹 Search by Gender")
async def search_by_gender_handler(message: types.Message, bot: Bot):
    await handle_vip_search(message, bot)


def search_menu_reply_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🏙️ Search by City")],
                  [KeyboardButton(text="🚹 Search by Gender")]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


@router.callback_query(F.data == "set_location")
async def set_location_callback(query: types.CallbackQuery):
    user_id = query.from_user.id # Get user_id for localization

    # Localize the button text
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text=_("📍 Share Location", user_id), request_location=True)
        ]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    
    # Localize the message text
    await query.message.answer(
        _("Please share your live location:", user_id),
        reply_markup=keyboard
    )
    # Always answer the callback query to dismiss the loading state on the client
    await query.answer()

@router.message(Command("search"))
async def search_command(message: types.Message, bot: Bot):
    user_id = message.from_user.id
    conn = None # Initialize conn to None for safe cleanup

    try:
        conn = await create_database_connection()

        result = await conn.fetchrow(
            "SELECT is_vip FROM users WHERE user_id = $1", user_id)

        if result and result["is_vip"]:
            # This function should internally handle its own localization based on message.from_user.id
            await quick_vip_search(message)
        else:
            # This function should internally handle its own localization based on message.from_user.id
            await handle_non_vip_search(message, bot)
    except Exception as e:
        logger.error(f"Error in search_command for user {user_id}: {e}", exc_info=True)
        # Localize the error message
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.", user_id)
        )
    finally:
        if conn:
            await conn.close()

# In webhook/handlers.py (or wherever your global state is)
# ...
search_queue = []
current_chats = {}
user_search_cooldowns = {}  # New dictionary to track cooldowns
SEARCH_COOLDOWN_SECONDS = 30  # For example, 30 seconds


async def quick_vip_search(message: types.Message):
    user_id = message.from_user.id
    current_time = time.time() # Not directly used for localization

    # --- SIMPLIFIED COOLDOWN CHECK ---
    if any(uid == user_id for uid, _, _ in search_queue):
        await message.answer(
            _("⏳ You are already in the search queue. Please wait for your current search to complete, or you can /stop", user_id)
        )
        return
    # --- END SIMPLIFIED COOLDOWN CHECK ---

    # Prevent searching if already in a chat
    if user_id in current_chats:
        await message.answer(
            _("🤔 You are already in a dialog right now.\n/next — find a new partner\n/stop — stop this dialog", user_id)
        )
        return

    # Add user to queue
    search_queue[:] = [
        (uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id
    ]
    search_queue.append((user_id, time.time(), "any")) # VIP quick search is "any" gender
    logger.info(f"User {user_id} started quick VIP search and added to queue.")

    search_msg = await message.answer(_("🔍 Searching for a partner...", user_id))

    timeout = 20
    interval = 2
    elapsed = 0
    partner_id = None
    partner_is_vip = False

    while elapsed < timeout:
        partner_id, partner_is_vip = await find_match(user_id, "any", True) # 'True' indicates caller is VIP
        if partner_id:
            break
        await asyncio.sleep(interval)
        elapsed += interval

    # --- Always remove user from search queue after search attempt ---
    search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
    logger.info(f"User {user_id} removed from search queue after attempt.")

    try:
        await message.bot.delete_message(chat_id=message.chat.id, message_id=search_msg.message_id)
    except Exception as e:
        logger.error(f"Failed to delete search message for user {user_id}: {e}")

    if partner_id:
        # Match found
        current_chats[user_id] = partner_id
        current_chats[partner_id] = user_id

        # Fetch partner's language for localized message
        partner_language = 'en' # Default fallback
        # Assuming you have a way to get a DB connection here, e.g., bot.get('db_pool')
        conn = None
        try:
            conn = await create_database_connection() # Or acquire from pool
            partner_row = await conn.fetchrow("SELECT language FROM users WHERE user_id = $1", partner_id)
            if partner_row and partner_row['language']:
                partner_language = partner_row['language']
        except Exception as db_e:
            logger.error(f"Error fetching partner language for {partner_id}: {db_e}")
        finally:
            if conn:
                await conn.close()

        # Send messages to both users, localized for each
        if partner_is_vip:
            await message.answer(
                _("💎 You found another VIP partner! Start chatting!\n\n/next — new partner\n/stop — end chat", user_id),
                parse_mode=ParseMode.HTML
            )
        else:
            await message.answer(
                _("✅ Partner found! Start chatting!\n\n/next — new partner\n/stop — end chat", user_id)
            )

        try:
            await message.bot.send_message(
                partner_id,
                # Pass partner_id AND partner_language to your _() function
                _("💎 VIP partner found! Start chatting!\n\n/next — new partner\n/stop — end chat", partner_id, partner_language),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to send match message to partner {partner_id}: {e}")
        logger.info(f"Quick VIP search: Match found between {user_id} and {partner_id}")
    else:
        # No match found
        await message.answer(
            _("😔 No partners found at the moment. Please try again later.", user_id)
        )
        logger.info(f"Quick VIP search: No match found for user {user_id} after timeout.")

@router.message(Command("stop"))
async def stop_command(message: types.Message, bot: Bot):
    """Handles the /stop command."""
    global current_chats, search_queue # Declare global if modifying them
    user_id = message.from_user.id
    logger.info(f"Stop command from {user_id}. Current chats: {current_chats}")

    if user_id not in current_chats:
        await message.answer(_("You are not in an active chat. /search to find a partner.", user_id))
        logger.info(f"{user_id} is not in current_chats.")
        # Remove user from search queue if they were searching
        search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
        return

    partner_id = current_chats[user_id]
    logger.info(f"Partner ID: {partner_id}")

    # Check if the partner also has the user in their current_chats to ensure a valid pair
    if partner_id in current_chats and current_chats[partner_id] == user_id:
        # Remove both from chat map
        del current_chats[user_id]
        del current_chats[partner_id]
        logger.info(f"Chat stopped: {user_id} - {partner_id}. Current chats: {current_chats}")

        # Fetch partner's language for localized message
        partner_language = 'en' # Default fallback
        conn = None # Initialize conn for safe cleanup
        try:
            conn = await create_database_connection() # Or acquire from pool
            partner_row = await conn.fetchrow("SELECT language FROM users WHERE user_id = $1", partner_id)
            if partner_row and partner_row['language']:
                partner_language = partner_row['language']
        except Exception as db_e:
            logger.error(f"Error fetching partner language for {partner_id}: {db_e}")
        finally:
            if conn:
                await conn.close()

        # Notify partner (localized for partner's language)
        try:
            await bot.send_message(
                partner_id,
                _("✅ Your partner has stopped the chat. /search to find a new partner", partner_id, partner_language),
                reply_markup=search_menu_reply_keyboard(partner_id) # Pass partner_id to keyboard
            )
        except Exception as e:
            logger.error(f"Failed to notify partner {partner_id} about chat stop: {e}")

        # Notify user (localized for user's language)
        await message.answer(
            _("✅ Chat stopped. /search to find a new partner", user_id),
            reply_markup=search_menu_reply_keyboard(user_id) # Pass user_id to keyboard
        )

        # Send feedback buttons (localize feedback_keyboard if it contains text)
        try:
            # Assuming feedback_keyboard is a function that accepts user_id for localization
            await bot.send_message(
                partner_id,
                _("How was your experience with your last partner?", partner_id, partner_language),
                reply_markup=feedback_keyboard(partner_id) # Pass partner_id
            )
            await message.answer(
                _("How was your experience with your last partner?", user_id),
                reply_markup=feedback_keyboard(user_id) # Pass user_id
            )
        except Exception as e:
            logger.error(f"Failed to send feedback keyboard to {user_id} or {partner_id}: {e}")

        # Remove both from search queue if they were there (redundant if they were matched)
        search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid not in (user_id, partner_id)]

    else:
        # This branch indicates an inconsistent state in current_chats
        await message.answer(_("There was an issue stopping the chat.", user_id))
        logger.error(f"Inconsistent state when stopping chat for {user_id} - {partner_id}. Current chats: {current_chats}")



@router.message(Command("settings"))
async def settings_command(message: types.Message):
    user_id = message.from_user.id # Get user_id for localization

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=_("🔄 Change Gender", user_id), # Localized
                                 callback_data="set_gender")
        ],
        [
            InlineKeyboardButton(text=_("📍 Set Location", user_id), # Localized
                                 callback_data="set_location")
        ],
        [
            InlineKeyboardButton(text=_("🎂 Set Age", user_id), # Localized
                                 callback_data="set_age")
        ],
        [  # New row for Language Setting
            InlineKeyboardButton(text=_("🌐 Set Language", user_id), # Localized
                                 callback_data="set_language")
        ]
    ])
    await message.answer(
        _("⚙️ Choose what you want to update:", user_id), # Localized
        reply_markup=keyboard
    )

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


async def get_user_credits(user_id):
    """Retrieves user credits and search data from the database."""
    conn = None  # Initialize conn to None for safe cleanup
    try:
        conn = await create_database_connection()
        # Use await conn.fetchrow() for a single row
        # Replace %s with $1
        result = await conn.fetchrow(
            "SELECT credit, last_search_date, search_count FROM users WHERE user_id = $1",
            user_id)
        if result:
            # asyncpg.Record objects can be accessed like dictionaries
            return {
                "credits": result['credit'],
                "last_search_date": result['last_search_date'],
                "search_count": result['search_count']
            }
        # Return default values if user not found
        return {"credits": 0, "last_search_date": None, "search_count": 0}
    except Exception as e:
        logger.error(f"Error getting user credits for {user_id}: {e}",
                     exc_info=True)
        # Return default or error indicator on failure
        return {"credits": 0, "last_search_date": None, "search_count": 0}
    finally:
        if conn:
            await conn.close()


async def update_user_credits(user_id, credits, last_search_date,
                              search_count):
    """Updates user credits and search data in the database."""
    conn = None  # Initialize conn to None for safe cleanup
    try:
        conn = await create_database_connection()
        # Use await conn.execute() for UPDATE statements
        # Replace %s with $1, $2, $3, $4 for asyncpg parameters
        await conn.execute(
            "UPDATE users SET credit = $1, last_search_date = $2, search_count = $3 WHERE user_id = $4",
            credits, last_search_date, search_count, user_id)
        logger.info(
            f"User {user_id} credits updated to {credits}, last_search_date to {last_search_date},                  search_count to {search_count}."
        )
    except Exception as e:
        logger.error(f"Error updating user credits for {user_id}: {e}",
                     exc_info=True)
        # Handle the error appropriately, maybe re-raise or return a status
    finally:
        if conn:
            await conn.close()


# Assuming router is initialized, e.g., router = Router()


@router.message(Command("credit"))
async def credit_command(message: types.Message):
    """Handles the /credit command."""
    user_id = message.from_user.id

    try:
        user_data = await get_user_credits(user_id)  # Get current user data
        new_credits = user_data['credits'] + 10  # Add 10 credits

        await update_user_credits(user_id, new_credits,
                                  user_data['last_search_date'],
                                  user_data['search_count'])

        # Send an image showing the credit reward visually
        photo = FSInputFile("media/download.png") # Use your actual image file
        # No localization needed for the photo itself, but consider a localized caption if you add one.
        await message.answer_photo(photo=photo) # parse_mode is not needed for just a photo

        # Then send the actual credit update message, localized
        await message.answer(
            _("💰 You earned 10 credits!\nYour total credits: {new_credits}", user_id).format(new_credits=new_credits)
        )

        logger.info(f"User {user_id} added 10 credits. Total: {new_credits}")

    except Exception as e:
        logger.error(
            f"Error processing /credit command for user {user_id}: {e}",
            exc_info=True
        )
        # Localize the error message
        await message.answer(
            _("❌ An error occurred while adding credits. Please try again later.", user_id)
        )


# Initialize global variables at module level (as provided by you)
search_queue = []
non_vip_search_locks = defaultdict(bool)


# Assume get_user_credits, update_user_credits, and find_match are defined and corrected elsewhere
# Example placeholders if they are in other files:
# from .db_operations import get_user_credits, update_user_credits
# from .matchmaking import find_match
# Initialize logger
async def handle_non_vip_search(message: types.Message, bot: Bot):
    global search_queue, non_vip_search_locks, current_chats
    user_id = message.from_user.id
    today = date.today()

    if non_vip_search_locks[user_id]:
        await message.answer(
            _("Please wait for your previous search request to finish, or /stop to cancel.", user_id)
        )
        logger.info(f"User {user_id} tried to search while another search was active.")
        return

    non_vip_search_locks[user_id] = True # Set lock at the beginning of the try block

    conn = None # Initialize conn for safe cleanup of partner language fetch
    try:
        user_data = await get_user_credits(user_id)
        logger.debug(f"User {user_id} data fetched: {user_data}")

        # Reset search count if it's a new day
        if user_data.get('last_search_date') != today:
            user_data['search_count'] = 0
            await update_user_credits(user_id, user_data.get('credits', 0), today, 0)
            user_data['last_search_date'] = today
            logger.info(f"User {user_id} daily search count reset.")

        current_search_count = user_data.get('search_count', 0)
        current_credits = user_data.get('credits', 0)
        # Determine if credits are needed (after 10 free searches)
        needs_credit = current_search_count >= 10

        if needs_credit and current_credits <= 0:
            await message.answer(
                _("You have reached your daily search limit or have no credits. Use /credit to get more searches.", user_id)
            )
            logger.info(f"User {user_id} blocked from searching due to limit/credits.")
            return

        # Disconnect from current chat if active
        if user_id in current_chats:
            partner_id = current_chats.pop(user_id, None)
            if partner_id:
                current_chats.pop(partner_id, None)

                # Fetch partner's language for localized disconnect message
                partner_language = 'en' # Default fallback
                try:
                    conn = await create_database_connection() # Or acquire from pool
                    partner_row = await conn.fetchrow("SELECT language FROM users WHERE user_id = $1", partner_id)
                    if partner_row and partner_row['language']:
                        partner_language = partner_row['language']
                except Exception as db_e:
                    logger.error(f"Error fetching partner language for {partner_id}: {db_e}")
                finally:
                    if conn: # Close connection if it was opened in this block
                        await conn.close()
                        conn = None # Reset conn to None after closing

                try:
                    await bot.send_message(
                        partner_id,
                        _("Your partner has disconnected to /search for someone new.", partner_id, partner_language)
                    )
                    logger.info(f"User {user_id} disconnected from {partner_id}.")
                except Exception as e:
                    logger.error(f"Failed to send disconnect message to {partner_id}: {e}")
            await message.answer(
                _("You have been disconnected from your previous chat. Searching for a new partner.", user_id)
            )

        # Update search count and credits before adding to queue
        new_search_count = current_search_count + 1
        new_credits = current_credits - 1 if needs_credit else current_credits
        await update_user_credits(user_id, new_credits, today, new_search_count)
        logger.info(
            f"User {user_id} search count incremented to {new_search_count}, credits to {new_credits}."
        )

        # Add user to search queue
        # Atomically add to queue and find match to avoid race condition
        async with find_match_lock:
            # Ensure not duplicated if somehow already there before adding
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
            search_queue.append((user_id, time.time(), "any")) # Non-VIP search is "any" gender
            logger.info(f"User {user_id} added to search queue inside locked block.")

        searching_message = await message.answer(_("🔍Searching for a partner...", user_id))

        # Try to find a match immediately (outside lock to avoid holding it too long)
        partner_id = None
        is_partner_vip = False
        match_found = False

        # Attempt to find match, loop for a period
        timeout_seconds = 20
        sleep_interval = 2
        current_attempts = 0
        while current_attempts * sleep_interval < timeout_seconds:
            found_partner_id, found_is_partner_vip = await find_match(user_id, "any", False)
            if found_partner_id:
                partner_id = found_partner_id
                is_partner_vip = found_is_partner_vip
                match_found = True
                break
            await asyncio.sleep(sleep_interval)
            current_attempts += 1

        # Remove the 'searching' message
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)
        except Exception as e:
            logger.error(f"Failed to delete search message for user {user_id}: {e}")

        if match_found and partner_id: # Check match_found flag and partner_id existence
            # Match found
            # Ensure the user is removed from queue if they were still there
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]

            # The current_chats should be set by find_match, but ensure consistency
            current_chats[user_id] = partner_id
            current_chats[partner_id] = user_id

            # Re-fetch partner's language if the connection was closed
            partner_language = 'en'
            conn = None
            try:
                conn = await create_database_connection()
                partner_row = await conn.fetchrow("SELECT language FROM users WHERE user_id = $1", partner_id)
                if partner_row and partner_row['language']:
                    partner_language = partner_row['language']
            except Exception as db_e:
                logger.error(f"Error re-fetching partner language for {partner_id} after match: {db_e}")
            finally:
                if conn:
                    await conn.close()

            if is_partner_vip:
                await message.answer(
                    _("💎 VIP partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat", user_id)
                )
            else:
                await message.answer(
                    _("✅ Partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat", user_id)
                )

            try:
                await bot.send_message(
                    partner_id,
                    _("✅ Partner found! Start chatting!\n\n/next — find a new partner\n/stop — stop this chat", partner_id, partner_language)
                )
                logger.info(f"Match found between {user_id} and {partner_id}.")
            except Exception as e:
                logger.error(f"Failed to send match message to partner {partner_id}: {e}")
        else: # No match found after initial check and timeout
            # Ensure user is removed from queue if they're still there and no match was made
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
            await message.answer(
                _("😔 No users are currently available for chat. Removed from search queue.", user_id)
            )
            logger.info(f"User {user_id} removed from queue (no match found after timeout).")


    except Exception as e:
        logger.error(
            f"Unhandled error in handle_non_vip_search for user {user_id}: {e}",
            exc_info=True
        )
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.", user_id)
        )
    finally:
        # Ensure lock is released even if an error occurs
        if non_vip_search_locks[user_id]: # Only release if it was set
            non_vip_search_locks[user_id] = False
            logger.debug(f"Lock released for user {user_id}.")
        # Ensure final connection is closed if it was opened in the error block
        if conn:
            await conn.close()
@router.callback_query(F.data.startswith("gender:"))
async def gender_callback(query: types.CallbackQuery, bot: Bot):
    """Handles gender selection callback."""
    # Assuming the format is "gender:context:gender_value"
    parts = query.data.split(":")
    context = parts[1] if len(parts) > 2 else "start"  # Default context to "start"
    gender = parts[-1]  # Always take the last part as gender

    user_id = query.from_user.id
    conn = None  # Initialize conn to None for safe cleanup

    # Always answer the callback query to dismiss the loading state
    await query.answer()

    try:
        conn = await create_database_connection()
        await conn.execute("UPDATE users SET gender = $1 WHERE user_id = $2", gender, user_id)
        logger.info(f"User {user_id} gender updated to {gender}.")

        if context == "change":
            await query.message.answer(_("✅ Gender updated!", user_id)) # Localized

        # This delete ensures the inline keyboard is removed after selection
        await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)

        if context == "start":
            await query.message.answer(_("🔢 Please enter your age:", user_id)) # Localized

        # Assuming set_commands needs to be called after gender change
        # await set_commands(bot) # Uncomment if set_commands is needed here

    except Exception as e:
        logger.error(f"Database error updating gender for user {user_id}: {e}", exc_info=True)
        await query.message.answer(
            _("❌ An unexpected error occurred while saving your gender. Please try again later.", user_id) # Localized
        )
    finally:
        if conn:
            await conn.close()  # Ensure connection is closed

@router.message(Command("next"))
async def next_command(message: types.Message, bot: Bot):
    """Handle /next by disconnecting and routing based on VIP status."""
    global search_queue, current_chats, non_vip_search_locks

    user_id = message.from_user.id
    conn = None  # Initialize conn to None for safe cleanup
    logger.info(f"Next command from {user_id}.")

    try:
        conn = await create_database_connection()

        # 1. Check ban status
        banned_info = await conn.fetchrow(
            """
            SELECT banned_until FROM banned_users WHERE user_id = $1 AND banned_until > CURRENT_TIMESTAMP
            """,
            user_id
        )

        if banned_info:
            banned_until: datetime = banned_info['banned_until'] # Type hint for clarity
            # Localize the ban message, formatting the date within the localized string
            await message.answer(
                _("🚫 You are banned until {banned_time}.", user_id).format(
                    banned_time=banned_until.strftime('%Y-%m-%d %H:%M:%S')
                )
            )
            logger.info(f"User {user_id} is banned until {banned_until}.")
            return

        # 2. Disconnect from current chat (both users)
        if user_id in current_chats:
            partner_id = current_chats.pop(user_id)
            # Ensure partner exists in current_chats before attempting to pop
            if partner_id in current_chats:
                current_chats.pop(partner_id)
            logger.info(f"User {user_id} disconnected from {partner_id}.")

            # Fetch partner's language for localized message
            partner_language = 'en' # Default fallback
            # Re-using the existing `conn` if available from the ban check, otherwise get a new one
            db_conn_for_partner_lang = conn if conn else await create_database_connection()
            try:
                partner_row = await db_conn_for_partner_lang.fetchrow("SELECT language FROM users WHERE user_id = $1", partner_id)
                if partner_row and partner_row['language']:
                    partner_language = partner_row['language']
            except Exception as db_e:
                logger.error(f"Error fetching partner language for {partner_id}: {db_e}")
            finally:
                # Close the connection only if it was opened specifically for partner language here
                if db_conn_for_partner_lang != conn and db_conn_for_partner_lang:
                    await db_conn_for_partner_lang.close()


            try:
                await bot.send_message(
                    partner_id,
                    _("Your partner ended the chat. /search to find a new partner", partner_id, partner_language)
                )
                await bot.send_message(
                    partner_id,
                    _("How was your experience with your last partner?", partner_id, partner_language),
                    reply_markup=feedback_keyboard(partner_id) # Pass partner_id to keyboard
                )
            except Exception as e:
                logger.error(f"Failed to notify partner {partner_id} about /next: {e}")

            await message.answer(
                _("How was your experience with your last partner?", user_id),
                reply_markup=feedback_keyboard(user_id) # Pass user_id to keyboard
            )
        else:
            await message.answer(_("You're not currently in a chat.", user_id))
            logger.info(f"User {user_id} used /next but was not in a chat.")

        # 3. Check VIP status
        # Re-use `conn` from the beginning of the function
        # No need to open a new connection if `conn` is already active
        if conn is None: # This check is primarily for cases where the initial `create_database_connection()` failed.
            conn = await create_database_connection()

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
        logger.error(f"Error in /next command for user {user_id}: {e}", exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.", user_id)
        )
    finally:
        # Ensure the main connection is closed
        if conn:
            await conn.close()  # Ensure connection is closed


#@router.message(Command("vip"))
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
    # Localize the entire privacy policy content
    localized_policy = _(PRIVACY_POLICY_CONTENT_EN, user_id)
    await message.answer(localized_policy, parse_mode="Markdown") # Use Markdown for formatting



@router.message(Command("vip"))
async def vip_command(message: Message):
    user_id = message.from_user.id
    conn = None  # Initialize conn to None for safe cleanup

    try:
        conn = await create_database_connection()
        result = await conn.fetchrow(
            "SELECT is_vip FROM users WHERE user_id = $1", user_id
        )

        if result and result["is_vip"]:
            await message.answer(
                _("🎉 You already have 💎 **VIP access**!\nEnjoy all premium features.", user_id),
                parse_mode="Markdown" # Use Markdown for bold and diamond emoji
            )
            logger.info(f"User {user_id} tried to become VIP but already has access.")
            return

        gif = FSInputFile(r"media/Unlock VIP Access.gif") # Use raw string for Windows path

        # No localization needed for the animation itself, but if it had a caption, localize that.
        await message.answer_animation(animation=gif) # parse_mode can be removed if no caption

        # Show payment options
        # Localize the text content
        text = _("<b>💎 Become a VIP User</b>\n"
                 "Support the chat and unlock premium features instantly.\n\n"
                 "<b>Choose your preferred payment method:</b>", user_id)

        builder = InlineKeyboardBuilder()
        builder.button(
            text=_("🧾 Telegram Payments", user_id), # Localized button text
            callback_data="pay_telegram"
        )
        builder.button(
            text=_("💳 Chapa Payments", user_id), # Localized button text
            callback_data="pay_chapa"
        )

        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML") # Use HTML for bold tags
        logger.info(f"User {user_id} was shown VIP payment options.")

    except Exception as e:
        logger.error(f"Error in vip_command for user {user_id}: {e}", exc_info=True)
        await message.answer(
            _("❌ An unexpected error occurred. Please try again later.", user_id) # Localized error message
        )
    finally:
        if conn:
            await conn.close() 

@router.message(Command("userid"))
async def userid_command(message: types.Message):
    """Handles the /userid command."""
    user_id = message.from_user.id
    await message.answer(
        # Localize the message, using .format() for the variable part
        _("Your User ID is: `{user_id}`", user_id).format(user_id=user_id)
    )
    logger.info(f"User {user_id} requested their user ID.")

async def get_user_by_id(user_id):
    conn = None  # Initialize conn to None for safe cleanup
    try:
        conn = await create_database_connection()
        # Use await conn.fetchrow() for a single row
        # Replace %s with $1
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1",
                                  user_id)

        # asyncpg.Record objects can be accessed like dictionaries.
        # No need for psycopg2.extras.DictCursor or dict(row) conversion.
        return row if row else None

    except Exception as e:
        logger.error(f"❌ Error in get_user_by_id for user {user_id}: {e}",
                     exc_info=True)
        return None
    finally:
        if conn:
            await conn.close()


class SettingsStates(StatesGroup):
    waiting_for_age = State()  # This line needs to be indented


@router.callback_query(F.data == "set_age")
async def ask_age(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id # Get user_id for localization
    await query.answer()  # Always answer the callback query
    await query.message.answer(
        _("🔢 Please enter your age:", user_id) # Localized message
    )
    await state.set_state(SettingsStates.waiting_for_age)
    logger.info(f"User {user_id} initiated age setting.")
@router.message(SettingsStates.waiting_for_age)
async def age_input_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip()
    conn = None # Initialize conn outside try block for wider scope

    if text.isdigit():
        age = int(text)
        if 10 <= age <= 100:
            try:
                conn = await create_database_connection()
                await conn.execute(
                    "UPDATE users SET age = $1 WHERE user_id = $2", age, user_id
                )
                await message.answer(
                    _("✅ Your age has been set to: **{age}**", user_id).format(age=age),
                    parse_mode="Markdown" # Use Markdown for bold text
                )
                logger.info(f"User {user_id} successfully set age to {age}.")
            except Exception as e:
                logger.error(
                    f"Error updating age for user {user_id}: {e}", exc_info=True
                )
                await message.answer(
                    _("❌ An error occurred while saving your age. Please try again later.", user_id)
                )
            finally:
                if conn:
                    await conn.close()
            await state.clear()
        else:
            await message.answer(
                _("❌ Please enter a valid age between 10 and 100.", user_id)
            )
            logger.warning(f"User {user_id} entered invalid age: {age}.")
    else:
        await message.answer(
            _("❌ Please enter a valid numeric age.", user_id)
        )
        logger.warning(f"User {user_id} entered non-numeric age: '{text}'.")


# Get the logger from the main application

# Helper function to convert date to timezone-aware datetime for comparison


# Globals
@router.message(lambda message: message.text == "🏙️ Search by City")
async def search_by_city_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    conn = None

    try:
        conn = await create_database_connection()
        if not conn:
            logger.error("Failed to connect to DB in search_by_city_handler.")
            await message.answer(
                _("An internal error occurred. Please try again later.", user_id)
            )
            return

        user_row = await conn.fetchrow(
            "SELECT is_vip, vip_expires_at, location, language FROM users WHERE user_id = $1",
            user_id
        )

        # Get user's language for localization
        user_language = user_row['language'] if user_row and user_row['language'] else 'en'

        if not user_row or not user_row['is_vip'] or \
           (user_row['vip_expires_at'] and user_row['vip_expires_at'] < datetime.now(timezone.utc)):
            await message.answer(
                _("💎 City-based matching is a **VIP-only feature**.\nBecome a /vip member to unlock it!", user_id),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} tried city search without active VIP.")
            return

        user_location = user_row['location']
        if not user_location:
            await message.answer(
                _("📍 Please share your location first using the /setlocation command.", user_id)
            )
            logger.info(f"User {user_id} tried city search but has no location set.")
            return

        if user_id in current_chats:
            await message.answer(
                _("⚠️ You're already in a chat. Use /stop to end it first before searching.", user_id)
            )
            logger.info(f"User {user_id} tried city search while in an active chat.")
            return

        # Check if user is already in any search queue (general or city)
        if any(user_id == uid for uid, _, _ in search_queue):
            await message.answer(
                _("⏳ You're already searching. Please wait or use /stop to cancel.", user_id)
            )
            logger.info(f"User {user_id} tried city search but is already in the queue.")
            return

        city = user_location.strip()

        # Remove any previous presence in queue (to ensure they are only in one place)
        search_queue[:] = [(uid, ts, loc) for uid, ts, loc in search_queue if uid != user_id]
        search_queue.append((user_id, time.time(), city)) # Add to queue with city
        logger.info(f"User {user_id} added to city search queue for city: {city}.")

        searching_msg = await message.answer(
            _("🔍 Searching for a partner in your city...", user_id)
        )

        match_found = False
        partner_id = None
        partner_is_vip = False
        partner_language = 'en' # Default for partner language

        shuffled_queue = list(search_queue)
        random.shuffle(shuffled_queue)

        # 1. First try matching with a VIP user in the same city
        for p_id, _, p_city in shuffled_queue:
            if p_id != user_id and p_city == city and p_id not in current_chats:
                partner_row = await conn.fetchrow(
                    "SELECT is_vip, vip_expires_at, language FROM users WHERE user_id = $1", p_id
                )
                if partner_row and partner_row['is_vip'] and \
                   (partner_row['vip_expires_at'] and partner_row['vip_expires_at'] > datetime.now(timezone.utc)):
                    partner_id = p_id
                    partner_is_vip = True
                    partner_language = partner_row['language'] if partner_row['language'] else 'en'
                    match_found = True
                    break

        # 2. If no VIP found, try matching with a non-VIP user in the same city
        if not match_found:
            for p_id, _, p_city in shuffled_queue:
                if p_id != user_id and p_city == city and p_id not in current_chats:
                    partner_row = await conn.fetchrow(
                        "SELECT is_vip, language FROM users WHERE user_id = $1", p_id
                    )
                    if partner_row and not partner_row['is_vip']:
                        partner_id = p_id
                        partner_is_vip = False
                        partner_language = partner_row['language'] if partner_row['language'] else 'en'
                        match_found = True
                        break

        # Remove the 'searching' message if it was sent
        try:
            await bot.delete_message(chat_id=user_id, message_id=searching_msg.message_id)
        except Exception as e:
            logger.error(f"Failed to delete search message for user {user_id}: {e}")

        if match_found and partner_id:
            current_chats[user_id] = partner_id
            current_chats[partner_id] = user_id
            logger.info(f"City match: {user_id} matched with {partner_id} in {city}. Partner VIP: {partner_is_vip}")

            # Localize match message for the user
            user_message_text = (
                _("💎 **VIP City Match Found!** You're now chatting with another **VIP** member in your city.\n\n/next — find a new partner\n/stop — end chat", user_id)
                if partner_is_vip else
                _("🏙️ **City Match Found!** You're now chatting with someone in your city.\n\n/next — find a new partner\n/stop — end chat", user_id)
            )

            # Localize match message for the partner
            partner_message_text = (
                _("💎 **VIP City Match Found!** You're now chatting with another **VIP** member in your city.\n\n/next — find a new partner\n/stop — end chat", partner_id, partner_language)
                if partner_is_vip else
                _("🏙️ **City Match Found!** You're now chatting with someone in your city.\n\n/next — find a new partner\n/stop — end chat", partner_id, partner_language)
            )

            await message.answer(user_message_text, parse_mode="Markdown") # Changed to Markdown from HTML
            await bot.send_message(partner_id, partner_message_text, parse_mode="Markdown") # Changed to Markdown from HTML

            # Remove both users from the search queue
            search_queue[:] = [(uid, ts, loc) for uid, ts, loc in search_queue if uid not in (user_id, partner_id)]
            return

        else: # No match found
            await message.answer(
                _("😔 No active users are available in your city right now. You'll stay in the search queue and be matched as soon as someone nearby becomes available.", user_id)
            )
            logger.info(f"No match found for user {user_id} in {city}. Remaining in queue.")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred in search_by_city_handler for user {user_id}: {e}",
            exc_info=True
        )
        await message.answer(
            _("❌ An unexpected error occurred while searching for a city partner. Please try again later.", user_id)
        )
    finally:
        if conn:
            await conn.close()

# Common handler logic
async def handle_fallback(message: Message):
    user_id = message.from_user.id

    if user_id not in current_chats:
        await message.answer(
            _("🤖 You're not in a chat right now.\n\nTap /Search to start chatting.", user_id)
        )
        logger.info(f"User {user_id} sent a message but was not in a chat; fallback message sent.")

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
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO subscription_requests (user_id, payment_proof, request_date, status) VALUES (%s, %s, now(), %s)",
        (user_id, message.photo[-1].file_id, "pending"))
    conn.commit()
    cursor.close()
    conn.close()
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

    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET is_vip = TRUE, subscription_expiry = now() + interval '30 days' WHERE user_id = %s",
        (user_id, ))
    cursor.execute(
        "UPDATE subscription_requests SET status = 'approved' WHERE user_id = %s",
        (user_id, ))
    conn.commit()
    cursor.close()
    conn.close()
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

    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE subscription_requests SET status = 'rejected' WHERE user_id = %s",
        (user_id, ))
    conn.commit()
    cursor.close()
    conn.close()
    await message.answer(f"User {user_id} VIP rejected.")
    await bot.send_message(user_id, "Your VIP request has been rejected.")


@router.message(F.voice)
async def vip_voice_handler(message: types.Message, bot: Bot):
    """Handles VIP voice messages."""
    user_id = message.from_user.id
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT is_vip FROM users WHERE user_id = %s", (user_id, ))
    is_vip = cursor.fetchone()['is_vip']
    cursor.close()
    conn.close()

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
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT is_vip FROM users WHERE user_id = %s", (user_id, ))
    is_vip = cursor.fetchone()['is_vip']
    cursor.close()
    conn.close()

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
    """Creates necessary database tables if they don't exist."""
    conn = None  # Initialize conn to None
    try:
        conn = await create_database_connection(
        )  # This now returns an asyncpg connection

        # Execute the SQL directly on the connection
        # You don't need 'cursor = conn.cursor()' or 'cursor.execute()' for asyncpg.
        # Use await conn.execute() for DDL (CREATE TABLE) statements.
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
                notified_before_expiry BOOLEAN DEFAULT FALSE
                language TEXT DEFAULT 'en'             
            );
            CREATE TABLE IF NOT EXISTS subscription_requests (
                request_id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                payment_proof TEXT,
                request_date TIMESTAMP,
                status TEXT
            );
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
           CREATE TABLE IF NOT EXISTS banned_users (
               user_id BIGINT PRIMARY KEY,
               banned_until TIMESTAMP WITH TIME ZONE NOT NULL,
               reason TEXT
           );
        """)
        # conn.commit() is generally not needed for DDL (CREATE TABLE) in asyncpg
        # when executed directly, as it auto-commits.
        # But if you run multiple DDL statements in a transaction, you'd use conn.transaction()

        logger.info("Database tables created or already exist.")

    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        raise  # Re-raise the exception to prevent bot from starting with broken DB
    finally:
        if conn:
            await conn.close()  # Close the connection when done


def feedback_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """
    Generates a localized inline keyboard for feedback.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=_("👍 Good", user_id),
                callback_data="feedback_good"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("👎 Bad", user_id),
                callback_data="feedback_bad"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("⚠️ Report", user_id),
                callback_data="feedback_report"
            )
        ]
    ])
@router.callback_query(F.data == "feedback_good")
async def feedback_good(callback: CallbackQuery):
    user_id = callback.from_user.id # Get user_id for localization

    await callback.answer(
        _("Your feedback has been submitted successfully.", user_id), # Localized message
        show_alert=True
    )

    # Optional: Log or save feedback to DB here
    logger.info(f"User {user_id} submitted 'good' feedback.")

    try:
        await callback.message.delete()  # Delete the whole message (text + buttons)
    except Exception as e:
        logger.error(f"Failed to delete feedback message for user {user_id}: {e}")

# Remove inline buttons


@router.callback_query(F.data == "feedback_bad")
async def feedback_bad(callback: CallbackQuery):
    user_id = callback.from_user.id # Get user_id for localization

    await callback.answer(
        _("Your feedback has been submitted successfully.", user_id), # Localized message
        show_alert=True
    )

    # Optional: Save to DB or log it
    logger.info(f"User {user_id} submitted 'bad' feedback.")

    try:
        await callback.message.delete()  # Delete the whole message (text + buttons)
    except Exception as e:
        logger.error(f"Failed to delete feedback message for user {user_id}: {e}")
def get_report_reasons_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """
    Generates a localized inline keyboard with reasons to report a partner.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=_("📢 Advertising", user_id),
                callback_data="report_advertising"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("💰 Selling", user_id),
                callback_data="report_selling"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("🔞 Child Pornography", user_id), # Clarified explicit text for better translation context
                callback_data="report_childporn"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("🤲 Begging", user_id),
                callback_data="report_begging"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("😡 Insult", user_id),
                callback_data="report_insult"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("🪓 Violence", user_id),
                callback_data="report_violence"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("🌍 Racism", user_id),
                callback_data="report_racism"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("🤬 Vulgar Partner", user_id),
                callback_data="report_vulgar"
            )
        ],
        [
            InlineKeyboardButton(
                text=_("🔙 Back", user_id),
                callback_data="feedback_keyboard" # This should ideally point to a function that regenerates the main feedback keyboard
            )
        ]
    ])

@router.callback_query(F.data == "feedback_report")
async def feedback_report(callback: CallbackQuery):
    user_id = callback.from_user.id # Get user_id for localization
    try:
        await callback.message.edit_text(
            text=_("⚠️ Please select a reason to report your partner:", user_id), # Localized message
            reply_markup=get_report_reasons_keyboard(user_id) # Call the function to get localized keyboard
        )
        logger.info(f"User {user_id} requested report reasons.")
    except Exception as e:
        logger.error(f"Failed to update message with report reasons for user {user_id}: {e}")


@router.callback_query(F.data == "feedback_keyboard")
async def handle_feedback_main(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=feedback_keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("report_"))
async def handle_report_reason(callback: CallbackQuery):
    user_id = callback.from_user.id
    reason = callback.data.replace("report_", "")
    reported_id = 0  # Fake ID since we don't track partners yet yet, will be replaced with actual partner ID.

    # Log or store report (optional)
    logger.info(f"User {user_id} reported UNKNOWN user for: {reason}") # Changed from print to logger.info

    # Optional: Save to DB if needed
    # await db.execute(
    #     "INSERT INTO reports (reporter_id, reported_id, reason) VALUES ($1, $2, $3)",
    #     user_id, reported_id, reason
    # )

    try:
        await callback.message.edit_text(
            _("✅ Your report has been submitted. Thank you!", user_id) # Localized message
        )
        await callback.answer() # Always answer the callback query to dismiss loading state
    except Exception as e:
        logger.error(f"Failed to send report confirmation to user {user_id}: {e}")




def get_telegram_plans_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """
    Generates a localized inline keyboard for Telegram Stars payment plans.
    """
    builder = InlineKeyboardBuilder()
    builder.button(
        text=_("100 ⭐ / $1.99 a week", user_id), # Localized button text
        callback_data="tgpay_week"
    )
    builder.button(
        text=_("250 ⭐ / $3.99 a month", user_id), # Localized button text
        callback_data="tgpay_1m"
    )
    builder.button(
        text=_("1000 ⭐ / $19.99 a year", user_id), # Localized button text
        callback_data="tgpay_1y"
    )
    # Arrange buttons in a column
    builder.adjust(1)
    return builder.as_markup()


@router.callback_query(F.data == "pay_telegram")
async def choose_telegram_plan(callback: CallbackQuery):
    user_id = callback.from_user.id # Get user_id for localization
    await callback.answer() # Acknowledge the callback query

    try:
        await callback.message.edit_text(
            text=_("💫 Choose your plan with Telegram Stars:", user_id), # Localized message
            reply_markup=get_telegram_plans_keyboard(user_id) # Get the localized keyboard
        )
        logger.info(f"User {user_id} was shown Telegram Stars payment plans.")
    except Exception as e:
        logger.error(f"Failed to show Telegram Stars plans to user {user_id}: {e}")
        await callback.message.answer(
            _("❌ An unexpected error occurred. Please try again later.", user_id)
        )


# Inside your chapa_payment_callback function:
def get_chapa_plans_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """
    Generates a localized inline keyboard for Chapa payment plans.
    """
    builder = InlineKeyboardBuilder()
    builder.button(
        text=_("1 Month - 400 ETB", user_id), # Localized button text
        callback_data="chapa_1m"
    )
    builder.button(
        text=_("6 Months - 1500 ETB", user_id), # Localized button text
        callback_data="chapa_6m"
    )
    builder.button(
        text=_("1 Year - 2500 ETB", user_id), # Localized button text
        callback_data="chapa_1y"
    )
    # Arrange buttons in a column
    builder.adjust(1)
    return builder.as_markup()


@router.callback_query(F.data == "pay_chapa")
async def choose_chapa_plan(callback: CallbackQuery):
    user_id = callback.from_user.id # Get user_id for localization
    await callback.answer() # Acknowledge the callback query

    try:
        await callback.message.edit_text(
            text=_("Choose your Chapa plan:", user_id), # Localized message
            reply_markup=get_chapa_plans_keyboard(user_id) # Get the localized keyboard
        )
        logger.info(f"User {user_id} was shown Chapa payment plans.")
    except Exception as e:
        logger.error(f"Failed to show Chapa plans to user {user_id}: {e}")
        await callback.message.answer(
            _("❌ An unexpected error occurred. Please try again later.", user_id)
        )


@router.callback_query(F.data.startswith("chapa_"))
async def handle_chapa_plan(callback: CallbackQuery):
    user_id = callback.from_user.id
    selected_callback_data = callback.data
    tx_ref = str(uuid.uuid4())

    prices = {
        "chapa_1m": {
            "amount": 400.00,
            "name": "1 Month VIP" # This name will be localized when creating the plan selection keyboard
        },
        "chapa_6m": {
            "amount": 1500.00,
            "name": "6 Months VIP"
        },
        "chapa_1y": {
            "amount": 2500.00,
            "name": "1 Year VIP"
        }
    }

    plan_info = prices.get(selected_callback_data)

    if not plan_info:
        await callback.answer(_("Invalid plan selected.", user_id), show_alert=True) # Localized
        logger.warning(f"User {user_id} selected an invalid Chapa plan: {selected_callback_data}")
        return

    vip_amount = plan_info["amount"]
    # The 'name' here is for internal use or DB, the display text is localized via get_chapa_plans_keyboard
    vip_plan_name = plan_info["name"]

    await callback.answer(_("Preparing Chapa payment...", user_id)) # Localized

    # Prepare Chapa payment request
    async with aiohttp.ClientSession() as session:
        headers = {
            "Authorization": f"Bearer {CHAPA_SECRET_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "amount": str(vip_amount),
            "currency": "ETB",
            "email": "salahadinshemsu0@gmail.com",  # Your test email
            "first_name": f"user_{user_id}",
            "tx_ref": tx_ref,
            "callback_url": CHAPA_CALLBACK_URL,
            "return_url": "https://t.me/Selameselambot",
            "customization": {
                "title": _("VIP Subscription", user_id), # Localize title for Chapa API
                "description": _("Unlock VIP features in the bot", user_id) # Localize description for Chapa API
            }
        }

        try:
            async with session.post(CHAPA_BASE_URL, json=payload, headers=headers) as resp:
                text_response = await resp.text() # Keep raw text for debugging
                data = None
                try:
                    data = await resp.json()
                except Exception as e_json:
                    logger.error(f"Chapa response for user {user_id} is not valid JSON: {text_response}. Error: {e_json}", exc_info=True)
                    await callback.message.answer(
                        _("❌ Chapa's response was unreadable. Please try again later.", user_id) # Localized
                    )
                    return

                if resp.status == 200 and data and data.get("status") == "success":
                    payment_url = data["data"]["checkout_url"]

                    # Save transaction to DB
                    conn = None
                    try:
                        conn = await create_database_connection()
                        await conn.execute(
                            """
                            INSERT INTO chapa_payments (user_id, tx_ref, plan, amount, status)
                            VALUES ($1, $2, $3, $4::NUMERIC, $5);
                            """,
                            user_id,
                            tx_ref,
                            vip_plan_name, # Storing the English name or internal identifier
                            vip_amount,
                            'pending'
                        )
                        logger.info(
                            f"Chapa payment record for {user_id} ({vip_plan_name}) with tx_ref {tx_ref} saved as pending."
                        )
                    except Exception as db_error:
                        logger.error(
                            f"DB error saving Chapa payment for {user_id}: {db_error}",
                            exc_info=True
                        )
                        await callback.message.answer(
                            _("⚠ Payment prepared, but failed to save record. Please contact support.", user_id) # Localized
                        )
                        return # Stop execution if DB save fails
                    finally:
                        if conn:
                            await conn.close()

                    # Send payment link to user
                    builder = InlineKeyboardBuilder()
                    builder.button(text=_("✅ Pay with Chapa", user_id), url=payment_url) # Localized button text
                    await callback.message.edit_text(
                        _("💳 Click below to complete your payment securely:", user_id), # Localized message
                        reply_markup=builder.as_markup()
                    )
                    logger.info(f"User {user_id} received Chapa payment link for {vip_plan_name}.")

                else:
                    error_message = data.get("message", "Unknown Chapa error") if data else "No response data"
                    logger.error(f"Chapa API error for user {user_id}. Status: {resp.status}, Response: {text_response}")
                    await callback.message.answer(
                        _("❌ Failed to create payment. Please try again later.", user_id) # Localized
                    )

        except aiohttp.ClientError as e_http:
            logger.error(f"HTTP client error during Chapa payment for user {user_id}: {e_http}", exc_info=True)
            await callback.message.answer(
                _("❌ Network error during payment initiation. Please check your connection and try again.", user_id) # Localized
            )
        except Exception as e:
            logger.error(f"An unexpected error occurred during Chapa payment for user {user_id}: {e}", exc_info=True)
            await callback.message.answer(
                _("❌ An unexpected error occurred while processing your payment. Please try again later.", user_id) # Localized
            )


# Assuming this is in db_utils.py or handlers.py
# You'll need your database connection function here
# from .db_utils import create_database_connection # Example import if in a separate file


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
    Grants VIP access to a user based on the payment source (Chapa or Telegram Stars)
    and the relevant payment detail (duration for Chapa, payload for Stars).
    This function *must* interact with your database to update VIP status.
    """
    conn = None  # Initialize connection
    try:
        logger.info(
            f"Attempting to grant VIP to user {user_id} via {source_type}.")

        expiry_date = None
        plan_name_for_calc = ""  # This will be used to pass to calculate_expiry_date

        if source_type == 'chapa':
            try:
                # payment_detail for Chapa is the duration in days (e.g., "365")
                duration_days = int(payment_detail)
                # For Chapa, we just need to get the "plan name" string for calculate_expiry_date
                # which then determines the timedelta.
                if duration_days == 7: plan_name_for_calc = "7 Days VIP"
                elif duration_days == 30: plan_name_for_calc = "30 Days VIP"
                elif duration_days == 90: plan_name_for_calc = "90 Days VIP"
                elif duration_days == 180: plan_name_for_calc = "180 Days VIP"
                elif duration_days == 365: plan_name_for_calc = "365 Days VIP"
                else:
                    logger.error(
                        f"Unknown Chapa duration: '{duration_days}'. Cannot map to VIP plan."
                    )
                    return False

                expiry_date = calculate_expiry_date(plan_name_for_calc)
                logger.info(
                    f"VIP duration from Chapa: {duration_days} days. Raw expiry: {expiry_date}"
                )

            except ValueError:
                logger.error(
                    f"Invalid duration_days for Chapa: '{payment_detail}'. Cannot grant VIP."
                )
                return False

        elif source_type == 'telegram_stars':
            # payment_detail for Stars is the payload (e.g., "premium_week_sub")
            if payment_detail == "premium_week_sub":
                plan_name_for_calc = "1 Week VIP"
            elif payment_detail == "premium_month_sub":
                plan_name_for_calc = "1 Month VIP"
            elif payment_detail == "premium_year_sub":
                plan_name_for_calc = "1 Year VIP"
            else:
                logger.error(
                    f"Unknown Telegram Stars payload: '{payment_detail}'. Cannot determine VIP duration."
                )
                return False

            expiry_date = calculate_expiry_date(plan_name_for_calc)
            logger.info(
                f"VIP duration from Telegram Stars payload '{payment_detail}' determined as {plan_name_for_calc}. Raw expiry: {expiry_date}"
            )
        else:
            logger.error(
                f"Unknown payment source type: {source_type}. Cannot grant VIP."
            )
            return False

        if not expiry_date:
            logger.error(
                f"Could not determine expiry date for user {user_id}. Source: {source_type}, Detail: {payment_detail}"
            )
            return False

        # --- DATABASE INTERACTION START ---
        # 1. Get DB connection/session
        conn = await create_database_connection()
        if not conn:
            logger.error(
                "Failed to acquire DB connection in grant_vip_access.")
            return False

        # 2. Fetch current user VIP status to determine if we need to extend or set new
        user_record = await conn.fetchrow(
            "SELECT vip_expires_at FROM users WHERE user_id = $1", user_id)
        current_vip_expiry = user_record[
            'vip_expires_at'] if user_record and 'vip_expires_at' in user_record else None

        final_expiry_date = expiry_date  # Default to the newly calculated expiry

        if current_vip_expiry and current_vip_expiry > datetime.now(
                timezone.utc):
            # If current VIP is still active, extend from the current expiry date
            # Calculate the duration of the new purchase
            duration_of_new_purchase = expiry_date - datetime.now(timezone.utc)
            final_expiry_date = current_vip_expiry + duration_of_new_purchase
            logger.info(
                f"Extending VIP for user {user_id}. Old expiry: {current_vip_expiry}, Adding: {duration_of_new_purchase}, New final expiry: {final_expiry_date}"
            )
        else:
            # If current VIP is expired or non-existent, set new expiry from now
            final_expiry_date = expiry_date
            logger.info(
                f"Setting new VIP for user {user_id}. New final expiry: {final_expiry_date}"
            )

        # 3. Update the user's record in your 'users' table
        await conn.execute(
            """
            UPDATE users
            SET is_vip = TRUE,
                vip_plan = $1,
                vip_expires_at = $2,
                notified_before_expiry = FALSE
            WHERE user_id = $3
            """,
            plan_name_for_calc,  # This variable now consistently holds the plan string like "1 Week VIP"
            final_expiry_date,
            user_id)

        logger.info(
            f"Database: User {user_id} VIP status updated. New expiry: {final_expiry_date}"
        )
        return True  # Return True if DB update was successful

    except Exception as e:
        logger.error(
            f"Error granting VIP access to user {user_id} (Source: {source_type}, Detail: {payment_detail}): {e}",
            exc_info=True)
        return False
    finally:
        if conn:
            await conn.close()  # Ensure connection is closed


async def check_and_deactivate_expired_vip(bot: Bot):
    """
    Checks for expired VIP subscriptions in the database and deactivates them.
    Also sends expiry notifications if 'notified_before_expiry' is FALSE.
    """
    conn = None
    try:
        conn = await create_database_connection()
        if not conn:
            logger.error(
                "Failed to acquire DB connection in check_and_deactivate_expired_vip."
            )
            return

        now_utc = datetime.now(timezone.utc)
        logger.info(f"Running VIP expiry check at {now_utc}.")

        # --- Phase 1: Notify users before expiry (e.g., 24 hours before) ---
        # Select users who are VIP, not yet notified, and expiring within 24 hours
        expiring_soon_threshold = now_utc + timedelta(hours=24)

        users_to_notify = await conn.fetch(
            """
            SELECT user_id, vip_expires_at, language
            FROM users
            WHERE is_vip = TRUE
              AND notified_before_expiry = FALSE
              AND vip_expires_at <= $1
              AND vip_expires_at > $2
            """,
            expiring_soon_threshold,
            now_utc
        )

        for user_data in users_to_notify:
            user_id = user_data['user_id']
            expires_at = user_data['vip_expires_at']
            user_language = user_data['language'] if user_data['language'] else 'en' # Get user's language
            time_until_expiry_hours = int((expires_at - now_utc).total_seconds() / 3600)

            try:
                # Localize the message
                message_text = _(
                    "⏰ Your VIP subscription will expire in less than {hours} hours ({expiry_date})!\n\n"
                    "Don't lose access to exclusive features like city-based matching. Renew your VIP status now: /vip",
                    user_id,
                    user_language
                ).format(
                    hours=time_until_expiry_hours,
                    expiry_date=expires_at.strftime('%Y-%m-%d %H:%M UTC')
                )
                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode=ParseMode.HTML # Assuming HTML parse mode is intended for the original formatting
                )
                await conn.execute(
                    "UPDATE users SET notified_before_expiry = TRUE WHERE user_id = $1",
                    user_id
                )
                logger.info(
                    f"Sent VIP expiry notification to user {user_id}. Expires at {expires_at}."
                )
            except Exception as e:
                logger.warning(
                    f"Could not send VIP expiry notification to user {user_id}: {e}"
                )
                # Don't mark as notified if message failed, so it can be retried

        # --- Phase 2: Deactivate expired VIPs ---
        # Select users who are VIP, and their expiry date is in the past
        expired_users = await conn.fetch(
            """
            SELECT user_id, vip_plan, language
            FROM users
            WHERE is_vip = TRUE AND vip_expires_at <= $1
            """, now_utc)

        for user_data in expired_users:
            user_id = user_data['user_id']
            vip_plan = user_data['vip_plan']
            user_language = user_data['language'] if user_data['language'] else 'en' # Get user's language
            logger.info(
                f"Deactivating VIP for user {user_id}. Plan: {vip_plan}. Expiry was in the past."
            )

            # Update user's VIP status
            await conn.execute(
                """
                UPDATE users
                SET is_vip = FALSE,
                    vip_expires_at = NULL,
                    vip_plan = NULL,
                    notified_before_expiry = FALSE
                WHERE user_id = $1
                """, user_id)

            # Optionally notify the user they've lost VIP access
            try:
                # Localize the message
                message_text = _(
                    "😔 Your VIP subscription has expired. You no longer have access to exclusive features.\n\n"
                    "Renew your VIP access anytime to unlock all premium benefits: /vip",
                    user_id,
                    user_language
                )
                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode=ParseMode.HTML # Assuming HTML parse mode is intended for original formatting
                )
                logger.info(f"Sent VIP expired message to user {user_id}.")
            except Exception as e:
                logger.warning(
                    f"Could not send VIP expired message to user {user_id}: {e}"
                )

    except Exception as e:
        logger.error(f"Error in check_and_deactivate_expired_vip task: {e}",
                     exc_info=True)
    finally:
        if conn:
            await conn.close()


# handlers.py (Add this new function)
# In webhook/handlers.py (at the very top, with other imports)


# --- Chapa Webhook Handler ---
async def chapa_webhook_handler(request: web.Request):
    """
    Handles incoming webhook notifications from Chapa.
    """
    conn = None
    tx_ref = None # Initialize tx_ref here so it's available in outer error logs
    user_id = None # Initialize user_id here for broader scope
    user_language = 'en' # Default language

    try:
        data = await request.json()
        logger.info(f"Received Chapa webhook: {data}")
    except Exception as e:
        logger.error(f"Failed to parse Chapa webhook JSON: {e}")
        return web.Response(status=400, text="Bad Request: Invalid JSON")

    tx_ref = data.get("tx_ref")
    if not tx_ref:
        logger.warning("Chapa webhook received without tx_ref.")
        return web.Response(status=400, text="Bad Request: Missing tx_ref")

    try:
        conn = await create_database_connection()
        if not conn:
            logger.error(
                "Failed to connect to DB for Chapa webhook verification.")
            return web.Response(status=500, text="Internal Server Error")

        # Fetch original record WITH user's language
        # IMPORTANT: Make sure your 'chapa_payments' table has a 'language' column
        original_record = await conn.fetchrow(
            "SELECT user_id, status, plan, language FROM chapa_payments WHERE tx_ref = $1",
            tx_ref
        )

        if original_record:
            user_id = original_record['user_id']
            current_status = original_record['status']
            plan_name = original_record['plan']
            # Get the user's language from the database, default to 'en' if not found
            user_language = original_record['language'] if original_record['language'] else 'en'
        else:
            logger.warning(
                f"Chapa payment for {tx_ref} not found in DB during webhook. Cannot verify or activate VIP."
            )
            # Return 200 OK to Chapa to avoid retries for unknown transactions
            return web.Response(status=200, text="Transaction not found in our records.")


        async with ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {config.CHAPA_SECRET_KEY}",
                "Content-Type": "application/json"
            }
            async with session.get(f"{config.CHAPA_VERIFY_URL}{tx_ref}", headers=headers) as resp:
                verify_data = await resp.json()
                logger.info(
                    f"Chapa verification response for {tx_ref} (user {user_id}): {verify_data}")

                if resp.status == 200 and \
                   verify_data.get("status") == "success" and \
                   verify_data["data"]["status"] == "success":

                    if current_status != 'success':
                        # Update chapa_payments table status
                        await conn.execute(
                            "UPDATE chapa_payments SET status = $1 WHERE tx_ref = $2",
                            'success', tx_ref
                        )
                        logger.info(
                            f"Chapa payment for {tx_ref} (user {user_id}) confirmed as SUCCESS and DB chapa_payments updated."
                        )

                        # --- Call grant_vip_access ---
                        duration_map = {
                            "1 Week VIP": 7,
                            "1 Month VIP": 30,
                            "3 Months VIP": 90,
                            "6 Months VIP": 180,
                            "1 Year VIP": 365
                        }
                        # Get the duration from the map, default to 30 days (1 month) if not found
                        chapa_duration_days = duration_map.get(plan_name, 30)

                        vip_granted = await grant_vip_access(
                            user_id, 'chapa', str(chapa_duration_days))

                        if vip_granted:
                            bot_instance = request.app["bot"]
                            # Calculate expiry date for display based on the granted duration
                            expiry_date_display = (datetime.now(timezone.utc) + timedelta(days=chapa_duration_days)).strftime('%Y-%m-%d %H:%M UTC')

                            try:
                                await bot_instance.send_message(
                                    chat_id=user_id,
                                    text=_(
                                        "🎉 Congratulations! Your 💎{plan_name}💎 VIP subscription has been activated! It will expire on **{expiry_date_display}**.",
                                        user_id,
                                        user_language
                                    ).format(
                                        plan_name=_(plan_name, user_id, user_language), # Localize plan name for display
                                        expiry_date_display=expiry_date_display
                                    ),
                                    parse_mode=ParseMode.HTML
                                )
                                logger.info(
                                    f"VIP activation message sent to user {user_id}."
                                )
                            except Exception as send_err:
                                logger.error(
                                    f"Failed to send VIP activation message to {user_id}: {send_err}"
                                )
                        else:
                            logger.error(
                                f"Failed to grant VIP access via grant_vip_access for user {user_id} after Chapa success."
                            )
                            # Potentially send a localized message to the user about an internal error
                            try:
                                bot_instance = request.app["bot"]
                                await bot_instance.send_message(
                                    chat_id=user_id,
                                    text=_("❌ We received your payment, but there was an issue activating your VIP status. Please contact support immediately.", user_id, user_language),
                                    parse_mode=ParseMode.HTML
                                )
                            except Exception as notif_err:
                                logger.error(f"Failed to notify user {user_id} about VIP activation error: {notif_err}")

                    else:
                        logger.info(
                            f"Chapa payment {tx_ref} already marked as success. Skipping update for user {user_id}."
                        )
                    return web.Response(status=200, text="Payment processed successfully.")

                else:
                    chapa_data_status = verify_data.get('data', {}).get('status', 'N/A')
                    logger.warning(
                        f"Chapa verification failed for {tx_ref} (user {user_id}). Status: {chapa_data_status}. Response: {verify_data}"
                    )
                    if chapa_data_status == 'failed' and current_status != 'failed':
                        await conn.execute(
                            "UPDATE chapa_payments SET status = $1 WHERE tx_ref = $2",
                            'failed', tx_ref
                        )
                        logger.info(
                            f"Chapa payment for {tx_ref} marked as FAILED in DB for user {user_id}."
                        )
                        try:
                            # Notify user about failed payment
                            bot_instance = request.app["bot"]
                            await bot_instance.send_message(
                                chat_id=user_id,
                                text=_("😔 Your Chapa payment for {plan_name} failed. Please try again or contact support if you believe this is an error.", user_id, user_language).format(
                                    plan_name=_(plan_name, user_id, user_language) # Localize plan name for display
                                ),
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as notif_err:
                            logger.error(f"Failed to notify user {user_id} about failed Chapa payment: {notif_err}")

                    return web.Response(
                        status=200, # Still return 200 to Chapa to avoid retries for a truly failed payment
                        text=f"Verification failed or not success: {chapa_data_status}"
                    )

    except ClientError as ce:
        logger.error(
            f"Network error during Chapa verification for {tx_ref} (user {user_id}): {ce}",
            exc_info=True
        )
        return web.Response(status=500, text="Internal Server Error: Network issue with Chapa API")
    except Exception as general_err:
        logger.error(
            f"Unexpected error in Chapa webhook handler for {tx_ref} (user {user_id}): {general_err}",
            exc_info=True
        )
        return web.Response(status=500, text="Internal Server Error: Unexpected issue")
    finally:
        if conn:
            await conn.close()

    return web.Response(status=200, text="Webhook received and processed.")
    # --- Function to set up the Aiogram Dispatcher and aiohttp.web Application ---


WEEKLY_STARS_AMOUNT = 100
WEEKLY_TITLE = "Premium Access (1 Week)"
WEEKLY_DESCRIPTION = "Unlock premium features for 7 days!"
WEEKLY_PAYLOAD = "premium_week_sub"

MONTHLY_STARS_AMOUNT = 250
MONTHLY_TITLE = "Premium Access (1 Month)"
MONTHLY_DESCRIPTION = "Unlock premium features for 30 days!"
MONTHLY_PAYLOAD = "premium_month_sub"

YEARLY_STARS_AMOUNT = 1000
YEARLY_TITLE = "Premium Access (1 Year)"
YEARLY_DESCRIPTION = "Unlock premium features for 365 days!"
YEARLY_PAYLOAD = "premium_year_sub"

PLAN_DETAILS = {
    "tgpay_week": {
        "amount": WEEKLY_STARS_AMOUNT,
        "title": WEEKLY_TITLE,
        "description": WEEKLY_DESCRIPTION,
        "payload": WEEKLY_PAYLOAD
    },
    "tgpay_1m": {
        "amount": MONTHLY_STARS_AMOUNT,
        "title": MONTHLY_TITLE,
        "description": MONTHLY_DESCRIPTION,
        "payload": MONTHLY_PAYLOAD
    },
    "tgpay_1y": {
        "amount": YEARLY_STARS_AMOUNT,
        "title": YEARLY_TITLE,
        "description": YEARLY_DESCRIPTION,
        "payload": YEARLY_PAYLOAD
    },
}

# handlers.py

# ... (other imports and code) ...


@router.callback_query(F.data.startswith("tgpay_"))
async def handle_tgpay_plan_selection(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    conn = None # Initialize conn to None for finally block
    user_language = 'en' # Default language for localization

    try:
        # Establish database connection to fetch user's language
        conn = await create_database_connection()
        if conn:
            user_language = await get_user_language_from_db(user_id, conn)
        else:
            logger.error(f"Failed to get DB connection for user {user_id} in tgpay_plan_selection. Using default language.")
            # We'll proceed with the default 'en' if DB connection fails,
            # and send a generic error message later if sending the invoice fails.

        # 1. Acknowledge the callback immediately with a localized message
        await callback.answer(
            _("Preparing your invoice...", user_id, user_language),
            show_alert=False
        )

        plan_callback_data = callback.data  # e.g., "tgpay_week"

        plan_details = PLAN_DETAILS.get(plan_callback_data)
        if not plan_details:
            logger.error(
                f"User {user_id} selected unknown Telegram Stars plan callback: {plan_callback_data}"
            )
            # Send a new localized message for the error
            await callback.message.answer(
                _("An error occurred: Invalid plan selected. Please try again.", user_id, user_language)
            )
            return

        amount = plan_details["amount"]
        # Use your localization function `_()` to get the translated title and description
        title = _(plan_details["title_key"], user_id, user_language)
        description = _(plan_details["description_key"], user_id, user_language)
        payload = plan_details["payload"]

        try:
            # 2. Delete the message that contained the plan selection keyboard
            # This removes the old message so you don't try to edit it.
            await callback.message.delete()
            logger.info(
                f"Deleted previous plan selection message for user {user_id}."
            )
        except Exception as e:
            logger.warning(
                f"Could not delete previous plan selection message for user {user_id}: {e}"
            )
            # Log the warning but continue, as deleting the message isn't critical path

        # 3. Send a *new* message confirming invoice readiness (localized)
        await bot.send_message(
            chat_id=user_id,
            text=_(
                "💫 Your invoice for **{title}** is ready!", user_id, user_language
            ).format(title=title), # Format the localized string with the localized title
            parse_mode=ParseMode.HTML  # Assuming you want bold text
        )
        logger.info(f"Sent 'invoice ready' message to user {user_id}.")

        # 4. Send the actual invoice
        await bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=description,
            payload=payload,
            currency="XTR", # The currency code for Telegram Stars is "XTR"
            prices=[types.LabeledPrice(label=title, amount=amount)],  # Use types.LabeledPrice
            provider_token="", # For Telegram Stars, the provider_token should be an empty string
            is_flexible=False,
        )
        logger.info(
            f"Invoice for {title} ({amount} Stars) sent to user {user_id}."
        )

    except Exception as e:
        logger.error(
            f"Failed to send Stars invoice to {user_id} for {plan_callback_data}: {e}",
            exc_info=True)
        # Send a new localized message to the user if sending the invoice fails
        await bot.send_message(
            chat_id=user_id,
            text=_(
                "Sorry, something went wrong while creating your invoice. Please try again later.",
                user_id,
                user_language
            )
        )
    finally:
        # Ensure the database connection is closed
        if conn:
            await conn.close()

@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery, bot: Bot):
    user_id = pre_checkout_query.from_user.id
    payload = pre_checkout_query.invoice_payload
    total_amount_stars = pre_checkout_query.total_amount  # Amount user is paying

    logger.info(
        f"Received pre_checkout_query from {user_id} for payload: '{payload}', amount: {total_amount_stars} Stars."
    )

    # Validate the payload against your defined products
    if payload in [WEEKLY_PAYLOAD, MONTHLY_PAYLOAD, YEARLY_PAYLOAD]:
        # Optional: You can verify the amount matches your expectations for the payload
        # expected_amount = next((p['amount'] for k, p in PLAN_DETAILS.items() if p['payload'] == payload), None)
        # if expected_amount is not None and total_amount_stars != expected_amount:
        #     await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=False, error_message="Payment amount mismatch.")
        #     logger.warning(f"Pre-checkout amount mismatch for {user_id} with payload '{payload}'. Expected {expected_amount}, got {total_amount_stars}.")
        #     return

        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
        logger.info(
            f"Pre-checkout query from {user_id} for payload '{payload}' answered OK."
        )
    else:
        await bot.answer_pre_checkout_query(
            pre_checkout_query.id,
            ok=False,
            error_message="Invalid product or service.")
        logger.warning(
            f"Pre-checkout query from {user_id} for unknown payload '{payload}' answered with error."
        )


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    user_id = message.from_user.id
    payment_info = message.successful_payment
    invoice_payload = payment_info.invoice_payload

    logger.info(f"Successful payment received from {user_id}: {payment_info}")

    conn = None # Initialize connection for finally block
    user_language = 'en' # Default language

    try:
        # Fetch user's language from the database
        conn = await create_database_connection()
        if conn:
            user_language = await get_user_language_from_db(user_id, conn)
        else:
            logger.error(f"Failed to get DB connection for user {user_id} in successful_payment_handler. Using default language.")

        # Call grant_vip_access for Telegram Stars payment
        if await grant_vip_access(user_id, 'telegram_stars', invoice_payload):
            # Determine the plan name for the message to the user
            # Default to localized "VIP" if no specific plan is found
            plan_name_display = _("VIP", user_id, user_language)

            for key, details in PLAN_DETAILS.items():
                if details["payload"] == invoice_payload:
                    # Get the localized title using the stored title_key
                    plan_name_display = _(details["title_key"], user_id, user_language)
                    break

            # Calculate expiry date to show to user
            # Pass the localized plan_name_display to calculate_expiry_date.
            # Ensure calculate_expiry_date can parse keywords in the localized string.
            expiry_date_for_display = calculate_expiry_date(
                plan_name_display
            ).strftime('%Y-%m-%d %H:%M UTC')

            # Send a localized success message
            await message.answer(
                _(
                    "🎉 Congratulations! Your 💎**{plan_name_display}**💎 VIP subscription has been activated! It will expire on **{expiry_date_for_display}**.",
                    user_id,
                    user_language
                ).format(
                    plan_name_display=plan_name_display,
                    expiry_date_for_display=expiry_date_for_display
                ),
                parse_mode=ParseMode.HTML
            )
            logger.info(
                f"User {user_id} successfully bought VIP with Stars via payload '{invoice_payload}'."
            )
        else:
            # Send a localized error message if VIP access couldn't be granted
            await message.answer(
                _("Thank you for your payment, but there was an issue granting your VIP access. Please contact support.", user_id, user_language)
            )
            logger.error(
                f"Failed to grant VIP access for user {user_id} with payload '{invoice_payload}'."
            )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred in successful_payment_handler for user {user_id}: {e}",
            exc_info=True
        )
        # Fallback localized error message for any unexpected exceptions
        await message.answer(
            _("An unexpected error occurred. Please try again later or contact support.", user_id, user_language)
        )
    finally:
        # Ensure the database connection is closed
        if conn:
            await conn.close()