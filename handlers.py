from aiogram import Router, F, Bot
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
    BotCommand,
)
import psycopg2
import psycopg2.extras
from psycopg2 import connect, extras
import config as config
import random
import logging
import time
import asyncio
from aiogram import Bot, Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder


logging.basicConfig(level=logging.INFO)

router = Router()
vip_search_locks = {} 
async def create_database_connection():
    """Creates and returns a database connection."""
    conn = connect(config.DATABASE_URL, cursor_factory=extras.RealDictCursor)
    return conn




async def set_commands(bot: Bot):
    commands = [
        types.BotCommand(command="start", description="Start the bot"),
        types.BotCommand(command="search", description="🔍 Search for a partner"),
        types.BotCommand(command="stop", description="🛑 Stop the current chat"),
        types.BotCommand(command="next", description="➡️ Find a new partner"),
        types.BotCommand(command="settings", description="⚙️ Update gender, age or location"),
        types.BotCommand(command="vip", description="💎 Become a VIP member"),
        types.BotCommand(command="credit", description="💰 Earn credit"),
        types.BotCommand(command="userid", description="🆔 Display your user ID"),
        
    ]
    await bot.set_my_commands(commands)
def location_keyboard():
    """Creates a reply keyboard for location sharing."""
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="📍 Share Location", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


from aiogram import Router, types, F, Bot
from aiogram.types import ReplyKeyboardRemove
 #Import the function

router = Router()

import aiohttp
from aiogram.types import ReplyKeyboardRemove

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
    location = message.location
    lat, lon = location.latitude, location.longitude
    city = await get_city_from_coords(lat, lon)

    if not city:
        await message.answer("⚠️ Could not detect your city. Please try again later.", reply_markup=ReplyKeyboardRemove())
        return

    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET location = %s WHERE user_id = %s", (city, message.from_user.id))
    conn.commit()
    cursor.close()
    conn.close()

    await message.answer(f"✅ Location set to: {city}", reply_markup=ReplyKeyboardRemove())
    await set_commands(bot)



    # Global dictionary to store current chat partners
current_chats = {}

def gender_keyboard(context="start"):
    """Creates an inline keyboard for gender selection."""
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="♂️ Male", callback_data=f"gender:{context}:male")],
        [types.InlineKeyboardButton(text="♀️ Female", callback_data=f"gender:{context}:female")],
        [types.InlineKeyboardButton(text="any", callback_data=f"gender:{context}:any")],
    ])
    return keyboard

@router.message(CommandStart())
async def cmd_start(message: types.Message, bot: Bot):
    """Handles the /start command."""
    logging.info(f"Received /start from user {message.from_user.id}")
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, gender, age, location FROM users WHERE user_id = %s", (message.from_user.id,))
    user = cursor.fetchone()
    logging.info(f"User data: {user}")
    if not user:
        logging.info("User does not exist, inserting.")
        cursor.execute("INSERT INTO users (user_id) VALUES (%s)", (message.from_user.id,))
        conn.commit()
        await message.answer(
            "👋 Welcome to the Anonymous Chat Bot! Let's get you set up.\n\n"
            "Please select your gender:",
            reply_markup=gender_keyboard()
        )
        logging.info("Sent gender keyboard.")
    elif user['gender'] is None or user['age'] is None:
        logging.info("User gender or age is None.")
        await message.answer(
            "⚠️ Your profile is incomplete. Please finish the setup.\n\n"
            "Select your gender:",
            reply_markup=gender_keyboard()
        )
        logging.info("Sent gender keyboard.")
    elif user['location'] is None:
        logging.info("User location is None.")
        await message.answer(
            "📍 Would you like to share your location for better matches?\n\n"
            "This is optional, but helps us find people near you.if not use /search command to find match",
            reply_markup=location_keyboard()
        )
        logging.info("Sent location keyboard.")
    else:
        logging.info("User profile is complete.")
        await message.answer("🎉 Welcome back! You're all set.")
        logging.info("Sent welcome back message.")
    await set_commands(bot)
    logging.info("Set commands.")
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")

@router.callback_query(F.data.startswith("gender:"))
async def gender_callback(query: types.CallbackQuery, bot: Bot):
    """Handles gender selection callback."""
    context, gender = query.data.split(":")[1], query.data.split(":")[2]  # Add context.
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET gender = %s WHERE user_id = %s", (gender, query.from_user.id))
    conn.commit()
    cursor.close()
    conn.close()
    if context == "change":
        await query.message.answer("✅ Gender updated!")
    await query.answer()
    if context == "start":  # Check context.
        await query.message.answer("🔢 Please enter your age:")
    await set_commands(bot) #Set commands after gender change.

@router.message(F.text.isdigit())
async def age_handler(message: types.Message, bot: Bot):
    """Handles age input."""
    age = int(message.text)
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET age = %s WHERE user_id = %s", (age, message.from_user.id))
    conn.commit()
    cursor.close()
    conn.close()
    await message.answer("✅ Your profile is complete!")
    await message.answer(
        "📍 Would you like to share your location for better matches?\n\n"
        "This is optional, but helps us find people near you.if not use /search command to find match",
        reply_markup=location_keyboard()
    )
    await set_commands(bot) #Set commands after age change.
@router.callback_query(F.data == "set_gender")
async def set_gender_handler(query: types.CallbackQuery):
    await query.message.answer("🔄 Select your new gender:", reply_markup=gender_keyboard(context="change"))
    await query.answer()

current_chats = {}  # Dictionary to store active chat pairs (user_id: partner_id)
def gender_selection_keyboard():
    """Creates an inline keyboard for gender selection."""
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="♂️ Male", callback_data="gender_pref:male")],
        [types.InlineKeyboardButton(text="♀️ Female", callback_data="gender_pref:female")],
        [types.InlineKeyboardButton(text="any", callback_data="gender_pref:any")],
    ])
    return keyboard
search_queue = [] 
match_lock = asyncio.Lock() # List to store searching users (user_id, timestamp, gender)

 # Ensure function exits safely
import asyncio

find_match_lock = asyncio.Lock()

async def find_match(user_id, gender_pref, is_vip):
    global current_chats, search_queue

    async with find_match_lock:
        try:
            # Verify user is in queue
            if not any(uid == user_id for uid, _, _ in search_queue):
                return False

            # Batch load info for all users in the queue
            user_ids = [uid for uid, _, _ in search_queue]
            conn = await create_database_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT user_id, is_vip, gender FROM users WHERE user_id = ANY(%s)", (user_ids,))
            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            user_info_map = {row['user_id']: row for row in rows}

            user_own_gender = user_info_map.get(user_id, {}).get('gender')
            if not user_own_gender:
                print(f"DEBUG: User {user_id} has no gender set.")
                return False

            potential_partners = []

            for other_user_id, _, other_user_gender_pref in search_queue:
                if other_user_id == user_id:
                    continue

                other_user_row = user_info_map.get(other_user_id)
                if not other_user_row:
                    continue

                other_user_is_vip = other_user_row['is_vip']
                other_user_gender = other_user_row['gender']

                # For non-VIP users, override their stored preference to "any"
                if not other_user_is_vip:
                    other_user_gender_pref = "any"

                # Does current user like the other user?
                i_like_them = (gender_pref == "any" or other_user_gender == gender_pref)

                # Does other user like the current user?
                they_like_me = (other_user_gender_pref == "any" or user_own_gender == other_user_gender_pref)

                # Match conditions
                if is_vip and other_user_is_vip:
                    if i_like_them and they_like_me:
                        potential_partners.append((other_user_id, other_user_gender))
                elif is_vip and not other_user_is_vip:
                    if i_like_them:
                        potential_partners.append((other_user_id, other_user_gender))
                elif not is_vip and other_user_is_vip:
                    if they_like_me:
                        potential_partners.append((other_user_id, other_user_gender))
                else:
                    potential_partners.append((other_user_id, other_user_gender))

            if potential_partners:
                partner_id, _ = random.choice(potential_partners)

                # Double-check both are still in queue
                ids_in_queue = {uid for uid, _, _ in search_queue}
                if user_id not in ids_in_queue or partner_id not in ids_in_queue:
                    return False

                # Remove matched users from queue
                search_queue[:] = [
                    (uid, ts, gen) for uid, ts, gen in search_queue
                    if uid not in (user_id, partner_id)
                ]

                # Add to current chats
                current_chats[user_id] = partner_id
                current_chats[partner_id] = user_id

                print(f"✅ MATCHED: {user_id} <-> {partner_id}")
                return partner_id

            return False

        except Exception as e:
            print(f"❌ ERROR in find_match(): {e}")
            return False


async def handle_vip_search(message: types.Message, bot: Bot):

       """Handles /search for VIP users."""
       await message.answer("Choose the gender you want to chat with:", reply_markup=gender_selection_keyboard())

@router.callback_query(F.data.startswith("gender_pref:"))
async def gender_preference_callback(query: types.CallbackQuery, bot: Bot):
    """Handles gender preference selection and initiates search for VIP users only."""
    global search_queue
    user_id = query.from_user.id
    gender_pref = query.data.split(":")[1]  # User's preferred gender to chat with

    # Remove the inline keyboard after selection
    await bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)

    # Clean up any existing chat
    if user_id in current_chats:
        partner_id = current_chats.pop(user_id, None)
        if partner_id and partner_id in current_chats:
            del current_chats[partner_id]
            await bot.send_message(partner_id, "Your partner has disconnected.")
        await query.message.answer("You were in a chat. Disconnected.")

    # --- Check VIP status & gender ---
    conn = await create_database_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("SELECT is_vip, gender FROM users WHERE user_id = %s", (user_id,))
    user_row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not user_row:
        await query.message.answer("⚠️ Could not retrieve your user info. Please try again.")
        return

    is_vip = user_row['is_vip']
    user_own_gender = user_row['gender']

    if not user_own_gender:
        await query.message.answer("⚠️ Please set your gender first using /setgender.")
        return

    if not is_vip:
        await query.message.answer("🚫 Gender-based matching is a VIP-only feature.")
        return
# Non-VIPs can't have gender preference
   

    search_queue.append((user_id, time.time(), gender_pref))

    # Send "searching..." message
    searching_message = await query.message.answer("🔍 Searching for a partner...")
    searching_message_id = searching_message.message_id

    # Attempt to find a match
    partner_id = await find_match(user_id, gender_pref, is_vip)

    if partner_id:
        await bot.delete_message(chat_id=query.message.chat.id, message_id=searching_message_id)

        await query.message.answer(
            "✅ Partner found! Start chatting!\n\n"
            "/next — find a new partner\n"
            "/stop — stop this chat"
        )

        await bot.send_message(partner_id,
            "✅ Partner found! Start chatting!\n\n"
            "/next — find a new partner\n"
            "/stop — stop this chat"
        )
   


        # Delete the partner's searching message (assuming you have this logic)
async def get_partner_searching_message_id(partner_id: int) -> int | None:
    """Retrieves the searching message ID for a given partner ID from the database."""
    conn = await create_database_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        cursor.execute("SELECT message_id FROM search_messages WHERE user_id = %s", (partner_id,))
        result = cursor.fetchone()  # Fetch one row
        if result:
            return result['message_id']  # Access the 'message_id' key
        else:
            return None
    except Exception as e:
        print(f"ERROR: Error in get_partner_searching_message_id: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


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
    #await query.answer()
@router.message(lambda message: message.text == "🚹 Search by Gender")
async def search_by_gender_handler(message: Message, bot: Bot):
    await handle_vip_search(message, bot)

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def search_menu_reply_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏙️ Search by City")],
            [ KeyboardButton(text="🚹 Search by Gender")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )
@router.callback_query(F.data == "set_location")
async def set_location_callback(query: types.CallbackQuery):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Share Location", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await query.message.answer("Please share your live location:", reply_markup=keyboard)
    await query.answer()


@router.message(Command("search"))
async def search_command(message: types.Message, bot: Bot):
    """Handles the /search command (simple random matching)."""
    await handle_non_vip_search(message, bot)


@router.message(Command("stop"))
async def stop_command(message: types.Message, bot: Bot):
    """Handles the /stop command."""
    global current_chats, search_queue
    user_id = message.from_user.id
    logging.info(f"Stop command from {user_id}. Current chats: {current_chats}")

    if user_id not in current_chats:
        await message.answer("You are not in an active chat.")
        logging.info(f"{user_id} is not in current_chats.")
        #Remove user from search que.
        search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
        return

    partner_id = current_chats[user_id]
    logging.info(f"Partner ID: {partner_id}")

    if partner_id in current_chats and current_chats[partner_id] == user_id:
        del current_chats[user_id]
        del current_chats[partner_id]
        logging.info(f"Chat stopped: {user_id} - {partner_id}. Current chats: {current_chats}")
        await bot.send_message(partner_id, "✅ Your partner has stopped the chat. /search to find a new partner",  reply_markup=search_menu_reply_keyboard())
        await message.answer("✅ Chat stopped. /search to find a new partner",  reply_markup=search_menu_reply_keyboard())
        
        #Remove users from the search queue.
        search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid not in (user_id, partner_id)]
    else:
        await message.answer("There was an issue stopping the chat.")
        logging.error(f"Error stopping chat: {user_id} - {partner_id}. Current chats: {current_chats}")
@router.message(Command("settings"))
async def settings_command(message: Message):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Change Gender", callback_data="set_gender")],
            [InlineKeyboardButton(text="📍 Set Location", callback_data="set_location")],
            [InlineKeyboardButton(text="🎂 Set Age", callback_data="set_age")]
        ]
    )
    await message.answer("⚙️ Choose what you want to update:", reply_markup=keyboard)


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


import logging

import logging
import datetime

async def get_user_credits(user_id):
    """Retrieves user credits from the database."""
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT credit, last_search_date, search_count FROM users WHERE user_id = %s", (user_id,)) # database column is credit.
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result:
        return {"credits": result['credit'], "last_search_date": result['last_search_date'], "search_count": result['search_count']}
    return {"credits": 0, "last_search_date": None, "search_count": 0}

async def update_user_credits(user_id, credits, last_search_date, search_count):
    """Updates user credits in the database."""
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET credit = %s, last_search_date = %s, search_count = %s WHERE user_id = %s", (credits, last_search_date, search_count, user_id)) # database column is credit.
    conn.commit()
    cursor.close()
    conn.close()

@router.message(Command("credit"))
async def credit_command(message: types.Message):
    """Handles the /credit command."""
    user_id = message.from_user.id
    user_data = await get_user_credits(user_id)  # Get current user data

    try:
        new_credits = user_data['credits'] + 10  # Calculate new credits
        await update_user_credits(user_id, new_credits, user_data['last_search_date'], user_data['search_count'])
        await message.answer(f"💰 You earned 10 credits! Your total credits: {new_credits}")
        logging.info(f"User {user_id} added 10 credits. Total: {new_credits}")

    except Exception as e:
        logging.error(f"Error adding credits for user {user_id}: {e}")
        await message.answer("❌ An error occurred while adding credits. Please try again later.")

from collections import defaultdict
import datetime
import asyncio
from aiogram.types import Message
from aiogram import Bot

# Initialize global variables at module level
search_queue = []
non_vip_search_locks = defaultdict(bool)
current_chats = {}

async def handle_non_vip_search(message: types.Message, bot: Bot):
    global search_queue, non_vip_search_locks, current_chats
    user_id = message.from_user.id
    today = datetime.date.today()

    if user_id in non_vip_search_locks and non_vip_search_locks[user_id]:
        await message.answer("Please wait for your previous search request to finish.")
        return

    try:
        user_data = await get_user_credits(user_id)

        if user_data.get('last_search_date') != today:
            user_data['search_count'] = 0
            await update_user_credits(user_id, user_data.get('credits', 0), today, 0)
            user_data['last_search_date'] = today

        current_search_count = user_data.get('search_count', 0)
        current_credits = user_data.get('credits', 0)
        needs_credit = current_search_count >= 10

        if needs_credit and current_credits <= 0:
            await message.answer("You have reached your daily search limit or have no credits. Use /credit to get more searches.")
            return

        non_vip_search_locks[user_id] = True
        
        new_search_count = current_search_count + 1
        new_credits = current_credits - 1 if needs_credit else current_credits
        await update_user_credits(user_id, new_credits, today, new_search_count)

        if user_id in current_chats:
            partner_id = current_chats.pop(user_id, None)
            if partner_id:
                current_chats.pop(partner_id, None)
                await bot.send_message(partner_id, "Your partner has disconnected to search for someone new.")
            await message.answer("You have been disconnected from your previous chat.use /search to find a partner")

        search_queue.append((user_id, time.time(), "any"))
        searching_message = await message.answer("🔍Searching for a partner...")

        match_made = await find_match(user_id, "any", False)

        if match_made:
            partner_id = current_chats.get(user_id)
            if partner_id:
                await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)
                await message.answer("✅ Partner found!start chatting!\n\n"
        "/next — find a new partner\n\n"
        "/stop — stop this chat")
                await bot.send_message(partner_id, "✅ Partner found!start chatting!\n\n"
        "/next — find a new partner\n\n"
        "/stop — stop this chat")
            non_vip_search_locks[user_id] = False
        
        else:
            # ✅ Exit early if already matched or disconnected during wait
            if user_id not in search_queue or user_id in current_chats:
                non_vip_search_locks[user_id] = False
                return

            await asyncio.sleep(20)

            if user_id in search_queue and user_id not in current_chats:
                search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
                await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)
                await message.answer("No users are currently available for chat. Removed from search queue.")
            elif user_id in current_chats:
                await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)

    except Exception as e:
        await message.answer("An error occurred during the search process. Please try again later.")
    
    finally:
        non_vip_search_locks[user_id] = False


@router.callback_query(F.data.startswith("gender:"))
async def gender_callback(query: types.CallbackQuery):
    """Handles gender selection callback."""
    gender = query.data.split(":")[1]
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET gender = %s WHERE user_id = %s", (gender, query.from_user.id))
    conn.commit()
    cursor.close()
    conn.close()
    await query.message.answer("✅ Gender updated!")
    await query.answer()    

@router.message(Command("next"))
async def next_command(message: types.Message, bot: Bot):
    """Handles the /next command by disconnecting and searching for a new partner."""
    global search_queue, current_chats, non_vip_search_locks

    user_id = message.from_user.id

    # Disconnect from current chat
    if user_id in current_chats:
        partner_id = current_chats.pop(user_id)
        current_chats.pop(partner_id, None)

        
        await bot.send_message(partner_id, "Your partner ended the chat. /search to find a new partner")
    else:
       info_msg = await message.answer("You're not currently in a chat. Searching for a partner...")


    # Mark user as searching (lock to prevent overlap)
    if user_id in non_vip_search_locks and non_vip_search_locks[user_id]:
        return  # avoid double searches

    non_vip_search_locks[user_id] = True
    search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]  # clean stale entry
    search_queue.append((user_id, time.time(), "any"))

    searching_message = await message.answer("✅ Chat ended.🔍 Searching for a partner...")

    match_made = await find_match(user_id, "any", False)

    if match_made:
        partner_id = current_chats.get(user_id)
        if partner_id:
            await bot.delete_message(chat_id=message.chat.id, message_id=info_msg.message_id)
            await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)
            await message.answer("✅ Partner found! Start chatting!\n\n/next — new partner\n/stop — end chat")
            await bot.send_message(partner_id, "✅ Partner found! Start chatting!\n\n/next — new partner\n/stop — end chat")
        non_vip_search_locks[user_id] = False
    else:
        # Wait max 20 seconds for match
        await asyncio.sleep(20)
        if user_id in search_queue and user_id not in current_chats:
            search_queue[:] = [(uid, ts, gen) for uid, ts, gen in search_queue if uid != user_id]
            await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)
            await message.answer("No users available right now. Removed from search queue.")
        elif user_id in current_chats:
            await bot.delete_message(chat_id=message.chat.id, message_id=searching_message.message_id)

    non_vip_search_locks[user_id] = False

#@router.message(Command("vip"))
#async def show_vip_options(message: types.Message):
    #await message.answer("Choose your VIP plan:", reply_markup=payment_method_keyboard)
@router.message(Command("vip"))
async def vip_command(message: Message):
    text = (
        "<b>💎 Become a VIP User</b>\n"
        "Support the chat and unlock premium features instantly.\n\n"
        "<b>Choose your preferred payment method:</b>"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="🧾 Telegram Payments", callback_data="pay_telegram")
    builder.button(text="💳 Chapa Payments", callback_data="pay_chapa")

    await message.answer(text, reply_markup=builder.as_markup()) 

@router.message(Command("userid"))
async def userid_command(message: types.Message):
    """Handles the /userid command."""
    await message.answer(f"Your User ID is: {message.from_user.id}")
    import psycopg2
import psycopg2.extras

async def get_user_by_id(user_id):
    try:
        conn = await create_database_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        return dict(row) if row else None

    except Exception as e:
        print(f"❌ Error in get_user_by_id: {e}")
        return None
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

class SettingsStates(StatesGroup):
    waiting_for_age = State()

@router.callback_query(F.data == "set_age")
async def ask_age(query: types.CallbackQuery, state: FSMContext):
    await query.message.answer("🔢 Please enter your age:")
    await state.set_state(SettingsStates.waiting_for_age)

@router.message(SettingsStates.waiting_for_age)
async def age_input_handler(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if text.isdigit():
        age = int(text)
        if 10 <= age <= 100:
            # Update database here
            # ...
            await message.answer(f"✅ Age set to: {age}")
            await state.clear()
        else:
            await message.answer("❌ Please enter a valid age between 10 and 100.")
    else:
        await message.answer("❌ Please enter a valid numeric age.")


from aiogram.types import Message
import psycopg2.extras
import time
import random
@router.message(lambda message: message.text == "🏙️ Search by City")
async def search_by_city_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    conn = await create_database_connection()

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Check VIP and location
        cursor.execute("SELECT is_vip, location FROM users WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if not row or not row['is_vip']:
            await message.answer("🚫 City-based matching is a VIP-only feature.")
            return

        user_location = row['location']
        if not user_location:
            await message.answer("📍 Please share your location first using /setlocation.")
            return

        city = user_location.strip()  # exact city string

        if user_id in current_chats:
            await message.answer("⚠️ You are already in a chat. Use /stop to end it first.")
            return

        search_queue.append((user_id, time.time(), city))
        searching_msg = await message.answer("🔍 Searching for a partner in your city...")

        cursor.execute("""
            SELECT user_id FROM users 
            WHERE user_id != %s AND location = %s
        """, (user_id, city))
        available_users = cursor.fetchall()

        available_users = [u['user_id'] for u in available_users if u['user_id'] not in current_chats]

        if not available_users:
            await bot.delete_message(chat_id=user_id, message_id=searching_msg.message_id)
            await message.answer("😔 No users available in your city right now.")
            return

        partner_id = random.choice(available_users)
        current_chats[user_id] = partner_id
        current_chats[partner_id] = user_id

        await bot.delete_message(chat_id=user_id, message_id=searching_msg.message_id)

        await bot.send_message(partner_id, "✅ City match found! You are now chatting.")
        await message.answer("✅ City match found! You are now chatting.")

        # Clean search queue
        search_queue[:] = [(uid, ts, loc) for uid, ts, loc in search_queue if uid not in (user_id, partner_id)]

    finally:
        cursor.close()
        conn.close()

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
    cursor.execute("INSERT INTO subscription_requests (user_id, payment_proof, request_date, status) VALUES (%s, %s, now(), %s)",(user_id, message.photo[-1].file_id, "pending"))
    conn.commit()
    cursor.close()
    conn.close()
    await bot.send_photo(admin_id, message.photo[-1].file_id, caption=f"User {user_id} requests VIP.")
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
    cursor.execute("UPDATE users SET is_vip = TRUE, subscription_expiry = now() + interval '30 days' WHERE user_id = %s", (user_id,))
    cursor.execute("UPDATE subscription_requests SET status = 'approved' WHERE user_id = %s", (user_id,))
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
    cursor.execute("UPDATE subscription_requests SET status = 'rejected' WHERE user_id = %s", (user_id,))
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
    cursor.execute("SELECT is_vip FROM users WHERE user_id = %s", (user_id,))
    is_vip = cursor.fetchone()['is_vip']
    cursor.close()
    conn.close()

    if is_vip and user_id in current_chats:
        partner_id = current_chats[user_id]
        await bot.send_voice(partner_id, message.voice.file_id)
    elif not is_vip:
        await message.answer("This is a VIP feature. Become a VIP to use voice messages.")

@router.message(Command("voicecall"))
async def voice_call_command(message: types.Message, bot: Bot):
    """Handles the /voicecall command (simulated)."""
    user_id = message.from_user.id
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT is_vip FROM users WHERE user_id = %s", (user_id,))
    is_vip = cursor.fetchone()['is_vip']
    cursor.close()
    conn.close()

    if is_vip and user_id in current_chats:
        partner_id = current_chats[user_id]
        await message.answer("📞 Initiating voice call (simulated).")
        await bot.send_message(partner_id, "📞 Incoming voice call (simulated).")
    elif not is_vip:
        await message.answer("This is a VIP feature. Become a VIP to use voice calls.")
    else:
        await message.answer("You are not currently in a chat.")
async def create_tables():
    """Creates necessary database tables if they don't exist."""
    conn = await create_database_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            gender TEXT,
            age INTEGER,
            location TEXT,
            is_vip BOOLEAN DEFAULT FALSE,
            subscription_expiry TIMESTAMP,
            pending_vip BOOLEAN DEFAULT FALSE,
            credit INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS subscription_requests (
            request_id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id),
            payment_proof TEXT,
            request_date TIMESTAMP,
            status TEXT
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
@router.message(Command("vip"))
async def vip_command(message: Message):
    text = (
        "<b>💎 Become a VIP User</b>\n"
        "Support the chat and unlock premium features instantly.\n\n"
        "<b>Choose your preferred payment method:</b>"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="🧾 Telegram Payments", callback_data="pay_telegram")
    builder.button(text="💳 Chapa Payments", callback_data="pay_chapa")

    await message.answer(text, reply_markup=builder.as_markup())

@router.callback_query(F.data == "pay_telegram")
async def choose_telegram_plan(callback: CallbackQuery):
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="100 ⭐ / $1.99 a week", callback_data="tgpay_week")
    keyboard.button(text="250 ⭐ / $3.99 a month", callback_data="tgpay_1m")
    keyboard.button(text="1000 ⭐ / $19.99 a year", callback_data="tgpay_1y")
    await callback.message.edit_text("💫 Choose your plan with Telegram Stars:", reply_markup=keyboard.as_markup())

@router.callback_query(F.data == "pay_chapa")
async def choose_chapa_plan(callback: CallbackQuery):
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="1 Month - 400 ETB", callback_data="chapa_1m")
    keyboard.button(text="6 Months - 1500 ETB", callback_data="chapa_6m")
    keyboard.button(text="1 Year - 2500 ETB", callback_data="chapa_1y")
    await callback.message.edit_text("Choose your Chapa plan:", reply_markup=keyboard.as_markup())
import aiohttp
import uuid

CHAPA_SECRET_KEY = config.CHAPA_SECRET_KEY
CHAPA_BASE_URL = "https://api.chapa.co/v1/transaction/initialize"
CHAPA_CALLBACK_URL = "https://yourdomain.com/chapa/webhook"  # Replace this

@router.callback_query(F.data.startswith("chapa_"))
async def handle_chapa_plan(callback: CallbackQuery):
    user_id = callback.from_user.id
    plan = callback.data
    tx_ref = str(uuid.uuid4())

    # Define pricing
    prices = {
        "chapa_1m": 400,
        "chapa_6m": 1500,
        "chapa_1y": 2500
    }
    amount = prices.get(plan)

    if not amount:
        await callback.answer("Invalid plan.", show_alert=True)
        return

    await callback.answer("Preparing Chapa payment...", show_alert=False)

    # Create payment request
    async with aiohttp.ClientSession() as session:
        headers = {
            "Authorization": f"Bearer {CHAPA_SECRET_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "amount": str(amount),
            "currency": "ETB",
            "email": f"user_{user_id}@bot.fake",
            "first_name": f"user_{user_id}",
            "tx_ref": tx_ref,
            "callback_url": CHAPA_CALLBACK_URL,
            "return_url": "https://t.me/Selameselambot",  # Replace with your bot
            "customization[title]": "VIP Subscription",
            "customization[description]": "Unlock VIP features in the bot"
        }

        async with session.post(CHAPA_BASE_URL, json=payload, headers=headers) as resp:
            data = await resp.json()

            if resp.status == 200 and data.get("status") == "success":
                payment_url = data["data"]["checkout_url"]

                # Store the transaction
                conn = await create_database_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO chapa_payments (user_id, tx_ref, plan) VALUES (%s, %s, %s)",
                    (user_id, tx_ref, plan)
                )
                conn.commit()
                cursor.close()
                conn.close()

                # Send user the payment link
                builder = InlineKeyboardBuilder()
                builder.button(text="✅ Pay with Chapa", url=payment_url)
                await callback.message.edit_text(
                    "💳 Click below to complete your payment securely:",
                    reply_markup=builder.as_markup()
                )
            else:
                await callback.message.answer("❌ Failed to create payment. Please try again later.")
