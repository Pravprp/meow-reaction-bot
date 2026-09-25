import os
import random
import re
import threading
import time
import queue
from datetime import datetime, timezone, timedelta
from flask import Flask
from pymongo import MongoClient
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from waitress import serve

# ---------------- CONFIGURATION ----------------
TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")

MMB_CHAT_ID = int(os.environ.get("MMB_CHAT_ID") or 0)
MMG_CHAT_ID = int(os.environ.get("MMG_CHAT_ID") or 0)
MMB_FLIRT_CHAT_ID = int(os.environ.get("MMB_FLIRT_CHAT_ID") or 0)
MMG_FLIRT_CHAT_ID = int(os.environ.get("MMG_FLIRT_CHAT_ID") or 0)
MM_MEMES_CHAT_ID = int(os.environ.get("MM_MEMES_CHAT_ID") or 0)

# Create the bot and limit background workers to 3 to protect RAM
bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=3)
app = Flask(__name__)

# Pre-compile regex for commands (/meme and /flirt, supports /cmd@botname)
MEME_CMD = re.compile(r'^/meme(@\w+)?(\s|$)', re.IGNORECASE)
FLIRT_CMD = re.compile(r'^/flirt(@\w+)?(\s|$)', re.IGNORECASE)

# Extract numeric bot ID directly from token for instant admin checks
try:
    BOT_ID = int(TOKEN.split(":")[0])
except Exception:
    BOT_ID = 0

# Queue for outgoing messages (1 job at a time, 2-second rate-limit gap)
reaction_queue = queue.Queue(maxsize=1000)

# Dictionary to prevent database spam (cooldown memory)
activity_cooldowns = {}
COOLDOWN_TIME = 1800  # 30 minutes in seconds

# Names of private storage groups that should NEVER be treated as target groups
STORAGE_GROUP_NAMES = ["mmb flirt", "mmg flirt", "mmb", "mmg", "mm memes"]

# ---------------- MONGODB SETUP ----------------
mongo_client = None
db = None
users_col = None
group_members_col = None
public_groups_col = None
flirt_media_col = None
flirt_logs_col = None
manual_flirt_logs_col = None
topics_col = None
media_col = None
memes_col = None
meme_logs_col = None

if MONGO_URI:
    while True:
        try:
            mongo_client = MongoClient(MONGO_URI)
            db = mongo_client["telegram_bot"]

            users_col = db["users"]                  # Gender registrations via /start
            group_members_col = db["group_members"]  # Member roster per group
            public_groups_col = db["public_groups"]  # Target public groups
            flirt_media_col = db["flirt_media"]      # Flirt media from source storage groups
            flirt_logs_col = db["flirt_logs"]        # Dispatched scheduled flirts tracker
            manual_flirt_logs_col = db["manual_flirt_logs"]  # Tracks /flirt command usage
            topics_col = db["topics"]                # Forum topic keywords for MMB/MMG
            media_col = db["media"]                  # Reaction media for MMB/MMG
            memes_col = db["memes"]                  # Memes for MM Memes
            meme_logs_col = db["meme_logs"]          # Tracks daily /meme dispatches

            # Build indexes for rapid lookups
            users_col.create_index("user_id", unique=True)
            public_groups_col.create_index("chat_id", unique=True)
            group_members_col.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
            flirt_media_col.create_index([("gender", 1), ("message_id", 1)], unique=True)
            flirt_logs_col.create_index([("date", 1), ("user_id", 1)])
            manual_flirt_logs_col.create_index([("date", 1), ("chat_id", 1)])
            topics_col.create_index([("chat_id", 1), ("thread_id", 1)], unique=True)
            meme_logs_col.create_index([("date", 1), ("chat_id", 1)])

            print("Connected to MongoDB successfully.")
            break
            
        except Exception as e:
            print(f"MongoDB Connection Failed: {e}. Retrying in 10 seconds...")
            time.sleep(10)
else:
    print("CRITICAL ERROR: MONGO_URI is missing from environment variables.")

# ---------------- DATABASE & HELPER FUNCTIONS ----------------

def is_storage_group(chat_id, title=""):
    """Returns True if the group is one of the private media vaults by ID or Title."""
    if chat_id in [MMB_CHAT_ID, MMG_CHAT_ID, MMB_FLIRT_CHAT_ID, MMG_FLIRT_CHAT_ID, MM_MEMES_CHAT_ID]:
        return True
    title_clean = (title or "").strip().lower()
    return title_clean in STORAGE_GROUP_NAMES

def is_bot_admin(chat_id):
    """Verifies that the bot is an administrator in the target group."""
    if not BOT_ID:
        return False
    try:
        member = bot.get_chat_member(chat_id, BOT_ID)
        return member.status in ['administrator', 'creator']
    except Exception:
        return False

def get_user_gender(user_id):
    """Retrieves gender registered via /start ('m' or 'f')."""
    if users_col is None:
        return None
    user_doc = users_col.find_one({"user_id": user_id})
    return user_doc.get("gender") if user_doc else None

def track_activity(chat, user):
    """Indexes target groups and members who speak in them, using a cooldown to save CPU."""
    if chat.type not in ["group", "supergroup"] or user is None or user.is_bot:
        return

    if is_storage_group(chat.id, chat.title):
        return

    current_time = time.time()
    cache_key = f"{chat.id}_{user.id}"

    last_active = activity_cooldowns.get(cache_key, 0)
    if current_time - last_active < COOLDOWN_TIME:
        return

    if public_groups_col is not None:
        public_groups_col.update_one(
            {"chat_id": chat.id},
            {"$set": {"chat_id": chat.id, "title": chat.title or "", "last_active": current_time}},
            upsert=True
        )

    if group_members_col is not None:
        gender = get_user_gender(user.id)
        group_members_col.update_one(
            {"chat_id": chat.id, "user_id": user.id},
            {
                "$set": {
                    "first_name": user.first_name or "Friend",
                    "username": user.username or "",
                    "gender": gender,
                    "last_active": current_time
                }
            },
            upsert=True
        )
    
    activity_cooldowns[cache_key] = current_time

def save_topic(chat_id, thread_id, keyword):
    if topics_col is None:
        return
    topics_col.update_one(
        {"chat_id": chat_id, "thread_id": thread_id},
        {"$set": {"keyword": keyword.strip().lower()}},
        upsert=True
    )

def update_topic_keyword(chat_id, thread_id, new_keyword):
    if topics_col is None or media_col is None:
        return
    new_kw = new_keyword.strip().lower()
    old_topic = topics_col.find_one({"chat_id": chat_id, "thread_id": thread_id})
    if old_topic:
        old_kw = old_topic.get("keyword")
        topics_col.update_one({"chat_id": chat_id, "thread_id": thread_id}, {"$set": {"keyword": new_kw}})
        media_col.update_many({"chat_id": chat_id, "keyword": old_kw}, {"$set": {"keyword": new_kw}})
    else:
        save_topic(chat_id, thread_id, new_kw)

def get_keyword(chat_id, thread_id):
    if topics_col is None:
        return None
    doc = topics_col.find_one({"chat_id": chat_id, "thread_id": thread_id})
    return doc["keyword"] if doc else None

def save_media(chat_id, keyword, message_id):
    if media_col is None:
        return
    media_col.update_one(
        {"chat_id": chat_id, "message_id": message_id},
        {"$set": {"keyword": keyword.strip().lower()}},
        upsert=True
    )

def get_random_media(chat_id, keyword):
    if media_col is None:
        return None
    pipeline = [
        {"$match": {"chat_id": chat_id, "keyword": keyword.strip().lower()}},
        {"$sample": {"size": 1}}
    ]
    matches = list(media_col.aggregate(pipeline))
    return matches[0]["message_id"] if matches else None

def get_random_meme():
    if memes_col is None:
        return None
    matches = list(memes_col.aggregate([{"$sample": {"size": 1}}]))
    return matches[0]["message_id"] if matches else None

def remove_dead_media(chat_id, msg_id):
    if media_col is not None:
        media_col.delete_one({"chat_id": chat_id, "message_id": msg_id})
    if flirt_media_col is not None:
        flirt_media_col.delete_one({"message_id": msg_id})
    if memes_col is not None and chat_id == MM_MEMES_CHAT_ID:
        memes_col.delete_one({"message_id": msg_id})
    print(f"Purged deleted media ID {msg_id} from group {chat_id}")

# ---------------- QUEUE WORKER (ONE BY ONE, 2s GAP) ----------------

def process_queue():
    while True:
        try:
            job = reaction_queue.get()
            job_type = job.get("type")
            queued_time = job.get("queued_time", time.time())

            # Drop stale triggers older than 45 seconds
            if time.time() - queued_time <= 45:
                target_chat_id = job["target_chat_id"]
                storage_chat_id = job["storage_chat_id"]
                media_msg_id = job["media_msg_id"]

                # Case A: Keyword Reaction or On-Demand Meme
                if job_type == "reaction":
                    reply_to_id = job.get("reply_to_id")
                    try:
                        bot.copy_message(
                            chat_id=target_chat_id,
                            from_chat_id=storage_chat_id,
                            message_id=media_msg_id,
                            reply_to_message_id=reply_to_id
                        )
                    except ApiTelegramException as e:
                        err_msg = str(e).lower()
                        if "replied message not found" in err_msg or "reply" in err_msg:
                            try:
                                bot.copy_message(
                                    chat_id=target_chat_id,
                                    from_chat_id=storage_chat_id,
                                    message_id=media_msg_id
                                )
                            except Exception:
                                pass
                        elif "message to copy not found" in err_msg or "message can't be copied" in err_msg:
                            remove_dead_media(storage_chat_id, media_msg_id)
                    except Exception as e:
                        print(f"Reaction transfer error: {e}")

                # Case B: Flirt Dispatch (Copy Media -> Sleep 1s -> Tag User in Reply)
                elif job_type == "flirt":
                    user_id = job["user_id"]
                    first_name = job["first_name"]
                    try:
                        sent_msg = bot.copy_message(
                            chat_id=target_chat_id,
                            from_chat_id=storage_chat_id,
                            message_id=media_msg_id
                        )
                        sent_msg_id = getattr(sent_msg, 'message_id', sent_msg)

                        time.sleep(1)

                        clean_name = first_name.replace('[', '').replace(']', '')
                        mention = f"[{clean_name}](tg://user?id={user_id})"
                        bot.send_message(
                            chat_id=target_chat_id,
                            text=mention,
                            reply_to_message_id=sent_msg_id,
                            parse_mode="Markdown"
                        )
                    except ApiTelegramException as e:
                        err_msg = str(e).lower()
                        if "message to copy not found" in err_msg or "message can't be copied" in err_msg:
                            remove_dead_media(storage_chat_id, media_msg_id)
                    except Exception as e:
                        print(f"Flirt transfer error: {e}")

            reaction_queue.task_done()
            time.sleep(2)  # Rate limit gap to protect CPU

        except Exception as e:
            print(f"Queue worker exception: {e}")
            time.sleep(1)

# ---------------- DYNAMIC DAILY FLIRT ENGINE (UNCHANGED) ----------------

def generate_daily_schedule():
    m1 = random.randint(510, 595)
    m2 = random.randint(605, 690)
    a1 = random.randint(810, 930)
    e1 = random.randint(1110, 1195)
    e2 = random.randint(1205, 1290)
    return sorted([m1, m2, a1, e1, e2])

def dispatch_flirts_for_slot(slot_index):
    """Evaluates genuine public groups and dispatches automated flirts."""
    if public_groups_col is None or group_members_col is None or flirt_media_col is None or flirt_logs_col is None:
        return

    tz_ist = timezone(timedelta(hours=5, minutes=30))
    today_str = datetime.now(tz_ist).strftime("%Y-%m-%d")

    public_groups = list(public_groups_col.find())
    has_female_media = flirt_media_col.count_documents({"gender": "f"}) > 0
    has_male_media = flirt_media_col.count_documents({"gender": "m"}) > 0

    for group in public_groups:
        group_id = group["chat_id"]
        group_title = group.get("title", "")

        if is_storage_group(group_id, group_title):
            continue
        if not is_bot_admin(group_id):
            continue

        # 1. FLIRTS FOR GIRLS
        if has_female_media and MMG_FLIRT_CHAT_ID != 0:
            verified_girls = list(group_members_col.find({"chat_id": group_id, "gender": "f"}))
            total_girls = len(verified_girls)
            active_slots = min(total_girls, 5)

            if slot_index < active_slots:
                used_today = set(flirt_logs_col.distinct("user_id", {"date": today_str}))
                eligible_girls = [g for g in verified_girls if g["user_id"] not in used_today]

                if eligible_girls:
                    chosen_girl = random.choice(eligible_girls)
                    random_f_media = list(flirt_media_col.aggregate([
                        {"$match": {"gender": "f"}},
                        {"$sample": {"size": 1}}
                    ]))
                    media_id = random_f_media[0]["message_id"]

                    flirt_logs_col.insert_one({
                        "date": today_str,
                        "chat_id": group_id,
                        "user_id": chosen_girl["user_id"],
                        "gender": "f",
                        "slot": slot_index,
                        "timestamp": time.time()
                    })

                    reaction_queue.put_nowait({
                        "type": "flirt",
                        "target_chat_id": group_id,
                        "storage_chat_id": MMG_FLIRT_CHAT_ID,
                        "media_msg_id": media_id,
                        "user_id": chosen_girl["user_id"],
                        "first_name": chosen_girl.get("first_name", "Friend"),
                        "queued_time": time.time()
                    })

        # 2. FLIRTS FOR BOYS
        if has_male_media and MMB_FLIRT_CHAT_ID != 0:
            verified_boys = list(group_members_col.find({"chat_id": group_id, "gender": "m"}))
            total_boys = len(verified_boys)
            active_slots = min(total_boys, 5)

            if slot_index < active_slots:
                used_today = set(flirt_logs_col.distinct("user_id", {"date": today_str}))
                eligible_boys = [b for b in verified_boys if b["user_id"] not in used_today]

                if eligible_boys:
                    chosen_boy = random.choice(eligible_boys)
                    random_m_media = list(flirt_media_col.aggregate([
                        {"$match": {"gender": "m"}},
                        {"$sample": {"size": 1}}
                    ]))
                    media_id = random_m_media[0]["message_id"]

                    flirt_logs_col.insert_one({
                        "date": today_str,
                        "chat_id": group_id,
                        "user_id": chosen_boy["user_id"],
                        "gender": "m",
                        "slot": slot_index,
                        "timestamp": time.time()
                    })

                    reaction_queue.put_nowait({
                        "type": "flirt",
                        "target_chat_id": group_id,
                        "storage_chat_id": MMB_FLIRT_CHAT_ID,
                        "media_msg_id": media_id,
                        "user_id": chosen_boy["user_id"],
                        "first_name": chosen_boy.get("first_name", "Friend"),
                        "queued_time": time.time()
                    })

def flirt_scheduler_loop():
    tz_ist = timezone(timedelta(hours=5, minutes=30))
    current_day = None
    daily_schedule = []
    executed_slots = set()

    while True:
        try:
            now = datetime.now(tz_ist)
            today_str = now.strftime("%Y-%m-%d")
            total_minutes = now.hour * 60 + now.minute

            if current_day != today_str:
                current_day = today_str
                daily_schedule = generate_daily_schedule()
                executed_slots.clear()
                print(f"New daily schedule generated: {daily_schedule}")

            for idx, slot_minute in enumerate(daily_schedule):
                if total_minutes >= slot_minute and idx not in executed_slots:
                    executed_slots.add(idx)
                    threading.Thread(target=dispatch_flirts_for_slot, args=(idx,), daemon=True).start()

        except Exception as e:
            print(f"Flirt scheduler exception: {e}")

        seconds_to_sleep = 60 - datetime.now(tz_ist).second
        time.sleep(seconds_to_sleep)

# ---------------- DAILY RANDOM MORNING MEME PINNER ----------------

def daily_meme_pinner():
    tz_ist = timezone(timedelta(hours=5, minutes=30))
    pinned_today_date = None
    target_hour = random.randint(7, 10)
    target_minute = random.randint(0, 59)

    while True:
        try:
            now = datetime.now(tz_ist)
            today_str = now.strftime("%Y-%m-%d")
            curr_mins = now.hour * 60 + now.minute
            target_mins = target_hour * 60 + target_minute

            if pinned_today_date != today_str:
                if curr_mins >= target_mins:
                    if (
                        memes_col is not None
                        and public_groups_col is not None
                        and MM_MEMES_CHAT_ID != 0
                        and memes_col.count_documents({}) > 0
                    ):
                        random_meme_cursor = list(memes_col.aggregate([{"$sample": {"size": 1}}]))
                        public_groups = list(public_groups_col.find())

                        if random_meme_cursor and public_groups:
                            selected_meme = random_meme_cursor[0]["message_id"]

                            for group in public_groups:
                                group_id = group["chat_id"]
                                group_title = group.get("title", "")

                                if is_storage_group(group_id, group_title):
                                    continue
                                if not is_bot_admin(group_id):
                                    continue

                                try:
                                    sent = bot.copy_message(
                                        chat_id=group_id,
                                        from_chat_id=MM_MEMES_CHAT_ID,
                                        message_id=selected_meme
                                    )
                                    bot.pin_chat_message(
                                        chat_id=group_id,
                                        message_id=sent.message_id,
                                        disable_notification=False
                                    )
                                    print(f"Meme pinned successfully in {group_title} ({group_id}).")
                                    time.sleep(1)

                                except ApiTelegramException as e:
                                    err_msg = str(e).lower()
                                    if "message to copy not found" in err_msg or "message can't be copied" in err_msg:
                                        memes_col.delete_one({"message_id": selected_meme})
                                        break
                                    print(f"Failed to pin meme in group {group_id}: {e}")
                                except Exception as e:
                                    print(f"Error dispatching meme to {group_id}: {e}")

                            pinned_today_date = today_str
                            target_hour = random.randint(7, 10)
                            target_minute = random.randint(0, 59)
                            print(f"Daily meme broadcast finished for {today_str}.")

        except Exception as e:
            print(f"Meme scheduler error: {e}")

        seconds_to_sleep = 60 - datetime.now(tz_ist).second
        time.sleep(seconds_to_sleep)

# ---------------- DEPLOYMENT NOTIFICATION TASK ----------------

def notify_deployment():
    time.sleep(3)
    storage_targets = [
        ("MMB", MMB_CHAT_ID, "MMB Vault is Online!"),
        ("MMG", MMG_CHAT_ID, "MMG Vault is Online!"),
        ("MMB Flirt", MMB_FLIRT_CHAT_ID, "MMB Flirt Vault is Online!"),
        ("MMG Flirt", MMG_FLIRT_CHAT_ID, "MMG Flirt Vault is Online!"),
        ("MM Memes", MM_MEMES_CHAT_ID, "MM Memes Vault is Online!")
    ]

    for name, chat_id, message_text in storage_targets:
        if chat_id != 0:
            try:
                bot.send_message(chat_id, f"🤖 *{message_text}*", parse_mode="Markdown")
                time.sleep(1)
            except Exception as e:
                print(f"Failed to send deployment message to {name}: {e}")

# ---------------- KEEP-ALIVE SERVER ----------------
@app.route('/')
def home():
    return "Bot running 24/7 with Multi-Group Reactions and Dynamic Flirt Dispatcher!", 200

def run_web():
    port = int(os.environ.get("PORT", 8080))
    serve(app, host="0.0.0.0", port=port)

# ---------------- BOT HANDLERS ----------------

# A. Base Command: /getid
@bot.message_handler(commands=['getid'])
def send_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")

# B. Private Chat Commands: /start Gender Registration
def get_gender_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Male", callback_data="select_m"),
        types.InlineKeyboardButton("Female", callback_data="select_f")
    )
    return keyboard

@bot.message_handler(commands=['start'], func=lambda m: m.chat.type == "private")
def handle_start(message):
    bot.send_message(message.chat.id, "Select your gender:", reply_markup=get_gender_keyboard())

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_"))
def handle_gender_selection(call):
    bot.answer_callback_query(call.id)
    selected = call.data.split("_")[1]
    name = "Male (m)" if selected == "m" else "Female (f)"
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Confirm", callback_data=f"confirm_{selected}"),
        types.InlineKeyboardButton("Change", callback_data="change_gender")
    )
    bot.edit_message_text(f"Selected: {name}\nClick Confirm to save.", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data == "change_gender")
def handle_change(call):
    bot.answer_callback_query(call.id)
    bot.edit_message_text("Select your gender:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=get_gender_keyboard())

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_"))
def handle_confirm(call):
    bot.answer_callback_query(call.id)
    gender = call.data.split("_")[1]

    if users_col is not None:
        users_col.update_one(
            {"user_id": call.from_user.id},
            {"$set": {"user_id": call.from_user.id, "gender": gender}},
            upsert=True
        )
        if group_members_col is not None:
            group_members_col.update_many(
                {"user_id": call.from_user.id},
                {"$set": {"gender": gender}}
            )
        bot.edit_message_text(f"Saved! You are registered as {'Male (m)' if gender == 'm' else 'Female (f)'}.", chat_id=call.message.chat.id, message_id=call.message.message_id)
    else:
        bot.edit_message_text("Database connection error. Please try again later.", chat_id=call.message.chat.id, message_id=call.message.message_id)

# C. Media Uploads inside MMG Flirt and MMB Flirt (Source Storage Groups)
@bot.message_handler(content_types=['photo', 'animation', 'video'],
                     func=lambda m: (m.chat.id in [MMG_FLIRT_CHAT_ID, MMB_FLIRT_CHAT_ID] or (m.chat.title or "").strip().lower() in ["mmg flirt", "mmb flirt"]))
def index_flirt_media(message):
    title_lower = (message.chat.title or "").strip().lower()
    target_gender = "f" if (message.chat.id == MMG_FLIRT_CHAT_ID or title_lower == "mmg flirt") else "m"
    if flirt_media_col is not None:
        flirt_media_col.update_one(
            {"gender": target_gender, "message_id": message.message_id},
            {"$set": {"gender": target_gender, "message_id": message.message_id}},
            upsert=True
        )
        print(f"Indexed flirt media ID {message.message_id} for gender '{target_gender}'")

# D. Topic Creation in MMB or MMG
@bot.message_handler(content_types=['forum_topic_created'], func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def on_topic_created(message):
    name = message.forum_topic_created.name.strip().lower()
    save_topic(message.chat.id, message.message_thread_id, name)
    bot.reply_to(message, f"Topic auto-linked to keyword: '{name}'")

# E. Topic Renamed in MMB or MMG
@bot.message_handler(content_types=['forum_topic_edited'], func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def on_topic_edited(message):
    if message.forum_topic_edited.name:
        new_name = message.forum_topic_edited.name.strip().lower()
        update_topic_keyword(message.chat.id, message.message_thread_id, new_name)
        bot.reply_to(message, f"Topic updated to keyword: '{new_name}'")

# F. Media Uploads inside MMB or MMG topics
@bot.message_handler(content_types=['text', 'photo', 'animation', 'document', 'video', 'sticker'],
                     func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def index_media(message):
    if message.text and message.text.startswith('/'):
        return
    thread_id = message.message_thread_id
    if thread_id:
        keyword = get_keyword(message.chat.id, thread_id)
        if keyword:
            save_media(message.chat.id, keyword, message.message_id)

# G. MM Memes Media Storage
@bot.message_handler(content_types=['photo', 'animation', 'video', 'document'], func=lambda m: m.chat.id == MM_MEMES_CHAT_ID and m.chat.id != 0)
def index_memes(message):
    if memes_col is not None:
        memes_col.update_one(
            {"message_id": message.message_id},
            {"$set": {"message_id": message.message_id}},
            upsert=True
        )

# H. Public Group Activity, Commands & Reactions
@bot.message_handler(content_types=['text', 'photo', 'animation', 'video', 'document', 'sticker'],
                     func=lambda m: m.chat.type in ['group', 'supergroup'] and not is_storage_group(m.chat.id, m.chat.title))
def handle_public_group(message):
    track_activity(message.chat, message.from_user)

    if message.content_type != 'text' or message.reply_to_message is not None or not message.from_user:
        return

    text_clean = message.text.strip().lower()

    # 1. /meme Command Trigger (Limit: 5 times per day per group)
    if MEME_CMD.match(text_clean):
        tz_ist = timezone(timedelta(hours=5, minutes=30))
        today_str = datetime.now(tz_ist).strftime("%Y-%m-%d")

        if meme_logs_col is not None and MM_MEMES_CHAT_ID != 0:
            memes_sent_today = meme_logs_col.count_documents({"date": today_str, "chat_id": message.chat.id})
            if memes_sent_today < 5:
                selected_meme_id = get_random_meme()
                if selected_meme_id:
                    meme_logs_col.insert_one({
                        "date": today_str,
                        "chat_id": message.chat.id,
                        "user_id": message.from_user.id,
                        "message_id": selected_meme_id,
                        "timestamp": time.time()
                    })
                    try:
                        reaction_queue.put_nowait({
                            "type": "reaction",
                            "target_chat_id": message.chat.id,
                            "storage_chat_id": MM_MEMES_CHAT_ID,
                            "media_msg_id": selected_meme_id,
                            "reply_to_id": message.message_id,
                            "queued_time": time.time()
                        })
                    except queue.Full:
                        pass
        return

    # 2. /flirt Command Trigger (Limit: 1 time per day per group, sender must be registered)
    if FLIRT_CMD.match(text_clean):
        # Check if the user is registered in the database
        gender = get_user_gender(message.from_user.id)
        if not gender:
            # If the user is NOT in the database, ignore completely
            return

        tz_ist = timezone(timedelta(hours=5, minutes=30))
        today_str = datetime.now(tz_ist).strftime("%Y-%m-%d")

        if manual_flirt_logs_col is not None and flirt_media_col is not None:
            flirts_sent_today = manual_flirt_logs_col.count_documents({"date": today_str, "chat_id": message.chat.id})
            if flirts_sent_today < 1:
                storage_chat_id = MMB_FLIRT_CHAT_ID if gender == "m" else MMG_FLIRT_CHAT_ID
                if storage_chat_id != 0:
                    random_flirt = list(flirt_media_col.aggregate([
                        {"$match": {"gender": gender}},
                        {"$sample": {"size": 1}}
                    ]))
                    if random_flirt:
                        media_id = random_flirt[0]["message_id"]
                        manual_flirt_logs_col.insert_one({
                            "date": today_str,
                            "chat_id": message.chat.id,
                            "user_id": message.from_user.id,
                            "gender": gender,
                            "message_id": media_id,
                            "timestamp": time.time()
                        })
                        try:
                            reaction_queue.put_nowait({
                                "type": "flirt",
                                "target_chat_id": message.chat.id,
                                "storage_chat_id": storage_chat_id,
                                "media_msg_id": media_id,
                                "user_id": message.from_user.id,
                                "first_name": message.from_user.first_name or "Friend",
                                "queued_time": time.time()
                            })
                        except queue.Full:
                            pass
        return

    # 3. Topic Keyword Reactions for MMB / MMG
    gender = get_user_gender(message.from_user.id)
    if not gender:
        return

    trigger = text_clean
    storage_chat_id = MMB_CHAT_ID if gender == "m" else MMG_CHAT_ID
    selected_msg_id = get_random_media(storage_chat_id, trigger)

    if selected_msg_id:
        try:
            reaction_queue.put_nowait({
                "type": "reaction",
                "target_chat_id": message.chat.id,
                "storage_chat_id": storage_chat_id,
                "media_msg_id": selected_msg_id,
                "reply_to_id": message.message_id,
                "queued_time": time.time()
            })
        except queue.Full:
            pass

# ---------------- START SERVICES ----------------
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    threading.Thread(target=process_queue, daemon=True).start()
    threading.Thread(target=flirt_scheduler_loop, daemon=True).start()
    threading.Thread(target=daily_meme_pinner, daemon=True).start()
    threading.Thread(target=notify_deployment, daemon=True).start()
    bot.infinity_polling()
