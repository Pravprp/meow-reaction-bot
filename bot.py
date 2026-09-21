import os
import random
import threading
import time
import queue
from datetime import datetime, timezone, timedelta
from flask import Flask
from pymongo import MongoClient
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# ---------------- CONFIGURATION ----------------
TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")

MMB_CHAT_ID = int(os.environ.get("MMB_CHAT_ID") or 0)
MMG_CHAT_ID = int(os.environ.get("MMG_CHAT_ID") or 0)
MMB_FLIRT_CHAT_ID = int(os.environ.get("MMB_FLIRT_CHAT_ID") or 0)
MMG_FLIRT_CHAT_ID = int(os.environ.get("MMG_FLIRT_CHAT_ID") or 0)
MM_MEMES_CHAT_ID = int(os.environ.get("MM_MEMES_CHAT_ID") or 0)

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# Extract numeric bot ID directly from the token for instant admin checks
try:
    BOT_ID = int(TOKEN.split(":")[0])
except Exception:
    BOT_ID = 0

# Queue for outgoing messages (1 job at a time, 2-second rate-limit gap)
reaction_queue = queue.Queue(maxsize=1000)

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
topics_col = None
media_col = None
memes_col = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["telegram_bot"]

        users_col = db["users"]                  # Gender registrations via /start
        group_members_col = db["group_members"]  # Member roster per group
        public_groups_col = db["public_groups"]  # Target public groups (e.g. Walkie Talkies)
        flirt_media_col = db["flirt_media"]      # Flirt media from source storage groups
        flirt_logs_col = db["flirt_logs"]        # Dispatched flirts tracker
        topics_col = db["topics"]                # Forum topic keywords for MMB/MMG
        media_col = db["media"]                  # Reaction media for MMB/MMG
        memes_col = db["memes"]                  # Memes storage for MM Memes

        # Build indexes for rapid lookups
        users_col.create_index("user_id", unique=True)
        public_groups_col.create_index("chat_id", unique=True)
        group_members_col.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
        flirt_media_col.create_index([("gender", 1), ("message_id", 1)], unique=True)
        flirt_logs_col.create_index([("date", 1), ("user_id", 1)])
        topics_col.create_index([("chat_id", 1), ("thread_id", 1)], unique=True)

        print("Connected to MongoDB successfully.")
    except Exception as e:
        print(f"MongoDB Initialization Error: {e}")

# ---------------- DATABASE & HELPER FUNCTIONS ----------------

def is_storage_group(chat_id, title=""):
    """Returns True if the group is one of our private media vaults by ID or Title."""
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
    """Indexes target groups and members who speak in them."""
    if chat.type not in ["group", "supergroup"] or user is None or user.is_bot:
        return

    # Safety Lock: Never register private storage vaults as public target groups
    if is_storage_group(chat.id, chat.title):
        return

    if public_groups_col is not None:
        public_groups_col.update_one(
            {"chat_id": chat.id},
            {"$set": {"chat_id": chat.id, "title": chat.title or "", "last_active": time.time()}},
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
                    "last_active": time.time()
                }
            },
            upsert=True
        )

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
    matches = list(media_col.find({"chat_id": chat_id, "keyword": keyword.strip().lower()}))
    return random.choice(matches)["message_id"] if matches else None

def remove_dead_media(chat_id, msg_id):
    if media_col is not None:
        media_col.delete_one({"chat_id": chat_id, "message_id": msg_id})
    if flirt_media_col is not None:
        flirt_media_col.delete_one({"message_id": msg_id})
    if memes_col is not None:
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

                # Case A: Keyword Reaction in Public Group
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

                # Case B: Flirt Dispatch (Send Meme first -> Then Tag in Reply)
                elif job_type == "flirt":
                    user_id = job["user_id"]
                    first_name = job["first_name"]
                    try:
                        # 1. Send the meme from the storage group into target group (e.g. Walkie Talkies)
                        sent_msg = bot.copy_message(
                            chat_id=target_chat_id,
                            from_chat_id=storage_chat_id,
                            message_id=media_msg_id
                        )
                        sent_msg_id = getattr(sent_msg, 'message_id', sent_msg)

                        # Small 1-second pause before replying with mention
                        time.sleep(1)

                        # 2. Tag the target user as a direct reply without saying "Hey"
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
            time.sleep(2)  # 2-second rate-limit buffer to protect CPU

        except Exception as e:
            print(f"Queue worker exception: {e}")
            time.sleep(1)

# ---------------- DYNAMIC DAILY FLIRT ENGINE ----------------

def generate_daily_schedule():
    """
    Generates 5 distinct time slots across the day (minutes from midnight IST):
    Slot 0: Morning 1 (08:30 - 09:55)
    Slot 1: Morning 2 (10:05 - 11:30)
    Slot 2: Afternoon (13:30 - 15:30)
    Slot 3: Evening 1 (18:30 - 19:55)
    Slot 4: Evening 2 (20:05 - 21:30)
    """
    m1 = random.randint(510, 595)
    m2 = random.randint(605, 690)
    a1 = random.randint(810, 930)
    e1 = random.randint(1110, 1195)
    e2 = random.randint(1205, 1290)
    return sorted([m1, m2, a1, e1, e2])

def dispatch_flirts_for_slot(slot_index):
    """Evaluates genuine public groups (e.g. Walkie Talkies) and dispatches flirts."""
    if public_groups_col is None or group_members_col is None or flirt_media_col is None or flirt_logs_col is None:
        return

    tz_ist = timezone(timedelta(hours=5, minutes=30))
    today_str = datetime.now(tz_ist).strftime("%Y-%m-%d")

    public_groups = list(public_groups_col.find())
    female_media = list(flirt_media_col.find({"gender": "f"}))
    male_media = list(flirt_media_col.find({"gender": "m"}))

    for group in public_groups:
        group_id = group["chat_id"]
        group_title = group.get("title", "")

        # Safeguard: Never send flirts into storage groups
        if is_storage_group(group_id, group_title):
            continue

        # Only dispatch if the bot is an Administrator in the target group
        if not is_bot_admin(group_id):
            continue

        # ---------------- 1. FLIRTS FOR GIRLS ----------------
        if female_media and MMG_FLIRT_CHAT_ID != 0:
            verified_girls = list(group_members_col.find({"chat_id": group_id, "gender": "f"}))
            total_girls = len(verified_girls)

            active_slots = min(total_girls, 5)

            if slot_index < active_slots:
                used_today = set(flirt_logs_col.distinct("user_id", {"date": today_str}))
                eligible_girls = [g for g in verified_girls if g["user_id"] not in used_today]

                if eligible_girls:
                    chosen_girl = random.choice(eligible_girls)
                    media_id = random.choice(female_media)["message_id"]

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

        # ---------------- 2. FLIRTS FOR BOYS ----------------
        if male_media and MMB_FLIRT_CHAT_ID != 0:
            verified_boys = list(group_members_col.find({"chat_id": group_id, "gender": "m"}))
            total_boys = len(verified_boys)

            active_slots = min(total_boys, 5)

            if slot_index < active_slots:
                used_today = set(flirt_logs_col.distinct("user_id", {"date": today_str}))
                eligible_boys = [b for b in verified_boys if b["user_id"] not in used_today]

                if eligible_boys:
                    chosen_boy = random.choice(eligible_boys)
                    media_id = random.choice(male_media)["message_id"]

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
    """Monitors the 5 slots and triggers dynamic dispatches."""
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
                print(f"Generated new flirt schedule for {today_str}: {daily_schedule}")

            for idx, slot_minute in enumerate(daily_schedule):
                if total_minutes >= slot_minute and idx not in executed_slots:
                    executed_slots.add(idx)
                    print(f"Executing flirt slot {idx + 1}/5 at minute {total_minutes} IST")
                    dispatch_flirts_for_slot(idx)

        except Exception as e:
            print(f"Flirt scheduler exception: {e}")

        time.sleep(60)

# ---------------- DEPLOYMENT NOTIFICATION TASK ----------------

def notify_deployment():
    """Sends a one-time verification message to storage groups once deployment is active."""
    time.sleep(3)

    # Notify MM Memes
    if MM_MEMES_CHAT_ID != 0:
        try:
            bot.send_message(MM_MEMES_CHAT_ID, "you can use this group to store the memes.")
            print("Deployment notification sent to MM Memes.")
        except Exception as e:
            print(f"Failed to send deployment message to MM Memes: {e}")

    # Notify MMB Flirt
    if MMB_FLIRT_CHAT_ID != 0:
        try:
            bot.send_message(MMB_FLIRT_CHAT_ID, "You can use this group now to store flirt images.")
            print("Deployment notification sent to MMB Flirt.")
        except Exception as e:
            print(f"Failed to send deployment message to MMB Flirt: {e}")

    # Notify MMG Flirt
    if MMG_FLIRT_CHAT_ID != 0:
        try:
            bot.send_message(MMG_FLIRT_CHAT_ID, "You can use this group now to store flirt images.")
            print("Deployment notification sent to MMG Flirt.")
        except Exception as e:
            print(f"Failed to send deployment message to MMG Flirt: {e}")

# ---------------- BOT HANDLERS ----------------

# A. Base Command: /getid works everywhere
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

# C. Media Storage: MMG Flirt and MMB Flirt (Silent indexing, zero responses)
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

# D. Media Storage: MM Memes (Silent indexing, zero responses)
@bot.message_handler(content_types=['photo', 'animation', 'video', 'document'],
                     func=lambda m: (m.chat.id == MM_MEMES_CHAT_ID or (m.chat.title or "").strip().lower() == "mm memes") and m.chat.id != 0)
def index_memes(message):
    if memes_col is not None:
        memes_col.update_one(
            {"message_id": message.message_id},
            {"$set": {"message_id": message.message_id}},
            upsert=True
        )
        print(f"Indexed meme ID {message.message_id} in MM Memes storage.")

# E. Topic Creation in MMB or MMG (Auto-reply restored)
@bot.message_handler(content_types=['forum_topic_created'],
                     func=lambda m: (m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] or (m.chat.title or "").strip().lower() in ["mmb", "mmg"]) and m.chat.id != 0)
def on_topic_created(message):
    name = message.forum_topic_created.name.strip().lower()
    save_topic(message.chat.id, message.message_thread_id, name)
    try:
        bot.reply_to(message, f"Topic auto-linked to keyword: '{name}'")
    except Exception as e:
        print(f"Error replying to topic creation: {e}")

# F. Topic Renamed in MMB or MMG (Auto-reply restored)
@bot.message_handler(content_types=['forum_topic_edited'],
                     func=lambda m: (m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] or (m.chat.title or "").strip().lower() in ["mmb", "mmg"]) and m.chat.id != 0)
def on_topic_edited(message):
    if message.forum_topic_edited.name:
        new_name = message.forum_topic_edited.name.strip().lower()
        update_topic_keyword(message.chat.id, message.message_thread_id, new_name)
        try:
            bot.reply_to(message, f"Topic updated to keyword: '{new_name}'")
        except Exception as e:
            print(f"Error replying to topic rename: {e}")

# G. Media Uploads inside MMB or MMG topics (Silent indexing, zero responses)
@bot.message_handler(content_types=['text', 'photo', 'animation', 'document', 'video', 'sticker'],
                     func=lambda m: (m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] or (m.chat.title or "").strip().lower() in ["mmb", "mmg"]) and m.chat.id != 0)
def index_media(message):
    if message.text and message.text.startswith('/'):
        return
    thread_id = message.message_thread_id
    if thread_id:
        keyword = get_keyword(message.chat.id, thread_id)
        if keyword:
            save_media(message.chat.id, keyword, message.message_id)

# H. Public Group Activity & Reaction Listener (Only runs on genuine public chat groups)
@bot.message_handler(content_types=['text', 'photo', 'animation', 'video', 'document', 'sticker'],
                     func=lambda m: m.chat.type in ['group', 'supergroup'] and not is_storage_group(m.chat.id, m.chat.title))
def handle_public_group(message):
    # Track presence of users who speak in public groups
    track_activity(message.chat, message.from_user)

    # Keywords apply only to plain text non-replies
    if message.content_type != 'text' or message.reply_to_message is not None or not message.from_user:
        return

    gender = get_user_gender(message.from_user.id)
    if not gender:
        return

    trigger = message.text.strip().lower()
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
    threading.Thread(target=notify_deployment, daemon=True).start()
    bot.infinity_polling()
