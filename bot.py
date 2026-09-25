import os
import random
import threading
import time
import queue
from datetime import datetime, timezone, timedelta
from itertools import islice

from flask import Flask, request, abort
from pymongo import MongoClient, UpdateOne
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from waitress import serve

# ---------------- CONFIGURATION ----------------
TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g., https://my-bot.onrender.com

MMB_CHAT_ID = int(os.environ.get("MMB_CHAT_ID") or 0)
MMG_CHAT_ID = int(os.environ.get("MMG_CHAT_ID") or 0)
MMB_FLIRT_CHAT_ID = int(os.environ.get("MMB_FLIRT_CHAT_ID") or 0)
MMG_FLIRT_CHAT_ID = int(os.environ.get("MMG_FLIRT_CHAT_ID") or 0)
MM_MEMES_CHAT_ID = int(os.environ.get("MM_MEMES_CHAT_ID") or 0)

# Global API Timeouts (Prevents worker threads from freezing indefinitely on network hangs)
telebot.apihelper.CONNECT_TIMEOUT = 10
telebot.apihelper.READ_TIMEOUT = 10

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=3)
app = Flask(__name__)

try:
    BOT_ID = int(TOKEN.split(":")[0])
except Exception:
    BOT_ID = 0

reaction_queue = queue.Queue(maxsize=1000)

# ---------------- RAM CACHES & BATCHING ----------------
activity_cooldowns = {}
COOLDOWN_TIME = 1800  # 30 minutes in seconds

gender_cache = {}
spam_cooldowns = {}
group_rate_limits = {}  
group_command_cooldowns = {}  
SPAM_LIMIT_SECONDS = 2  

STORAGE_GROUP_NAMES = ["mmb flirt", "mmg flirt", "mmb", "mmg", "mm memes"]

# Batch Writing Storage (Saves DB I/O)
pending_group_updates = {}
pending_member_updates = {}
batch_lock = threading.Lock()

# ---------------- HELPER TO SAFELY PRUNE CACHES ----------------
def prune_cache(cache_dict, max_size, prune_amount):
    """Uses itertools.islice to safely prune dictionaries without massive RAM spikes."""
    if len(cache_dict) > max_size:
        keys_to_delete = list(islice(cache_dict.keys(), prune_amount))
        for k in keys_to_delete:
            cache_dict.pop(k, None)

# ---------------- MONGODB (NON-BLOCKING SETUP) ----------------
mongo_client = None
db = None
users_col = None
group_members_col = None
public_groups_col = None
flirt_media_col = None
flirt_logs_col = None
flirt_manual_logs_col = None  
topics_col = None
media_col = None
memes_col = None
meme_logs_col = None

def init_db():
    """Initializes MongoDB in the background so Flask can boot instantly on Render."""
    global mongo_client, db, users_col, group_members_col, public_groups_col
    global flirt_media_col, flirt_logs_col, flirt_manual_logs_col
    global topics_col, media_col, memes_col, meme_logs_col

    if not MONGO_URI:
        print("CRITICAL ERROR: MONGO_URI is missing from environment variables.")
        return

    while True:
        try:
            # serverSelectionTimeoutMS ensures the connection fails fast if network hangs
            mongo_client = MongoClient(MONGO_URI, maxPoolSize=10, minPoolSize=1, serverSelectionTimeoutMS=5000)
            db = mongo_client["telegram_bot"]

            users_col = db["users"]                  
            group_members_col = db["group_members"]  
            public_groups_col = db["public_groups"]  
            flirt_media_col = db["flirt_media"]      
            flirt_logs_col = db["flirt_logs"]        
            flirt_manual_logs_col = db["flirt_manual_logs"] 
            topics_col = db["topics"]                
            media_col = db["media"]                  
            memes_col = db["memes"]                  
            meme_logs_col = db["meme_logs"]          

            users_col.create_index("user_id", unique=True)
            public_groups_col.create_index("chat_id", unique=True)
            group_members_col.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
            flirt_media_col.create_index([("gender", 1), ("message_id", 1)], unique=True)
            flirt_logs_col.create_index([("date", 1), ("user_id", 1)])
            flirt_manual_logs_col.create_index([("date", 1), ("chat_id", 1)])
            topics_col.create_index([("chat_id", 1), ("thread_id", 1)], unique=True)
            meme_logs_col.create_index([("date", 1), ("chat_id", 1)])

            print("Connected to MongoDB successfully.")
            break
            
        except Exception as e:
            print(f"MongoDB Connection Failed: {e}. Retrying in 10 seconds...")
            time.sleep(10)

# ---------------- DATABASE & HELPER FUNCTIONS ----------------

def is_storage_group(chat_id, title=""):
    if chat_id in [MMB_CHAT_ID, MMG_CHAT_ID, MMB_FLIRT_CHAT_ID, MMG_FLIRT_CHAT_ID, MM_MEMES_CHAT_ID]:
        return True
    title_clean = (title or "").strip().lower()
    return title_clean in STORAGE_GROUP_NAMES

def is_bot_admin(chat_id):
    if not BOT_ID:
        return False
    try:
        member = bot.get_chat_member(chat_id, BOT_ID)
        return member.status in ['administrator', 'creator']
    except Exception:
        return False

def get_user_gender(user_id):
    if user_id in gender_cache:
        return gender_cache[user_id]
        
    if users_col is None:
        return None
        
    try:
        user_doc = users_col.find_one({"user_id": user_id})
        gender = user_doc.get("gender") if user_doc else None
        
        if gender:
            prune_cache(gender_cache, 5000, 1000)
            gender_cache[user_id] = gender
            
        return gender
    except Exception as e:
        print(f"Database error in get_user_gender: {e}")
        return None

def is_group_rate_limited(chat_id):
    current_time = time.time()
    group_data = group_rate_limits.get(chat_id, {"count": 0, "reset_time": current_time + 60})
    
    if current_time > group_data["reset_time"]:
        group_data = {"count": 0, "reset_time": current_time + 60}
        
    if group_data["count"] >= 18:
        return True 
        
    group_data["count"] += 1
    group_rate_limits[chat_id] = group_data
    
    prune_cache(group_rate_limits, 2000, 500)
    return False

def track_activity(chat, user):
    """Queues tracking data in RAM instead of instantly writing to MongoDB."""
    if chat.type not in ["group", "supergroup"] or user is None or user.is_bot:
        return

    if is_storage_group(chat.id, chat.title):
        return

    current_time = time.time()
    cache_key = f"{chat.id}_{user.id}"

    last_active = activity_cooldowns.get(cache_key, 0)
    if current_time - last_active < COOLDOWN_TIME:
        return  

    activity_cooldowns[cache_key] = current_time
    prune_cache(activity_cooldowns, 10000, 2000)

    # Resolve gender in the background to not hold up the main thread
    gender = get_user_gender(user.id)

    # Stage the data for background batch writing
    with batch_lock:
        pending_group_updates[chat.id] = {
            "title": chat.title or "",
            "last_active": current_time
        }
        pending_member_updates[cache_key] = {
            "chat_id": chat.id,
            "user_id": user.id,
            "first_name": user.first_name or "Friend",
            "username": user.username or "",
            "gender": gender,
            "last_active": current_time
        }

def db_batch_writer_loop():
    """Runs every 5 minutes. Writes all pending group/member updates to DB at once."""
    while True:
        time.sleep(300)  # Wait 5 minutes
        
        if public_groups_col is None or group_members_col is None:
            continue

        with batch_lock:
            if not pending_group_updates and not pending_member_updates:
                continue
                
            groups_to_write = pending_group_updates.copy()
            members_to_write = pending_member_updates.copy()
            pending_group_updates.clear()
            pending_member_updates.clear()

        # Batch write groups
        if groups_to_write:
            group_ops = [
                UpdateOne(
                    {"chat_id": chat_id},
                    {"$set": {"chat_id": chat_id, **data}},
                    upsert=True
                ) for chat_id, data in groups_to_write.items()
            ]
            try:
                public_groups_col.bulk_write(group_ops, ordered=False)
            except Exception as e:
                print(f"Group batch write error: {e}")

        # Batch write members
        if members_to_write:
            member_ops = [
                UpdateOne(
                    {"chat_id": data["chat_id"], "user_id": data["user_id"]},
                    {"$set": data},
                    upsert=True
                ) for _, data in members_to_write.items()
            ]
            try:
                group_members_col.bulk_write(member_ops, ordered=False)
            except Exception as e:
                print(f"Member batch write error: {e}")

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
    try:
        pipeline = [
            {"$match": {"chat_id": chat_id, "keyword": keyword.strip().lower()}},
            {"$sample": {"size": 1}}
        ]
        matches = list(media_col.aggregate(pipeline))
        return matches[0]["message_id"] if matches else None
    except Exception as e:
        print(f"Database error in get_random_media: {e}")
        return None

def get_random_meme():
    if memes_col is None:
        return None
    try:
        matches = list(memes_col.aggregate([{"$sample": {"size": 1}}]))
        return matches[0]["message_id"] if matches else None
    except Exception as e:
        print(f"Database error in get_random_meme: {e}")
        return None

def remove_dead_media(chat_id, msg_id):
    if media_col is not None:
        media_col.delete_one({"chat_id": chat_id, "message_id": msg_id})
    if flirt_media_col is not None:
        flirt_media_col.delete_one({"message_id": msg_id})
    if memes_col is not None and chat_id == MM_MEMES_CHAT_ID:
        memes_col.delete_one({"message_id": msg_id})
    print(f"Purged deleted media ID {msg_id} from group {chat_id}")

# ---------------- QUEUE WORKER ----------------
def process_queue():
    while True:
        try:
            job = reaction_queue.get()
            job_type = job.get("type")
            queued_time = job.get("queued_time", time.time())
            target_chat_id = job.get("target_chat_id")

            if time.time() - queued_time > 45:
                reaction_queue.task_done()
                continue

            if is_group_rate_limited(target_chat_id):
                reaction_queue.task_done()
                continue
                
            storage_chat_id = job["storage_chat_id"]
            media_msg_id = job["media_msg_id"]

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
                    if "kicked" in err_msg or "chat not found" in err_msg or "deactivated" in err_msg:
                        if public_groups_col is not None:
                            public_groups_col.delete_one({"chat_id": target_chat_id})
                            print(f"Auto-Cleanup: Removed dead group {target_chat_id}")
                    elif "replied message not found" in err_msg or "reply" in err_msg:
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
                    if "kicked" in err_msg or "chat not found" in err_msg or "deactivated" in err_msg:
                        if public_groups_col is not None:
                            public_groups_col.delete_one({"chat_id": target_chat_id})
                    elif "message to copy not found" in err_msg or "message can't be copied" in err_msg:
                        remove_dead_media(storage_chat_id, media_msg_id)
                except Exception as e:
                    print(f"Flirt transfer error: {e}")

            reaction_queue.task_done()
            time.sleep(0.2) 

        except Exception as e:
            print(f"Queue worker exception: {e}")
            time.sleep(1)

# ---------------- DYNAMIC DAILY FLIRT ENGINE ----------------
def generate_daily_schedule():
    m1 = random.randint(510, 595)
    m2 = random.randint(605, 690)
    a1 = random.randint(810, 930)
    e1 = random.randint(1110, 1195)
    e2 = random.randint(1205, 1290)
    return sorted([m1, m2, a1, e1, e2])

def dispatch_flirts_for_slot(slot_index):
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

        if has_female_media and MMG_FLIRT_CHAT_ID != 0:
            total_girls = group_members_col.count_documents({"chat_id": group_id, "gender": "f"})
            active_slots = min(total_girls, 5)
            if slot_index < active_slots:
                used_today = flirt_logs_col.distinct("user_id", {"date": today_str})
                cursor = group_members_col.aggregate([
                    {"$match": {"chat_id": group_id, "gender": "f", "user_id": {"$nin": used_today}}},
                    {"$sample": {"size": 1}}
                ])
                eligible_girls = list(cursor)
                if eligible_girls:
                    chosen_girl = eligible_girls[0]
                    random_f_media = list(flirt_media_col.aggregate([{"$match": {"gender": "f"}}, {"$sample": {"size": 1}}]))
                    media_id = random_f_media[0]["message_id"]

                    flirt_logs_col.insert_one({
                        "date": today_str, "chat_id": group_id,
                        "user_id": chosen_girl["user_id"], "gender": "f",
                        "slot": slot_index, "timestamp": time.time()
                    })
                    try:
                        reaction_queue.put_nowait({
                            "type": "flirt", "target_chat_id": group_id,
                            "storage_chat_id": MMG_FLIRT_CHAT_ID, "media_msg_id": media_id,
                            "user_id": chosen_girl["user_id"], "first_name": chosen_girl.get("first_name", "Friend"),
                            "queued_time": time.time()
                        })
                    except queue.Full:
                        pass

        if has_male_media and MMB_FLIRT_CHAT_ID != 0:
            total_boys = group_members_col.count_documents({"chat_id": group_id, "gender": "m"})
            active_slots = min(total_boys, 5)
            if slot_index < active_slots:
                used_today = flirt_logs_col.distinct("user_id", {"date": today_str})
                cursor = group_members_col.aggregate([
                    {"$match": {"chat_id": group_id, "gender": "m", "user_id": {"$nin": used_today}}},
                    {"$sample": {"size": 1}}
                ])
                eligible_boys = list(cursor)
                if eligible_boys:
                    chosen_boy = eligible_boys[0]
                    random_m_media = list(flirt_media_col.aggregate([{"$match": {"gender": "m"}}, {"$sample": {"size": 1}}]))
                    media_id = random_m_media[0]["message_id"]

                    flirt_logs_col.insert_one({
                        "date": today_str, "chat_id": group_id,
                        "user_id": chosen_boy["user_id"], "gender": "m",
                        "slot": slot_index, "timestamp": time.time()
                    })
                    try:
                        reaction_queue.put_nowait({
                            "type": "flirt", "target_chat_id": group_id,
                            "storage_chat_id": MMB_FLIRT_CHAT_ID, "media_msg_id": media_id,
                            "user_id": chosen_boy["user_id"], "first_name": chosen_boy.get("first_name", "Friend"),
                            "queued_time": time.time()
                        })
                    except queue.Full:
                        pass

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
                    if memes_col is not None and public_groups_col is not None and MM_MEMES_CHAT_ID != 0 and memes_col.count_documents({}) > 0:
                        random_meme_cursor = list(memes_col.aggregate([{"$sample": {"size": 1}}]))
                        public_groups = list(public_groups_col.find())

                        if random_meme_cursor and public_groups:
                            selected_meme = random_meme_cursor[0]["message_id"]
                            for group in public_groups:
                                group_id = group["chat_id"]
                                group_title = group.get("title", "")

                                if is_storage_group(group_id, group_title) or not is_bot_admin(group_id):
                                    continue

                                try:
                                    sent = bot.copy_message(chat_id=group_id, from_chat_id=MM_MEMES_CHAT_ID, message_id=selected_meme)
                                    bot.pin_chat_message(chat_id=group_id, message_id=sent.message_id, disable_notification=False)
                                    time.sleep(1)
                                except ApiTelegramException as e:
                                    err_msg = str(e).lower()
                                    if "kicked" in err_msg or "not found" in err_msg:
                                        public_groups_col.delete_one({"chat_id": group_id})
                                    elif "message can't be copied" in err_msg:
                                        memes_col.delete_one({"message_id": selected_meme})
                                        break
                                except Exception:
                                    pass

                    pinned_today_date = today_str
                    target_hour = random.randint(7, 10)
                    target_minute = random.randint(0, 59)

        except Exception as e:
            print(f"Meme scheduler error: {e}")

        seconds_to_sleep = 60 - datetime.now(tz_ist).second
        time.sleep(seconds_to_sleep)

# ---------------- DEPLOYMENT NOTIFICATION TASK ----------------
def notify_deployment():
    time.sleep(3) 
    storage_targets = [
        ("MMB", MMB_CHAT_ID, "🤖 *MMB Vault is Online!*\n\n*Purpose:* Storage vault for male keyword reaction media.\n\n*How it works & how to use it:*\n• Create a forum topic titled with your trigger keyword (e.g., `sad`, `cool`, `bye`).\n• Send media (photos, GIFs, videos, stickers) inside that topic.\n• When a registered male member says that keyword in an active public group, the bot pulls random media from here to reply."),
        ("MMG", MMG_CHAT_ID, "🤖 *MMG Vault is Online!*\n\n*Purpose:* Storage vault for female keyword reaction media.\n\n*How it works & how to use it:*\n• Create a forum topic titled with your trigger keyword (e.g., `happy`, `angry`, `hello`).\n• Send media (photos, GIFs, videos, stickers) inside that topic.\n• When a registered female member says that keyword in an active public group, the bot pulls random media from here to reply."),
        ("MMB Flirt", MMB_FLIRT_CHAT_ID, "🤖 *MMB Flirt Vault is Online!*\n\n*Purpose:* Media vault for flirts targeted at male members.\n\n*How it works & how to use it:*\n• Upload photos, animations, or videos directly into this group.\n• The bot automatically indexes every item under the male flirt catalog.\n• During daily scheduled flirt slots, the bot posts a media item from here into public groups and tags an eligible male user in the reply."),
        ("MMG Flirt", MMG_FLIRT_CHAT_ID, "🤖 *MMG Flirt Vault is Online!*\n\n*Purpose:* Media vault for flirts targeted at female members.\n\n*How it works & how to use it:*\n• Upload photos, animations, or videos directly into this group.\n• The bot automatically indexes every item under the female flirt catalog.\n• During daily scheduled flirt slots, the bot posts a media item from here into public groups and tags an eligible female user in the reply."),
        ("MM Memes", MM_MEMES_CHAT_ID, "🤖 *MM Memes Vault is Online!*\n\n*Purpose:* Central storage vault for general memes.\n\n*How it works & how to use it:*\n• Upload photos, videos, or animations here to add them to the meme repository.\n• *On-Demand:* When a user types `/meme` in a public group, the bot copies a meme from here as a reply (limit 5 per day per group).\n• *Daily Morning Pin:* Every morning between 7:00 AM and 11:00 AM IST, the bot randomly selects a meme from here and pins it across all public groups where it is an admin.")
    ]
    for name, chat_id, message_text in storage_targets:
        if chat_id != 0:
            try:
                bot.send_message(chat_id, message_text, parse_mode="Markdown")
                time.sleep(1) 
            except Exception:
                pass

# ---------------- BOT HANDLERS ----------------
@bot.message_handler(commands=['getid'])
def send_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")

@bot.message_handler(commands=['stats'], func=lambda m: m.chat.type == "private")
def bot_stats(message):
    if not BOT_ID or message.from_user.id != BOT_ID:
        return

    stats_msg = "*🤖 Bot Live Server Stats*\n\n"
    stats_msg += f"• **RAM: Gender Cache:** {len(gender_cache)} users\n"
    stats_msg += f"• **RAM: Spam Cooldowns:** {len(spam_cooldowns)}\n"
    stats_msg += f"• **RAM: Rate Limits:** {len(group_rate_limits)} groups\n"
    stats_msg += f"• **RAM: Pending Batch DB Writes:** {len(pending_member_updates)}\n"
    stats_msg += f"• **Queue Backlog:** {reaction_queue.qsize()} jobs waiting\n\n"
    
    try:
        total_groups = public_groups_col.count_documents({}) if public_groups_col else 0
        total_users = users_col.count_documents({}) if users_col else 0
        total_memes = memes_col.count_documents({}) if memes_col else 0
        total_f_flirts = flirt_media_col.count_documents({"gender": "f"}) if flirt_media_col else 0
        total_m_flirts = flirt_media_col.count_documents({"gender": "m"}) if flirt_media_col else 0
        
        stats_msg += f"• **DB: Tracked Groups:** {total_groups}\n"
        stats_msg += f"• **DB: Registered Users:** {total_users}\n"
        stats_msg += f"• **DB: Total Memes:** {total_memes}\n"
        stats_msg += f"• **DB: Girls Flirts:** {total_f_flirts}\n"
        stats_msg += f"• **DB: Boys Flirts:** {total_m_flirts}\n"
    except Exception:
        stats_msg += "\n_(Database currently unreachable)_"

    bot.reply_to(message, stats_msg, parse_mode="Markdown")

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
        
        prune_cache(gender_cache, 5000, 1000)
        gender_cache[call.from_user.id] = gender
        
        bot.edit_message_text(f"Saved! You are registered as {'Male (m)' if gender == 'm' else 'Female (f)'}.", chat_id=call.message.chat.id, message_id=call.message.message_id)
    else:
        bot.edit_message_text("Database connection error. Please try again later.", chat_id=call.message.chat.id, message_id=call.message.message_id)

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

@bot.message_handler(content_types=['forum_topic_created'], func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def on_topic_created(message):
    name = message.forum_topic_created.name.strip().lower()
    save_topic(message.chat.id, message.message_thread_id, name)
    bot.reply_to(message, f"Topic auto-linked to keyword: '{name}'")

@bot.message_handler(content_types=['forum_topic_edited'], func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def on_topic_edited(message):
    if message.forum_topic_edited.name:
        new_name = message.forum_topic_edited.name.strip().lower()
        update_topic_keyword(message.chat.id, message.message_thread_id, new_name)
        bot.reply_to(message, f"Topic updated to keyword: '{new_name}'")

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

@bot.message_handler(content_types=['photo', 'animation', 'video', 'document'], func=lambda m: m.chat.id == MM_MEMES_CHAT_ID and m.chat.id != 0)
def index_memes(message):
    if memes_col is not None:
        memes_col.update_one(
            {"message_id": message.message_id},
            {"$set": {"message_id": message.message_id}},
            upsert=True
        )

# ---------------- PUBLIC GROUP LISTENER ----------------
@bot.message_handler(content_types=['text', 'photo', 'animation', 'video', 'document', 'sticker'],
                     func=lambda m: m.chat.type in ['group', 'supergroup'] and not is_storage_group(m.chat.id, m.chat.title))
def handle_public_group(message):
    if message.content_type != 'text' or message.reply_to_message is not None or not message.from_user:
        return

    text_clean = message.text.strip().lower()

    if text_clean == '/meme' or text_clean.startswith('/meme@'):
        current_time = time.time()
        if current_time - group_command_cooldowns.get(message.chat.id, 0) < 5:
            return
        group_command_cooldowns[message.chat.id] = current_time
        prune_cache(group_command_cooldowns, 2000, 500)

        track_activity(message.chat, message.from_user)
        tz_ist = timezone(timedelta(hours=5, minutes=30))
        today_str = datetime.now(tz_ist).strftime("%Y-%m-%d")

        if meme_logs_col is not None and MM_MEMES_CHAT_ID != 0:
            memes_sent_today = meme_logs_col.count_documents({"date": today_str, "chat_id": message.chat.id})
            if memes_sent_today < 5:
                selected_meme_id = get_random_meme()
                if selected_meme_id:
                    meme_logs_col.insert_one({
                        "date": today_str, "chat_id": message.chat.id,
                        "user_id": message.from_user.id, "message_id": selected_meme_id,
                        "timestamp": time.time()
                    })
                    try:
                        reaction_queue.put_nowait({
                            "type": "reaction", "target_chat_id": message.chat.id,
                            "storage_chat_id": MM_MEMES_CHAT_ID, "media_msg_id": selected_meme_id,
                            "reply_to_id": message.message_id, "queued_time": time.time()
                        })
                    except queue.Full:
                        pass
        return

    if text_clean == '/flirt' or text_clean.startswith('/flirt@'):
        current_time = time.time()
        if current_time - group_command_cooldowns.get(message.chat.id, 0) < 5:
            return
        group_command_cooldowns[message.chat.id] = current_time
        prune_cache(group_command_cooldowns, 2000, 500)

        track_activity(message.chat, message.from_user)
        tz_ist = timezone(timedelta(hours=5, minutes=30))
        today_str = datetime.now(tz_ist).strftime("%Y-%m-%d")

        if flirt_manual_logs_col is not None:
            flirts_sent_today = flirt_manual_logs_col.count_documents({"date": today_str, "chat_id": message.chat.id})
            if flirts_sent_today < 5:
                cursor = group_members_col.aggregate([
                    {"$match": {"chat_id": message.chat.id, "gender": {"$in": ["m", "f"]}}},
                    {"$sample": {"size": 1}}
                ])
                members = list(cursor)
                
                if members:
                    chosen_user = members[0]
                    gender = chosen_user["gender"]
                    
                    has_media = flirt_media_col.count_documents({"gender": gender}) > 0
                    if has_media:
                        random_media = list(flirt_media_col.aggregate([
                            {"$match": {"gender": gender}},
                            {"$sample": {"size": 1}}
                        ]))
                        media_id = random_media[0]["message_id"]
                        storage_chat_id = MMB_FLIRT_CHAT_ID if gender == "m" else MMG_FLIRT_CHAT_ID
                        
                        flirt_manual_logs_col.insert_one({
                            "date": today_str, "chat_id": message.chat.id,
                            "user_id": chosen_user["user_id"], "timestamp": time.time()
                        })
                        
                        try:
                            reaction_queue.put_nowait({
                                "type": "flirt", "target_chat_id": message.chat.id,
                                "storage_chat_id": storage_chat_id, "media_msg_id": media_id,
                                "user_id": chosen_user["user_id"], "first_name": chosen_user.get("first_name", "Friend"),
                                "queued_time": time.time()
                            })
                        except queue.Full:
                            pass
        return

    gender = get_user_gender(message.from_user.id)
    if not gender:
        return

    current_time = time.time()
    last_reaction_time = spam_cooldowns.get(message.from_user.id, 0)
    
    if current_time - last_reaction_time < SPAM_LIMIT_SECONDS:
        return 

    trigger = text_clean
    storage_chat_id = MMB_CHAT_ID if gender == "m" else MMG_CHAT_ID
    
    selected_msg_id = get_random_media(storage_chat_id, trigger)

    if selected_msg_id:
        track_activity(message.chat, message.from_user)
        
        spam_cooldowns[message.from_user.id] = current_time
        prune_cache(spam_cooldowns, 5000, 1000)

        try:
            reaction_queue.put_nowait({
                "type": "reaction", "target_chat_id": message.chat.id,
                "storage_chat_id": storage_chat_id, "media_msg_id": selected_msg_id,
                "reply_to_id": message.message_id, "queued_time": time.time()
            })
        except queue.Full:
            pass


# ---------------- WEBHOOK ROUTES & STARTUP ----------------
@app.route('/')
def home():
    return "Bot running 24/7 with Multi-Group Reactions and Webhooks!", 200

@app.route(f'/{TOKEN}', methods=['POST'])
def webhook():
    """Receives updates from Telegram directly (Zero CPU Idle Mode)."""
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    else:
        abort(403)

def run_web():
    port = int(os.environ.get("PORT", 8080))
    if WEBHOOK_URL:
        # WEBHOOK MODE: Perfect for Render
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=f"{WEBHOOK_URL}/{TOKEN}")
        print(f"Webhook securely established at {WEBHOOK_URL}/{TOKEN}")
        serve(app, host="0.0.0.0", port=port)
    else:
        # POLLING MODE: Fallback for local PC testing
        bot.remove_webhook()
        print("No WEBHOOK_URL detected. Falling back to infinity_polling...")
        threading.Thread(target=lambda: serve(app, host="0.0.0.0", port=port), daemon=True).start()
        bot.infinity_polling(skip_pending=True, allowed_updates=['message', 'callback_query'])

if __name__ == "__main__":
    # Start all background workers as daemons
    threading.Thread(target=init_db, daemon=True).start()
    threading.Thread(target=process_queue, daemon=True).start()
    threading.Thread(target=flirt_scheduler_loop, daemon=True).start()
    threading.Thread(target=daily_meme_pinner, daemon=True).start()
    threading.Thread(target=notify_deployment, daemon=True).start()
    threading.Thread(target=db_batch_writer_loop, daemon=True).start()
    
    # Start the web server (blocks the main thread to keep script alive)
    run_web()
