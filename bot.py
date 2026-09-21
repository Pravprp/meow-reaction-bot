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
MMB_APPROVED_CHAT_ID = int(os.environ.get("MMB_APPROVED_CHAT_ID") or 0)
MMG_APPROVED_CHAT_ID = int(os.environ.get("MMG_APPROVED_CHAT_ID") or 0)
MM_MEMES_CHAT_ID = int(os.environ.get("MM_MEMES_CHAT_ID") or 0)

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# Queue for outgoing reactions (1 job at a time, 2-second cooldown)
reaction_queue = queue.Queue(maxsize=1000)

# ---------------- MONGODB SETUP ----------------
mongo_client = None
db = None
users_col = None
approved_col = None
topics_col = None
media_col = None
memes_col = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["telegram_bot"]

        users_col = db["users"]                  # Manual registrations (/start)
        approved_col = db["approved_users"]      # Overrides from Approved groups
        topics_col = db["topics"]                # Forum topic keywords
        media_col = db["media"]                  # Media pointers for MMB and MMG
        memes_col = db["memes"]                  # Meme IDs for MM Memes

        # Ensure indexes for rapid lookups
        topics_col.create_index([("chat_id", 1), ("thread_id", 1)], unique=True)
        approved_col.create_index("user_id", unique=True)
        users_col.create_index("user_id", unique=True)
        print("Connected to MongoDB successfully.")
    except Exception as e:
        print(f"MongoDB Initialization Error: {e}")

# ---------------- DATABASE HELPERS ----------------

def get_effective_gender(user_id):
    """
    Checks approved groups first, then manual database registrations.
    Returns 'm', 'f', or None if unverified.
    """
    if approved_col is None or users_col is None:
        return None

    approved_entry = approved_col.find_one({"user_id": user_id})
    if approved_entry:
        return approved_entry.get("gender")

    registered_entry = users_col.find_one({"user_id": user_id})
    if registered_entry:
        return registered_entry.get("gender")

    return None

def save_topic(chat_id, thread_id, keyword):
    if topics_col is None:
        return
    keyword_clean = keyword.strip().lower()
    topics_col.update_one(
        {"chat_id": chat_id, "thread_id": thread_id},
        {"$set": {"keyword": keyword_clean}},
        upsert=True
    )

def update_topic_keyword(chat_id, thread_id, new_keyword):
    if topics_col is None or media_col is None:
        return
    new_keyword_clean = new_keyword.strip().lower()
    old_topic = topics_col.find_one({"chat_id": chat_id, "thread_id": thread_id})
    if old_topic:
        old_keyword = old_topic.get("keyword")
        topics_col.update_one({"chat_id": chat_id, "thread_id": thread_id}, {"$set": {"keyword": new_keyword_clean}})
        media_col.update_many({"chat_id": chat_id, "keyword": old_keyword}, {"$set": {"keyword": new_keyword_clean}})
    else:
        save_topic(chat_id, thread_id, new_keyword_clean)

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
    if matches:
        return random.choice(matches)["message_id"]
    return None

def remove_dead_media(chat_id, msg_id):
    """Removes deleted media message pointer from MongoDB without wiping the topic mapping."""
    if media_col is not None:
        media_col.delete_one({"chat_id": chat_id, "message_id": msg_id})
        print(f"Purged deleted media ID {msg_id} from group {chat_id}")

# ---------------- QUEUE WORKER (ONE BY ONE, 2s GAP) ----------------

def process_queue():
    while True:
        try:
            job = reaction_queue.get()
            target_chat_id, storage_chat_id, media_msg_id, reply_to_id, keyword, queued_time = job

            # Only process if queued within the last 45 seconds
            if time.time() - queued_time <= 45:
                try:
                    bot.copy_message(
                        chat_id=target_chat_id,
                        from_chat_id=storage_chat_id,
                        message_id=media_msg_id,
                        reply_to_message_id=reply_to_id
                    )
                except ApiTelegramException as e:
                    err_msg = str(e).lower()

                    # Case A: The user deleted their message in the group before bot replied
                    if "replied message not found" in err_msg or "reply" in err_msg:
                        try:
                            # Send without replying to the deleted message
                            bot.copy_message(
                                chat_id=target_chat_id,
                                from_chat_id=storage_chat_id,
                                message_id=media_msg_id
                            )
                        except Exception:
                            pass

                    # Case B: The original storage media was deleted in MMB or MMG
                    elif "message to copy not found" in err_msg or "message can't be copied" in err_msg:
                        remove_dead_media(storage_chat_id, media_msg_id)

                except Exception as e:
                    print(f"Error copying media: {e}")

            reaction_queue.task_done()
            time.sleep(2)  # 2-second cooldown

        except Exception as e:
            print(f"Queue worker exception: {e}")
            time.sleep(1)

# ---------------- DAILY RANDOM MORNING MEME PINNER ----------------

def daily_meme_pinner():
    """Selects and pins a random meme once every morning at a randomized time."""
    tz_ist = timezone(timedelta(hours=5, minutes=30))
    pinned_today_date = None
    target_hour = random.randint(7, 10)
    target_minute = random.randint(0, 59)

    while True:
        try:
            now = datetime.now(tz_ist)
            today_str = now.strftime("%Y-%m-%d")

            current_total_minutes = now.hour * 60 + now.minute
            target_total_minutes = target_hour * 60 + target_minute

            # Fire once per calendar day when current time reaches or exceeds the target time
            if pinned_today_date != today_str:
                if current_total_minutes >= target_total_minutes:
                    if memes_col is not None and MM_MEMES_CHAT_ID != 0:
                        memes = list(memes_col.find())
                        if memes:
                            selected_meme = random.choice(memes)["message_id"]
                            try:
                                sent = bot.copy_message(
                                    chat_id=MM_MEMES_CHAT_ID,
                                    from_chat_id=MM_MEMES_CHAT_ID,
                                    message_id=selected_meme
                                )
                                bot.pin_chat_message(
                                    chat_id=MM_MEMES_CHAT_ID,
                                    message_id=sent.message_id,
                                    disable_notification=False
                                )
                                pinned_today_date = today_str

                                # Generate new target morning time for tomorrow
                                target_hour = random.randint(7, 10)
                                target_minute = random.randint(0, 59)
                                print(f"Daily meme pinned successfully on {today_str}.")
                            except ApiTelegramException as e:
                                err_msg = str(e).lower()
                                if "message to copy not found" in err_msg or "message can't be copied" in err_msg:
                                    memes_col.delete_one({"message_id": selected_meme})
                            except Exception as e:
                                print(f"Failed to pin daily meme: {e}")
        except Exception as e:
            print(f"Meme scheduler error: {e}")

        time.sleep(60)

# ---------------- KEEP-ALIVE SERVER ----------------
@app.route('/')
def home():
    return "Bot running with Multi-Group Gender Routing!", 200

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ---------------- BOT HANDLERS ----------------

@bot.message_handler(commands=['getid'])
def send_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")

# 1. Ingestion: "MM B Approved" and "MM G Approved" groups
@bot.message_handler(content_types=['text'], func=lambda m: m.chat.id in [MMB_APPROVED_CHAT_ID, MMG_APPROVED_CHAT_ID] and m.chat.id != 0)
def handle_approved_ids(message):
    if approved_col is None:
        return

    gender = "m" if message.chat.id == MMB_APPROVED_CHAT_ID else "f"
    lines = message.text.strip().splitlines()
    added_count = 0

    for line in lines:
        cleaned = line.strip().replace("@", "")
        if cleaned.isdigit():
            uid = int(cleaned)
            approved_col.update_one(
                {"user_id": uid},
                {"$set": {"user_id": uid, "gender": gender}},
                upsert=True
            )
            added_count += 1

    if added_count > 0:
        bot.reply_to(message, f"Registered {added_count} user(s) as {'Boy (m)' if gender == 'm' else 'Girl (f)'}.")

# 2. Ingestion: "MM Memes" media storage
@bot.message_handler(content_types=['photo', 'animation', 'video', 'document'], func=lambda m: m.chat.id == MM_MEMES_CHAT_ID and m.chat.id != 0)
def index_memes(message):
    if memes_col is not None:
        memes_col.update_one(
            {"message_id": message.message_id},
            {"$set": {"message_id": message.message_id}},
            upsert=True
        )

# 3. Topic Creation in MMB or MMG
@bot.message_handler(content_types=['forum_topic_created'], func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def on_topic_created(message):
    name = message.forum_topic_created.name.strip().lower()
    save_topic(message.chat.id, message.message_thread_id, name)
    bot.reply_to(message, f"Topic auto-linked to keyword: '{name}'")

# 4. Topic Renamed/Edited in MMB or MMG
@bot.message_handler(content_types=['forum_topic_edited'], func=lambda m: m.chat.id in [MMB_CHAT_ID, MMG_CHAT_ID] and m.chat.id != 0)
def on_topic_edited(message):
    if message.forum_topic_edited.name:
        new_name = message.forum_topic_edited.name.strip().lower()
        update_topic_keyword(message.chat.id, message.message_thread_id, new_name)
        bot.reply_to(message, f"Topic updated to keyword: '{new_name}'")

# 5. Media Uploads inside MMB or MMG topics
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

# 6. User Verification & Gender-Based Reaction Dispatcher
@bot.message_handler(content_types=['text'], func=lambda m: m.chat.id not in [
    MMB_CHAT_ID, MMG_CHAT_ID, MMB_APPROVED_CHAT_ID, MMG_APPROVED_CHAT_ID, MM_MEMES_CHAT_ID
])
def handle_group_trigger(message):
    # Ignore replies
    if message.reply_to_message is not None:
        return

    # Guard against anonymous group admins or channel posts
    if not message.from_user:
        return

    user_id = message.from_user.id
    gender = get_effective_gender(user_id)

    # If user has no verified gender in approved groups or DB, DO NOT RESPOND
    if not gender:
        return

    trigger = message.text.strip().lower()
    storage_chat_id = MMB_CHAT_ID if gender == "m" else MMG_CHAT_ID

    selected_msg_id = get_random_media(storage_chat_id, trigger)

    if selected_msg_id:
        try:
            reaction_queue.put_nowait(
                (message.chat.id, storage_chat_id, selected_msg_id, message.message_id, trigger, time.time())
            )
        except queue.Full:
            pass

# 7. Start Registration Fallback (For 1-on-1 Chats)
def get_gender_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    btn_male = types.InlineKeyboardButton("Male", callback_data="select_m")
    btn_female = types.InlineKeyboardButton("Female", callback_data="select_f")
    keyboard.add(btn_male, btn_female)
    return keyboard

@bot.message_handler(commands=['start'])
def handle_start(message):
    if message.chat.type != "private":
        return
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
        bot.edit_message_text(f"Saved! You are registered as {'Male (m)' if gender == 'm' else 'Female (f)'}.", chat_id=call.message.chat.id, message_id=call.message.message_id)
    else:
        bot.edit_message_text("Database connection error. Please try again later.", chat_id=call.message.chat.id, message_id=call.message.message_id)

# ---------------- START SERVICES ----------------
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    threading.Thread(target=process_queue, daemon=True).start()
    threading.Thread(target=daily_meme_pinner, daemon=True).start()
    bot.infinity_polling()
