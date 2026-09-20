import os
import random
import sqlite3
import threading
from flask import Flask
from pymongo import MongoClient
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

# ---------------- CONFIGURATION ----------------
TOKEN = os.environ.get("BOT_TOKEN")
STORAGE_CHAT_ID = int(os.environ.get("STORAGE_CHAT_ID", 0))
MONGO_URI = os.environ.get("MONGO_URI")

bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# ---------------- MONGODB SETUP ----------------
mongo_client = None
users_col = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client["telegram_bot"]
        users_col = mongo_db["users"]
        print("Connected to MongoDB successfully.")
    except Exception as e:
        print(f"MongoDB Connection Error: {e}")

# ---------------- SQLITE FOR MEDIA ----------------
def get_db():
    return sqlite3.connect("bot_storage.db")

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS topics (
                thread_id INTEGER PRIMARY KEY,
                keyword TEXT UNIQUE
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT,
                message_id INTEGER
            )
        """)

init_db()

def save_topic(thread_id, keyword):
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO topics (thread_id, keyword) VALUES (?, ?)", 
                     (thread_id, keyword.strip().lower()))

def update_topic_keyword(thread_id, new_keyword):
    new_keyword = new_keyword.strip().lower()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT keyword FROM topics WHERE thread_id = ?", (thread_id,))
        row = cur.fetchone()
        if row:
            old_keyword = row[0]
            conn.execute("UPDATE topics SET keyword = ? WHERE thread_id = ?", (new_keyword, thread_id))
            conn.execute("UPDATE media SET keyword = ? WHERE keyword = ?", (new_keyword, old_keyword))
        else:
            conn.execute("INSERT OR REPLACE INTO topics (thread_id, keyword) VALUES (?, ?)", (thread_id, new_keyword))

def get_keyword(thread_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT keyword FROM topics WHERE thread_id = ?", (thread_id,))
        row = cur.fetchone()
        return row[0] if row else None

def save_media(keyword, message_id):
    with get_db() as conn:
        conn.execute("INSERT INTO media (keyword, message_id) VALUES (?, ?)", 
                     (keyword.strip().lower(), message_id))

def get_random_media(keyword):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT message_id FROM media WHERE keyword = ?", (keyword.strip().lower(),))
        rows = cur.fetchall()
        return random.choice(rows)[0] if rows else None

def remove_dead_media(msg_id, keyword):
    with get_db() as conn:
        conn.execute("DELETE FROM media WHERE message_id = ?", (msg_id,))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM media WHERE keyword = ?", (keyword,))
        if cur.fetchone()[0] == 0:
            conn.execute("DELETE FROM topics WHERE keyword = ?", (keyword,))

# ---------------- KEEP-ALIVE SERVER ----------------
@app.route('/')
def home():
    return "Bot is running 24/7!", 200

def run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ---------------- START COMMAND & GENDER SELECTION ----------------

def get_gender_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    btn_male = types.InlineKeyboardButton("👨 Male", callback_data="select_m")
    btn_female = types.InlineKeyboardButton("👩 Female", callback_data="select_f")
    keyboard.add(btn_male, btn_female)
    return keyboard

@bot.message_handler(commands=['start'])
def handle_start(message):
    if message.chat.type != "private":
        bot.reply_to(message, "Please use /start in a private chat with me.")
        return

    text = "Welcome! Please select your gender to continue:"
    bot.send_message(message.chat.id, text, reply_markup=get_gender_keyboard())

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_"))
def handle_gender_selection(call):
    selected = call.data.split("_")[1] # 'm' or 'f'
    gender_name = "Male (m)" if selected == "m" else "Female (f)"

    keyboard = types.InlineKeyboardMarkup(row_width=2)
    confirm_btn = types.InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_{selected}")
    change_btn = types.InlineKeyboardButton("🔄 Change", callback_data="change_gender")
    keyboard.add(confirm_btn, change_btn)

    text = f"You selected: {gender_name}\n\nClick 'Confirm' to save your details."
    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data == "change_gender")
def handle_change_gender(call):
    text = "Please select your gender:"
    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=get_gender_keyboard())

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_"))
def handle_confirmation(call):
    gender = call.data.split("_")[1] # 'm' or 'f'
    user_id = call.from_user.id
    gender_full = "Male" if gender == "m" else "Female"

    if users_col is not None:
        try:
            users_col.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "gender": gender
                    }
                },
                upsert=True
            )
            text = f"Registration successful!\n\nUser ID: {user_id}\nGender: {gender_full} ({gender})\n\nYour details are saved in the database."
        except Exception as e:
            text = "An error occurred while saving your data. Please try again later."
            print(f"MongoDB insert error: {e}")
    else:
        text = "Database connection is not configured yet. Please contact the administrator."

    bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id)

# ---------------- STORAGE & TOPIC HANDLERS ----------------

@bot.message_handler(commands=['getid'])
def send_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")

@bot.message_handler(content_types=['forum_topic_created'], func=lambda m: m.chat.id == STORAGE_CHAT_ID)
def on_topic_created(message):
    name = message.forum_topic_created.name.strip().lower()
    thread_id = message.message_thread_id
    save_topic(thread_id, name)
    bot.reply_to(message, f"Topic auto-linked to keyword: '{name}'")

@bot.message_handler(content_types=['forum_topic_edited'], func=lambda m: m.chat.id == STORAGE_CHAT_ID)
def on_topic_edited(message):
    thread_id = message.message_thread_id
    if message.forum_topic_edited.name:
        new_name = message.forum_topic_edited.name.strip().lower()
        update_topic_keyword(thread_id, new_name)
        bot.reply_to(message, f"Topic updated to keyword: '{new_name}'")

@bot.message_handler(commands=['setword'], func=lambda m: m.chat.id == STORAGE_CHAT_ID)
def manual_set_word(message):
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1 and message.message_thread_id:
        word = parts[1].strip().lower()
        save_topic(message.message_thread_id, word)
        bot.reply_to(message, f"Linked this topic to word: '{word}'")
    else:
        bot.reply_to(message, "Usage: Send '/setword <word>' inside the target topic.")

@bot.message_handler(commands=['delword'], func=lambda m: m.chat.id == STORAGE_CHAT_ID)
def manual_del_word(message):
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        word = parts[1].strip().lower()
        with get_db() as conn:
            conn.execute("DELETE FROM topics WHERE keyword = ?", (word,))
            conn.execute("DELETE FROM media WHERE keyword = ?", (word,))
        bot.reply_to(message, f"Deleted keyword and all saved media for: '{word}'")
    else:
        bot.reply_to(message, "Usage: Send '/delword <word>' to delete it.")

@bot.message_handler(content_types=['text', 'photo', 'animation', 'document', 'video', 'sticker'], 
                     func=lambda m: m.chat.id == STORAGE_CHAT_ID)
def index_media(message):
    if message.text and message.text.startswith('/'):
        return
    thread_id = message.message_thread_id
    if thread_id:
        keyword = get_keyword(thread_id)
        if keyword:
            save_media(keyword, message.message_id)

@bot.message_handler(content_types=['text'], func=lambda m: m.chat.id != STORAGE_CHAT_ID)
def handle_group_trigger(message):
    if message.reply_to_message is not None:
        return

    trigger = message.text.strip().lower()
    selected_msg_id = get_random_media(trigger)

    if selected_msg_id:
        try:
            bot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=STORAGE_CHAT_ID,
                message_id=selected_msg_id,
                reply_to_message_id=message.message_id
            )
        except ApiTelegramException as e:
            error_text = str(e).lower()
            if "message to copy not found" in error_text or "not found" in error_text:
                remove_dead_media(selected_msg_id, trigger)

# ---------------- START ----------------
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    bot.infinity_polling()
