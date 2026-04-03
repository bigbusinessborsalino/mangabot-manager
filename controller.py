import asyncio
import sys

# --- CRITICAL FIX FOR PYTHON 3.14+ (MUST BE AT THE VERY TOP) ---
# We have to create the event loop BEFORE importing Pyrogram, or it crashes on Render.
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import logging
import os
import uuid
import aiohttp
from datetime import datetime, timedelta, timezone
from pyrogram import Client, filters
import motor.motor_asyncio
from aiohttp import web
from mangaplus import MangaPlus
from mangaplus.constants import Language, Viewer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
TARGET_GROUP = int(os.getenv("TARGET_GROUP", "0"))
MONGO_URL = os.getenv("MONGO_URL", "")
PORT = int(os.getenv("PORT", 8080))

def get_env_list(var_name):
    val = os.getenv(var_name)
    if val:
        return [int(x.strip()) for x in val.split(',') if x.strip().lstrip("-").isdigit()]
    return []

ADMIN_IDS = get_env_list("ADMIN_IDS")

# --- SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("MangaController")

if not MONGO_URL or not SESSION_STRING:
    logger.error("❌ Config Missing! Make sure MONGO_URL and SESSION_STRING are set.")
    sys.exit(1)

mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = mongo["MangaPlusBot"]
col_queue = db["queue"]

app = Client("controller_user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

manga_client = MangaPlus(lang=Language.ENGLISH, viewer=Viewer.VERTICAL)
manga_client.register(device_id=uuid.uuid4().hex)

# --- DUMMY WEB SERVER ---
async def web_server():
    async def handle(request): return web.Response(text="Manga Controller Userbot is Running!")
    server = web.Application()
    server.router.add_get("/", handle)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌍 Web server started on port {PORT}")

# --- HELPERS ---
async def is_admin(message):
    if not ADMIN_IDS: return True
    if message.from_user.id not in ADMIN_IDS:
        return False
    return True

def find_all_mangas(data, results_list):
    """Recursively hunts through the raw API dictionary to find manga chapters."""
    if isinstance(data, dict):
        title_obj = data.get('title') or data.get('manga')
        chapter_id = data.get('chapterId') or data.get('chapter_id')
        
        if isinstance(title_obj, dict) and chapter_id:
            manga_name = title_obj.get('name') or title_obj.get('titleName') or title_obj.get('title_name')
            raw_chapter = data.get('chapterName') or data.get('chapter_name') or data.get('name') or "Unknown"
            
            if manga_name:
                clean_chapter = str(raw_chapter).replace("#", "").strip()
                results_list.append({
                    'manga_name': manga_name,
                    'chapter_num': clean_chapter
                })
                
        for value in data.values():
            find_all_mangas(value, results_list)
    elif isinstance(data, list):
        for item in data:
            find_all_mangas(item, results_list)

async def wait_for_result(start_time, target_title, target_ep, timeout=7200):
    logger.info(f"👀 Watching for success message for: '{target_title}' Chapter {target_ep}")
    loop_start = asyncio.get_event_loop().time()

    while True:
        if (asyncio.get_event_loop().time() - loop_start) > timeout:
            return "timeout"

        try:
            # We check the last 50 messages so we don't miss the original message that gets edited
            async for msg in app.get_chat_history(TARGET_GROUP, limit=50):
                text = (msg.text or msg.caption or "").lower()
                if not text: continue
                
                # Check if it was edited recently
                msg_time = msg.edit_date or msg.date
                if msg_time < (start_time - timedelta(seconds=10)):
                    continue

                # SUCCESS TRIGGER: Catches "✅ Done!" or "✅ Batch complete!" when the worker bot edits its message
                if ("✅ done!" in text or "✅ batch complete!" in text) and target_title.lower() in text:
                    logger.info(f"✅ Detect: Success for {target_title}")
                    return "success"
                
                # FAILURE TRIGGER
                if "❌ error:" in text and target_title.lower() in text:
                    logger.info(f"❌ Detect: Explicit Failure for {target_title}")
                    return "failed"

        except Exception as e:
            logger.error(f"⚠️ Watcher Error: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(10)

async def warm_up():
    logger.info("🔥 Warming up and caching Telegram chats...")
    try:
        logger.info("Downloading dialogs to build internal cache (Takes ~4 seconds)...")
        async for dialog in app.get_dialogs(limit=50):
            if dialog.chat.id == TARGET_GROUP:
                logger.info(f"✅ Target Group Found in dialogs: {dialog.chat.title}")
                break
        
        chat = await app.get_chat(TARGET_GROUP)
        logger.info(f"✅ Target Group Access Confirmed: {chat.title}")
    except Exception as e:
        logger.warning(f"⚠️ Target Group cache failed: {e}\n👉 FIX: Go to Telegram and send a manual message in the target group, then restart the script!")

# --- COMMANDS ---
@app.on_message(filters.command("start"))
async def start(client, message):
    if not await is_admin(message): return
    await message.reply("👋 Manga Controller is Online!\n\n/list - View tracked DB mangas\n/queue - View download status\n/retry_all - Retry failed items\n/clear_queue - Delete pending items")

@app.on_message(filters.command("list"))
async def list_tracked(client, message):
    if not await is_admin(message): return
    count = await col_queue.count_documents({"status": "done"})
    if count == 0:
        await message.reply_text("Still gathering data or no mangas tracked yet... Wait a moment!")
        return
        
    text = f"📊 **Currently tracking {count} series in MongoDB!**\n\n**Latest Updates Found:**\n"
    cursor = col_queue.find({"status": "done"}).sort([("found_at", -1)]).limit(20)
    async for doc in cursor:
        text += f"• {doc.get('title')} (Chapter: {doc.get('ep')})\n"
    await message.reply_text(text)

@app.on_message(filters.command("queue"))
async def queue_status(client, message):
    if not await is_admin(message): return
    try:
        pending_count = await col_queue.count_documents({"status": "pending"})
        failed_count = await col_queue.count_documents({"status": "failed_dl"})
        
        cursor = col_queue.find({"status": {"$in": ["pending", "failed_dl"]}}).sort([("status", -1), ("found_at", 1)])
        items = await cursor.to_list(length=30)
        
        if not items:
            await message.reply("✅ **Queue is empty!**")
            return

        text = f"📋 **Manga Queue Status**\n⏳ Pending: `{pending_count}` | ⚠️ Failed: `{failed_count}`\n\n"
        for i, item in enumerate(items, 1):
            status_icon = "⏳" if item['status'] == "pending" else "⚠️ Failed"
            text += f"**{i}.** {item['title']} - Ch {item['ep']} `{status_icon}`\n"
        
        total = pending_count + failed_count
        if total > 30: text += f"\n...and {total - 30} more."

        await message.reply(text)
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("retry_all"))
async def retry_stuck(client, message):
    if not await is_admin(message): return
    try:
        r = await col_queue.update_many({"status": "failed_dl"}, {"$set": {"status": "pending"}})
        await message.reply(f"🔄 Queued {r.modified_count} failed items for retry.")
    except: pass

@app.on_message(filters.command("clear_queue"))
async def clear_queue(client, message):
    if not await is_admin(message): return
    try:
        r = await col_queue.delete_many({"status": {"$in": ["pending", "failed_dl"]}})
        await message.reply(f"🗑️ Deleted {r.deleted_count} items from queue.")
    except: pass


# --- TASKS ---
async def task_poller():
    logger.info("📡 Manga Polling Task Started.")
    first_run_sync = False
    
    while True:
        try:
            updates = manga_client.getUpdates()
            found_mangas = []
            find_all_mangas(updates, found_mangas)
            
            seen_in_this_fetch = set()
            
            for item in found_mangas:
                title = item["manga_name"]
                ep = item["chapter_num"]
                
                norm_name = title.lower().strip()
                unique_id = f"{norm_name}_{ep}"
                
                if unique_id in seen_in_this_fetch:
                    continue
                seen_in_this_fetch.add(unique_id)
                
                if not await col_queue.find_one({"_id": unique_id}):
                    if not first_run_sync:
                        await col_queue.insert_one({
                            "_id": unique_id, "title": title, "ep": ep,
                            "found_at": datetime.now(timezone.utc), "status": "done", "retry_count": 0
                        })
                    else:
                        await col_queue.insert_one({
                            "_id": unique_id, "title": title, "ep": ep,
                            "found_at": datetime.now(timezone.utc), "status": "pending", "retry_count": 0
                        })
                        logger.info(f"🆕 Queued: {title} - Ch {ep}")
                        
            if not first_run_sync:
                db_count = await col_queue.count_documents({})
                logger.info(f"Initial DB sync complete. Tracking {db_count} total chapters in MongoDB.")
                first_run_sync = True
                
        except Exception as e:
            logger.error(f"Polling Error: {e}")
            
        await asyncio.sleep(600)

async def task_downloader():
    logger.info("⬇️ Downloader Worker Started.")
    while True:
        item = await col_queue.find_one({"status": "pending"}, sort=[("found_at", 1)])
        
        if not item:
            await asyncio.sleep(10)
            continue
            
        title = item["title"]
        ep = item["ep"]
        
        try:
            await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloading"}})
            
            logger.info(f"▶️ [START] Processing: {title} Ch {ep}")
            start_time = datetime.now(timezone.utc)
            
            dl_cmd = f'/dl "{title}" -c {ep} -pdf'
            await app.send_message(TARGET_GROUP, dl_cmd)
            
            result = await wait_for_result(start_time, title, ep, timeout=7200)

            if result == "success":
                logger.info(f"✅ [FINISH] {title} completed.")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "downloaded"}})
                
                logger.info("❄️ Success. Worker resting for 10 minutes...")
                await app.send_message(TARGET_GROUP, f"💤 Success! Manga Worker resting 10m after **{title}**...")
                
                await asyncio.sleep(600) 

            else: 
                logger.warning(f"❌ [FAILED] {title}")
                await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "failed_dl"}})
                await asyncio.sleep(120)

        except Exception as e:
            logger.error(f"Downloader Error: {e}")
            await asyncio.sleep(60)

async def task_uploader():
    """Cleans up downloaded items in the queue"""
    logger.info("⬆️ Database cleanup monitor started.")
    while True:
        item = await col_queue.find_one({"status": "downloaded"})
        if not item:
            await asyncio.sleep(10)
            continue
        await col_queue.update_one({"_id": item["_id"]}, {"$set": {"status": "done"}})
        await asyncio.sleep(1)

async def main():
    await app.start()
    await warm_up()
    await asyncio.gather(web_server(), task_poller(), task_downloader(), task_uploader())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
