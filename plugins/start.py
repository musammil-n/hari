import random
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ForceReply, CallbackQuery
  

@Client.on_message(filters.private & filters.command("start"))
async def start(client, message):
    await message.reply_text("only for @MrMNTG")
