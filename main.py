from aiogram import Bot, Dispatcher, executor, types
from motor import motor_asyncio

from config import MONGODB_URL, MONGO_DB, MONGO_COLLECTION, TELEGRAM_TOKEN
from tools import query_validator
from commands import aggregator
import json

client = motor_asyncio.AsyncIOMotorClient(MONGODB_URL)
db = client[MONGO_DB]
collection = MONGO_COLLECTION

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)

query_example = """Пример входных данных:
            {
            "dt_from": "2022-09-01T00:00:00",
            "dt_upto": "2022-12-31T23:59:00",
            "group_type": "month"
            }
        """

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await bot.send_message(message.chat.id, query_example)


@dp.message_handler(content_types=['text'])
async def aggregate_query(message: types.Message):
    query = query_validator(message.text)
    if not query:
        await bot.send_message(message.chat.id, query_example)
    else:
        result = await aggregator(db, collection, **query)
        result = json.dumps(result)
        await bot.send_message(message.chat.id, result)


executor.start_polling(dp)