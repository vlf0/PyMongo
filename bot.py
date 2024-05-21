import os
import sys
import asyncio
import logging
import json

from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram import Bot, Dispatcher, types, filters
from algorithm import AggregateManager


TOKEN = os.getenv('TG_TOKEN_VLF')

dp = Dispatcher()


@dp.message(filters.CommandStart())
async def start_handler(message: types.Message) -> None:
    await message.answer(rf"Hello, *{message.from_user.full_name}*\! Please, insert query in json format\.")


@dp.message()
async def get_cumulative_salaries(message: types.Message) -> None:
    aggregation_params = json.loads(message.text)
    print(aggregation_params)

    result_dict = AggregateManager(dt_from=aggregation_params.get('dt_from', None),
                                   dt_upto=aggregation_params.get('dt_upto', None),
                                   group_type=aggregation_params.get('group_type', None)
                                   ).get_cumulative_data()
    result = json.dumps(result_dict)
    print(result)
    await message.answer(result, parse_mode='')


async def main() -> None:
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
