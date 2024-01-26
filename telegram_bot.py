import sys
import os
import time
import re
import logging
import traceback
import sqlite3
from argparse import ArgumentParser
from typing import Optional, Tuple, List
from telegram import Update, ChatMember, ChatMemberUpdated, Chat
from telegram.ext import (ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler,
                          filters, ChatMemberHandler)
from telegram.constants import ParseMode


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(module)s %(name)s %(levelname)s: %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
for loggr in ['asyncio', 'httpcore.http11', 'telegram.ext.ExtBot', 'telegram.ext.Updater', 'httpcore.connection']:
    logging.getLogger(loggr).setLevel(logging.INFO)
logger = logging.getLogger('main')

parser = ArgumentParser(description='AlfaLeak Telegram bot (@AlfaLeak_bot)')
parser.add_argument('db', help='Database file')
args = parser.parse_args()

db = sqlite3.connect(args.db)
cur = db.cursor()

HELP_TEXT = '''<b>ФОРМАТ ПОИСКА:</b>

<b>ФИО и дата рождения</b>
/search Фамилия Имя Отчество / YYYY-MM-DD
Если дата рождения не известна, можно вывести всех клиентов с ФИО при помощи
/search Фамилия Имя Отчество

<b>Контактные данные</b>
/contact номер | email

<b>Телефон</b>
/phone номер
Эта команда воспринимает суффикс (окончание) телефонного номера и требует не менее 7 цифр.

Все команды ищут по префиксам (кроме /phone), но возвращают не более 100 совпадений.
'''
MSG_RESPONSE = 'Вообще-то я игнорирую сообщения и отвечаю только на знакомые команды. Попробуйте /help.'
SYNTAX_ERROR_MESSAGE = 'Вероятно, какая-то ошибка. Проверьте, что формат поиска верный.'
NEWLINE = '\n'


class CommandSyntaxException(Exception):
    pass


def db_get_clients_by_name(name: str) -> List[Tuple[int, str, str]]:
    sql = '''
SELECT client_number, name, birthdate
FROM clients
WHERE name GLOB ?
ORDER BY name, birthdate
LIMIT 100
'''
    params = (name.upper() + '*',)
    logger.debug(f'SQL: {sql}, parameters: {params}')
    result = cur.execute(sql, params)
    return list(result)


def db_get_clients_by_name_and_dob(name: str, dob: str) -> List[Tuple[int, str, str]]:
    sql = """
SELECT client_number, name, birthdate
FROM clients
WHERE name GLOB ? AND birthdate GLOB ?
ORDER BY name, birthdate
LIMIT 100
"""
    params = (name.upper() + '*', dob + '*')
    logger.debug(f"SQL: {sql.replace(NEWLINE, ' ')}, parameters: {params}")
    result = cur.execute(sql, params)
    return list(result)


def db_get_clients_by_phone_suffix(phone_suffix: str) -> List[Tuple[int, str, str]]:
    phone_suffix_reversed = phone_suffix[::-1]
    sql = '''
SELECT cl.client_number, cl.name, cl.birthdate
FROM clients cl
JOIN contacts co ON cl.client_number = co.client_number
WHERE co.info_reversed GLOB ?
ORDER BY cl.name
LIMIT 100
'''
    params = (phone_suffix_reversed + '*',)
    logger.debug(f"SQL: {sql.replace(NEWLINE, ' ')}, parameters: {params}")
    result = cur.execute(sql, params)
    return list(result)


def db_get_clients_by_contact(info: str) -> List[Tuple[int, str, str]]:
    sql = '''
SELECT cl.client_number, cl.name, cl.birthdate
FROM clients cl
JOIN contacts co ON cl.client_number = co.client_number
WHERE co.info GLOB ?
ORDER BY cl.name
LIMIT 100
'''
    params = (info + '*',)
    logger.debug(f"SQL: {sql.replace(NEWLINE, ' ')}, parameters: {params}")
    result = cur.execute(sql, params)
    return list(result)


def get_contact_info(client_number: int) -> List[str]:
    result = cur.execute(
        'SELECT info FROM contacts WHERE client_number = ? ORDER BY info',
        (client_number,)
    )
    return [row[0] for row in result]


def get_cards(client_number: int) -> List[Tuple[str, str]]:
    result = cur.execute(
        'SELECT card_number, expiry_date FROM cards WHERE client_number = ? ORDER BY expiry_date DESC',
        (client_number,)
    )
    return list(result)


def extract_status_change(chat_member_update: ChatMemberUpdated) -> Optional[Tuple[bool, bool]]:
    """Takes a ChatMemberUpdated instance and extracts whether the 'old_chat_member' was a member
    of the chat and whether the 'new_chat_member' is a member of the chat. Returns None, if
    the status didn't change.
    """
    status_change = chat_member_update.difference().get("status")
    old_is_member, new_is_member = chat_member_update.difference().get("is_member", (None, None))

    if status_change is None:
        return None

    old_status, new_status = status_change
    was_member = old_status in [
        ChatMember.MEMBER,
        ChatMember.OWNER,
        ChatMember.ADMINISTRATOR,
    ] or (old_status == ChatMember.RESTRICTED and old_is_member is True)
    is_member = new_status in [
        ChatMember.MEMBER,
        ChatMember.OWNER,
        ChatMember.ADMINISTRATOR,
    ] or (new_status == ChatMember.RESTRICTED and new_is_member is True)

    return was_member, is_member


def get_command_value(text: str) -> str:
    """
    Extract string after command.
    E.g. '/command Expecto Patronum!' -> Expecto Patronum!
    """
    first_cut = text.find(' ')
    if first_cut == -1:
        raise CommandSyntaxException

    search_string = text[first_cut + 1:]
    if not search_string:
        raise CommandSyntaxException

    return search_string


def parse_search_command(text: str) -> Tuple[str, Optional[str]]:
    search_string = get_command_value(text)

    if '/' in search_string:
        name, dob = search_string.split('/')
        name, dob = name.strip(), dob.strip()
        if re.fullmatch(r'\d{4}(-\d\d){0,2}', dob):
            return name, dob
        else:
            raise CommandSyntaxException
    else:
        name = search_string.strip()
        if re.fullmatch(r'[^\W\d_]+( [^\W\d_]+)*', name.replace('-', '')):
            return name, None
        else:
            raise CommandSyntaxException


def parse_phone_command(text: str) -> Optional[str]:
    phone = get_command_value(text)
    if re.fullmatch(r'\d{7,}', phone):
        return phone
    else:
        raise CommandSyntaxException


def parse_contact_command(text: str) -> str:
    contact = get_command_value(text)
    return contact


def get_client_text(name: str, dob: str, contacts: List[str], cards: List[Tuple[str, str]]) -> str:
    contacts_txt = '\n'.join(contacts)
    cards_txt = '\n'.join(f'{c[0]} до {c[1][:4]}/{c[1][5:7]}' for c in cards)
    return f'''<b>{name} / {dob}</b>
<b>Контакты</b>:
{contacts_txt}
<b>Карты</b>:
{cards_txt}
'''


def render_response(clients: List[Tuple[int, str, str]]) -> str:
    if len(clients) == 0:
        reply_text = 'Не найдено'
    elif len(clients) <= 3:
        texts = [get_client_text(c[1], c[2], get_contact_info(c[0]), get_cards(c[0]))
                 for c in clients]
        reply_text = '\n\n'.join(texts)
    else:
        texts = [f'{c[1]} / {c[2]}' for c in clients]
        reply_text = '\n'.join(texts)

    return reply_text


def search_by_name_and_dob(search_name: str, search_dob: str):
    logger.info(f"Searching '{search_name} / {search_dob}'...")
    start_ts = time.monotonic()

    if search_dob:
        clients = db_get_clients_by_name_and_dob(search_name, search_dob)
    else:
        clients = db_get_clients_by_name(search_name)

    reply_text = render_response(clients)
    end_ts = time.monotonic()
    logger.info(f'Completed search in {end_ts - start_ts:.3f} s')

    return reply_text


def search_by_phone(phone_suffix: str):
    logger.info(f"Searching '%{phone_suffix}'...")
    start_ts = time.monotonic()
    clients = db_get_clients_by_phone_suffix(phone_suffix)
    reply_text = render_response(clients)
    end_ts = time.monotonic()
    logger.info(f'Completed search in {end_ts - start_ts:.3f} s')

    return reply_text


def search_by_contact(info: str):
    logger.info(f"Searching '{info}%'...")
    start_ts = time.monotonic()
    clients = db_get_clients_by_contact(info)
    reply_text = render_response(clients)
    end_ts = time.monotonic()
    logger.info(f'Completed search in {end_ts - start_ts:.3f} s')

    return reply_text


def log_activity(update: Update):
    message = update.message
    chat = update.effective_chat

    if message and chat:
        log_message = f"'{message.text}' from {message.from_user.full_name} ({message.from_user.username})"

        if chat.type == Chat.PRIVATE:
            log_message += ' privately'
        elif chat.type == Chat.GROUP:
            log_message += f' from {chat.title}'

        logger.info(log_message)
    else:
        logger.info(update)


async def on_help_command(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    log_activity(update)
    chat = update.effective_chat
    if chat.type == Chat.PRIVATE:
        await update.effective_user.send_message(HELP_TEXT, ParseMode.HTML)
    elif chat.type == Chat.GROUP:
        await chat.send_message(HELP_TEXT, ParseMode.HTML)


async def on_search_command(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    log_activity(update)
    message = update.message
    chat = update.effective_chat

    try:
        search_name, search_dob = parse_search_command(message.text)
        reply_text = search_by_name_and_dob(search_name, search_dob)
        await chat.send_message(reply_text, ParseMode.HTML)
    except CommandSyntaxException:
        await chat.send_message(SYNTAX_ERROR_MESSAGE)
    except Exception:
        etype, e, tb = sys.exc_info()
        logger.error(traceback.format_exception_only(etype, e)[0] + traceback.format_exc())


async def on_phone_command(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    log_activity(update)
    message = update.message
    chat = update.effective_chat

    try:
        search_phone = parse_phone_command(message.text)
        reply_text = search_by_phone(search_phone)
        await chat.send_message(reply_text, ParseMode.HTML)
    except CommandSyntaxException:
        await chat.send_message(SYNTAX_ERROR_MESSAGE)
    except Exception:
        etype, e, tb = sys.exc_info()
        logger.error(traceback.format_exception_only(etype, e)[0] + traceback.format_exc())


async def on_contact_command(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    log_activity(update)
    message = update.message
    chat = update.effective_chat

    try:
        info = parse_contact_command(message.text)
        reply_text = search_by_contact(info)
        await chat.send_message(reply_text, ParseMode.HTML)
    except CommandSyntaxException:
        await chat.send_message(SYNTAX_ERROR_MESSAGE)
    except Exception:
        etype, e, tb = sys.exc_info()
        logger.error(traceback.format_exception_only(etype, e)[0] + traceback.format_exc())


async def on_message(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    log_activity(update)
    chat = update.effective_chat
    await chat.send_message(MSG_RESPONSE)


async def on_chat_member(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    log_activity(update)
    was_member, is_member = extract_status_change(update.my_chat_member)
    action = 'unknown'
    if not was_member and is_member:
        action = 'added'
    elif was_member and not is_member:
        action = 'removed'
    logger.info(f'chat_member: {action} to ')


app = ApplicationBuilder().token(os.getenv('BOT_TOKEN')).build()
app.add_handler(CommandHandler("search", on_search_command))
app.add_handler(CommandHandler("contact", on_contact_command))
app.add_handler(CommandHandler("phone", on_phone_command))
app.add_handler(CommandHandler("help", on_help_command))
app.add_handler(CommandHandler("start", on_help_command))
app.add_handler(MessageHandler(filters.TEXT, on_message))
app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

app.run_polling(allowed_updates=Update.ALL_TYPES)
