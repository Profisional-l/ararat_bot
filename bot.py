import asyncio
import csv
import logging
import os
import re
import ssl
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Union

from dotenv import load_dotenv

load_dotenv()


def configure_ssl() -> None:
    """requests/gspread на Windows часто падают с certifi — используем системные сертификаты."""
    use_system_certs = os.getenv('SSL_USE_SYSTEM_CERTS', 'auto').strip().lower()
    if use_system_certs in {'0', 'false', 'no'}:
        return
    if use_system_certs == 'auto' and sys.platform != 'win32':
        return

    import truststore

    truststore.inject_into_ssl()


configure_ssl()

import gspread
from gspread.exceptions import WorksheetNotFound
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ChatAction, ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAnimation,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
    URLInputFile,
)
from aiogram.utils.chat_action import ChatActionSender
from aiogram.utils.keyboard import InlineKeyboardBuilder
from oauth2client.service_account import ServiceAccountCredentials

# ================== Настройки / конфигурация ==================
BOT_TOKEN = os.getenv('BOT_TOKEN', '').strip()
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '').strip()
PRIVACY_POLICY_URL = os.getenv('PRIVACY_POLICY_URL', '').strip()
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'credentials.json').strip()
CONSENT_LOG_FILE = os.getenv('CONSENT_LOG_FILE', 'consent_pd_log.csv').strip()
ADMIN_IDS_RAW = os.getenv('ADMIN_IDS', '').strip()
WELCOME_TEXT = os.getenv(
    'WELCOME_TEXT',
    'Добро пожаловать! Этот бот помогает записаться на авторскую экскурсию и узнать больше о проекте.',
).strip()
WELCOME_IMAGE_URL = os.getenv('WELCOME_IMAGE_URL', '').strip()
WELCOME_IMAGE_FILE = os.getenv('WELCOME_IMAGE_FILE', 'img/1.jpg').strip()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ADMIN_IDS: Set[int] = set()
for part in ADMIN_IDS_RAW.split(','):
    part = part.strip()
    if part.isdigit():
        ADMIN_IDS.add(int(part))

storage = MemoryStorage()

BROADCAST_ALBUM_DELAY_SEC = 1.0
_broadcast_album_buffers: Dict[str, List[Message]] = {}
_broadcast_album_tasks: Dict[str, asyncio.Task] = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


BELARUS_MOBILE_CODES = ('25', '29', '33', '44')


def normalize_belarus_phone(raw: str) -> Optional[str]:
    """Приводит номер к формату 375XXXXXXXXX (Беларусь)."""
    digits = re.sub(r'\D', '', raw)
    if not digits:
        return None

    if len(digits) == 12 and digits.startswith('375'):
        local = digits[3:]
    elif len(digits) == 11 and digits.startswith('80'):
        local = digits[2:]
    elif len(digits) == 9:
        local = digits
    else:
        return None

    if len(local) != 9 or not local.isdigit():
        return None
    if local[:2] not in BELARUS_MOBILE_CODES:
        return None

    return f'375{local}'


def format_belarus_phone(phone: str) -> str:
    if phone.startswith('375') and len(phone) == 12:
        return f'+375 {phone[3:5]} {phone[5:8]}-{phone[8:10]}-{phone[10:12]}'
    return phone


def validate_config() -> None:
    required = {
        'BOT_TOKEN': BOT_TOKEN,
        'SPREADSHEET_ID': SPREADSHEET_ID,
        'PRIVACY_POLICY_URL': PRIVACY_POLICY_URL,
        'SERVICE_ACCOUNT_FILE': SERVICE_ACCOUNT_FILE,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        logger.error('Не заданы обязательные переменные окружения: %s', ', '.join(missing))
        raise SystemExit('Установите обязательные переменные окружения перед запуском бота.')


validate_config()


def create_bot_session() -> AiohttpSession:
    """На Windows certifi часто не совпадает с системным хранилищем сертификатов."""
    session = AiohttpSession()
    use_system_certs = os.getenv('SSL_USE_SYSTEM_CERTS', 'auto').strip().lower()

    if use_system_certs in {'1', 'true', 'yes'} or (use_system_certs == 'auto' and sys.platform == 'win32'):
        session._connector_init['ssl'] = ssl.create_default_context()
        logger.info('SSL: используется системное хранилище сертификатов')
    else:
        logger.info('SSL: используется certifi')

    return session


bot = Bot(
    token=BOT_TOKEN,
    session=create_bot_session(),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=storage)


@asynccontextmanager
async def show_typing(chat_id: int, action: ChatAction = ChatAction.TYPING):
    async with ChatActionSender(bot=bot, chat_id=chat_id, action=action):
        yield


async def run_sync(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def delete_message_safe(chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def run_with_status(
    target: Message,
    status_text: str,
    func,
    *args,
    action: ChatAction = ChatAction.TYPING,
    **kwargs,
):
    status_msg = await target.answer(status_text)
    try:
        async with show_typing(target.chat.id, action):
            return await run_sync(func, *args, **kwargs)
    finally:
        await delete_message_safe(target.chat.id, status_msg.message_id)


# ================== Модели данных ==================
@dataclass
class ExcursionDate:
    label: str
    max_slots: Optional[int]
    booked: int

    @property
    def is_full(self) -> bool:
        if self.max_slots is None:
            return False
        return self.booked >= self.max_slots


@dataclass
class ContentItem:
    title: str
    content_type: str
    body: str


@dataclass
class UserProfile:
    user_id: int
    username: str
    age_confirmed_at: str
    consent_pd_at: str
    consent_mailing: str


@dataclass
class UserBooking:
    chosen_date: str
    name: str
    phone: str
    submission_timestamp: str


# ================== FSM ==================
class BookingStates(StatesGroup):
    waiting_name = State()
    waiting_phone = State()


class AdminStates(StatesGroup):
    waiting_broadcast_content = State()


# ================== Google Sheets ==================
def get_gspread_client() -> gspread.Client:
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            f'Файл сервисного аккаунта не найден: {SERVICE_ACCOUNT_FILE}. '
            'Укажите путь через SERVICE_ACCOUNT_FILE или поместите credentials.json рядом с bot.py.'
        )

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scopes)
    return gspread.authorize(credentials)


def _open_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


WORKSHEET_TEMPLATES = {
    'Dates': [
        ['date', 'max_slots'],
        ['2026-07-10', '15'],
        ['2026-07-12', '20'],
        ['2026-07-15', '15'],
    ],
    'Submissions': [
        [
            'user_id',
            'username',
            'consent_pd',
            'consent_mailing',
            'chosen_date',
            'name',
            'phone',
            'submission_timestamp',
        ],
    ],
    'Content': [
        ['title', 'type', 'body'],
        [
            'О бренде Ararat',
            'text',
            'Авторская экскурсия с дегустацией. Здесь можно добавить описание маршрута и проекта.',
        ],
    ],
    'MailingList': [
        ['user_id', 'username', 'subscribed_at'],
    ],
    'Users': [
        ['user_id', 'username', 'age_confirmed_at', 'consent_pd_at', 'consent_mailing', 'updated_at'],
    ],
}


def get_worksheet(title: str):
    spreadsheet = _open_spreadsheet()
    try:
        return spreadsheet.worksheet(title)
    except WorksheetNotFound:
        logger.info('Лист "%s" не найден — создаём автоматически', title)
        worksheet = spreadsheet.add_worksheet(title=title, rows=200, cols=20)
        template_rows = WORKSHEET_TEMPLATES.get(title)
        if template_rows:
            worksheet.update(template_rows, value_input_option='USER_ENTERED')
        return worksheet


def _is_header_row(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {'date', 'дата', 'title', 'заголовок', 'user_id'}


def _parse_max_slots(raw_value: str) -> Optional[int]:
    raw_value = raw_value.strip()
    if not raw_value:
        return None
    if not raw_value.isdigit():
        return None
    return int(raw_value)


def count_bookings_by_date() -> Dict[str, int]:
    worksheet = get_worksheet('Submissions')
    rows = worksheet.get_all_values()
    counts: Dict[str, int] = {}
    for row in rows[1:] if rows else []:
        if len(row) < 5:
            continue
        chosen_date = row[4].strip()
        if not chosen_date:
            continue
        counts[chosen_date] = counts.get(chosen_date, 0) + 1
    return counts


def read_available_dates() -> List[ExcursionDate]:
    logger.info('Чтение доступных дат из Google Sheets')
    worksheet = get_worksheet('Dates')
    rows = worksheet.get_all_values()
    booking_counts = count_bookings_by_date()

    dates: List[ExcursionDate] = []
    for index, row in enumerate(rows):
        if not row or not row[0].strip():
            continue
        label = row[0].strip()
        if index == 0 and _is_header_row(label):
            continue

        max_slots = _parse_max_slots(row[1]) if len(row) > 1 else None
        booked = booking_counts.get(label, 0)
        dates.append(ExcursionDate(label=label, max_slots=max_slots, booked=booked))

    logger.info('Найдено дат: %s', len(dates))
    return dates


def read_content_items() -> List[ContentItem]:
    logger.info('Чтение контента из Google Sheets')
    worksheet = get_worksheet('Content')
    rows = worksheet.get_all_values()

    items: List[ContentItem] = []
    for index, row in enumerate(rows):
        if len(row) < 3:
            continue
        title = row[0].strip()
        content_type = row[1].strip().lower()
        body = row[2].strip()
        if not title or not body:
            continue
        if index == 0 and _is_header_row(title):
            continue
        items.append(ContentItem(title=title, content_type=content_type, body=body))

    logger.info('Найдено материалов: %s', len(items))
    return items


def append_submission(
    user_id: int,
    username: str,
    consent_pd_ts: str,
    consent_mailing: str,
    chosen_date: str,
    name: str,
    phone: str,
    submission_ts: str,
) -> None:
    logger.info('Добавление новой заявки в Google Sheets')
    worksheet = get_worksheet('Submissions')
    worksheet.append_row(
        [
            user_id,
            username,
            consent_pd_ts,
            consent_mailing,
            chosen_date,
            name,
            phone,
            submission_ts,
        ],
        value_input_option='USER_ENTERED',
    )
    logger.info('Заявка успешно добавлена')


def save_mailing_subscription(user_id: int, username: str, mailing_value: str) -> None:
    if mailing_value != 'declined':
        upsert_mailing_subscriber(user_id, username, mailing_value)
    upsert_user_profile(user_id, username, consent_mailing=mailing_value)


def save_user_booking(
    user_id: int,
    username: str,
    consent_pd_ts: str,
    consent_mailing: str,
    chosen_date: str,
    name: str,
    phone: str,
    submission_ts: str,
) -> bool:
    dates = {item.label: item for item in read_available_dates()}
    excursion = dates.get(chosen_date)
    if excursion is None or excursion.is_full:
        return False

    append_submission(
        user_id=user_id,
        username=username,
        consent_pd_ts=consent_pd_ts,
        consent_mailing=consent_mailing,
        chosen_date=chosen_date,
        name=name,
        phone=phone,
        submission_ts=submission_ts,
    )
    upsert_user_profile(
        user_id,
        username,
        consent_pd_at=consent_pd_ts or None,
        consent_mailing=consent_mailing,
    )
    return True


def upsert_mailing_subscriber(user_id: int, username: str, subscribed_at: str) -> None:
    worksheet = get_worksheet('MailingList')
    rows = worksheet.get_all_values()
    for row_index, row in enumerate(rows[1:], start=2):
        if row and str(row[0]).strip() == str(user_id):
            worksheet.update(f'B{row_index}:C{row_index}', [[username or '', subscribed_at]])
            return
    worksheet.append_row([user_id, username or '', subscribed_at], value_input_option='USER_ENTERED')


def read_mailing_subscribers() -> List[int]:
    worksheet = get_worksheet('MailingList')
    rows = worksheet.get_all_values()
    subscribers: List[int] = []
    for row in rows[1:] if rows else []:
        if not row or not str(row[0]).strip().isdigit():
            continue
        subscribers.append(int(row[0]))
    return subscribers


def _row_to_user_profile(row: List[str]) -> UserProfile:
    return UserProfile(
        user_id=int(row[0]),
        username=row[1] if len(row) > 1 else '',
        age_confirmed_at=row[2] if len(row) > 2 else '',
        consent_pd_at=row[3] if len(row) > 3 else '',
        consent_mailing=row[4] if len(row) > 4 else 'declined',
    )


def _profile_from_submissions(user_id: int) -> Optional[UserProfile]:
    worksheet = get_worksheet('Submissions')
    rows = worksheet.get_all_values()
    latest_row: Optional[List[str]] = None
    for row in rows[1:] if rows else []:
        if row and str(row[0]).strip() == str(user_id):
            latest_row = row

    if not latest_row or len(latest_row) < 3 or not latest_row[2].strip():
        return None

    consent_pd = latest_row[2].strip()
    return UserProfile(
        user_id=user_id,
        username=latest_row[1] if len(latest_row) > 1 else '',
        age_confirmed_at=consent_pd,
        consent_pd_at=consent_pd,
        consent_mailing=latest_row[3].strip() if len(latest_row) > 3 and latest_row[3].strip() else 'declined',
    )


def get_user_profile(user_id: int) -> Optional[UserProfile]:
    worksheet = get_worksheet('Users')
    rows = worksheet.get_all_values()
    for row in rows[1:] if rows else []:
        if row and str(row[0]).strip() == str(user_id):
            return _row_to_user_profile(row)
    return _profile_from_submissions(user_id)


def upsert_user_profile(
    user_id: int,
    username: str,
    *,
    age_confirmed_at: Optional[str] = None,
    consent_pd_at: Optional[str] = None,
    consent_mailing: Optional[str] = None,
) -> UserProfile:
    worksheet = get_worksheet('Users')
    rows = worksheet.get_all_values()
    updated_at = utc_now_iso()
    existing: Optional[UserProfile] = None

    for row_index, row in enumerate(rows[1:], start=2):
        if row and str(row[0]).strip() == str(user_id):
            existing = _row_to_user_profile(row)
            profile = UserProfile(
                user_id=user_id,
                username=username or existing.username,
                age_confirmed_at=age_confirmed_at or existing.age_confirmed_at,
                consent_pd_at=consent_pd_at or existing.consent_pd_at,
                consent_mailing=consent_mailing if consent_mailing is not None else existing.consent_mailing,
            )
            worksheet.update(
                f'A{row_index}:F{row_index}',
                [[
                    profile.user_id,
                    profile.username,
                    profile.age_confirmed_at,
                    profile.consent_pd_at,
                    profile.consent_mailing,
                    updated_at,
                ]],
            )
            return profile

    fallback = _profile_from_submissions(user_id)
    profile = UserProfile(
        user_id=user_id,
        username=username or (fallback.username if fallback else ''),
        age_confirmed_at=age_confirmed_at or (fallback.age_confirmed_at if fallback else ''),
        consent_pd_at=consent_pd_at or (fallback.consent_pd_at if fallback else ''),
        consent_mailing=(
            consent_mailing
            if consent_mailing is not None
            else (fallback.consent_mailing if fallback else '')
        ),
    )
    worksheet.append_row(
        [profile.user_id, profile.username, profile.age_confirmed_at, profile.consent_pd_at, profile.consent_mailing, updated_at],
        value_input_option='USER_ENTERED',
    )
    return profile


def read_user_submissions(user_id: int) -> List[UserBooking]:
    worksheet = get_worksheet('Submissions')
    rows = worksheet.get_all_values()
    bookings: List[UserBooking] = []

    for row in rows[1:] if rows else []:
        if not row or str(row[0]).strip() != str(user_id):
            continue
        if len(row) < 8:
            continue
        chosen_date = row[4].strip()
        if not chosen_date:
            continue
        bookings.append(
            UserBooking(
                chosen_date=chosen_date,
                name=row[5].strip() if len(row) > 5 else '',
                phone=row[6].strip() if len(row) > 6 else '',
                submission_timestamp=row[7].strip() if len(row) > 7 else '',
            )
        )

    bookings.sort(key=lambda item: item.submission_timestamp or item.chosen_date, reverse=True)
    return bookings


def user_has_completed_onboarding(profile: Optional[UserProfile]) -> bool:
    return bool(profile and profile.consent_pd_at)


def user_needs_mailing_choice(profile: Optional[UserProfile]) -> bool:
    if not user_has_completed_onboarding(profile):
        return False
    return not (profile.consent_mailing or '').strip()


def log_pd_consent(user_id: int, username: str, timestamp: str) -> None:
    file_exists = os.path.exists(CONSENT_LOG_FILE)
    with open(CONSENT_LOG_FILE, mode='a', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['user_id', 'username', 'consent_pd_timestamp'])
        writer.writerow([user_id, username or '', timestamp])
    logger.info('Согласие ПДн записано локально в %s', CONSENT_LOG_FILE)


# ================== UI helpers ==================
def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Записаться на экскурсию', callback_data='book_excursion')],
        [InlineKeyboardButton(text='Мои записи', callback_data='my_bookings')],
        [InlineKeyboardButton(text='О проекте', callback_data='about_project')],
    ])


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Назад в меню', callback_data='main_menu')],
    ])


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def is_broadcastable_message(message: Message) -> bool:
    return bool(
        message.text
        or message.photo
        or message.video
        or message.document
        or message.animation
        or message.audio
        or message.voice
    )


def build_input_media_item(message: Message, caption: Optional[str]) -> Optional[Union[InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAnimation, InputMediaAudio]]:
    caption_kwargs = {'caption': caption, 'parse_mode': ParseMode.HTML} if caption else {}

    if message.photo:
        return InputMediaPhoto(media=message.photo[-1].file_id, **caption_kwargs)
    if message.video:
        return InputMediaVideo(media=message.video.file_id, **caption_kwargs)
    if message.document:
        return InputMediaDocument(media=message.document.file_id, **caption_kwargs)
    if message.animation:
        return InputMediaAnimation(media=message.animation.file_id, **caption_kwargs)
    if message.audio:
        return InputMediaAudio(media=message.audio.file_id, **caption_kwargs)
    return None


def build_album_media(messages: List[Message]) -> List[Union[InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAnimation, InputMediaAudio]]:
    sorted_messages = sorted(messages, key=lambda item: item.message_id)
    album_caption = next((msg.caption for msg in sorted_messages if msg.caption), None)
    media_items = []

    for index, msg in enumerate(sorted_messages):
        caption = album_caption if index == 0 else None
        item = build_input_media_item(msg, caption)
        if item is not None:
            media_items.append(item)

    return media_items


async def send_broadcast_to_subscribers(
    source_message: Message,
    subscribers: List[int],
    album_messages: Optional[List[Message]] = None,
) -> tuple[int, int]:
    sent = 0
    failed = 0
    album_media = build_album_media(album_messages) if album_messages else None

    for user_id in subscribers:
        try:
            if album_media:
                await bot.send_media_group(chat_id=user_id, media=album_media)
            else:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=source_message.chat.id,
                    message_id=source_message.message_id,
                )
            sent += 1
        except Exception:
            failed += 1
            logger.exception('Не удалось отправить рассылку пользователю %s', user_id)
        await asyncio.sleep(0.05)

    return sent, failed


async def execute_broadcast(
    message: Message,
    state: FSMContext,
    album_messages: Optional[List[Message]] = None,
) -> None:
    try:
        subscribers = await run_with_status(message, 'Подготавливаем рассылку…', read_mailing_subscribers)
    except Exception:
        logger.exception('Ошибка при чтении списка рассылки')
        await message.answer('Не удалось получить список подписчиков.')
        await state.clear()
        return

    if not subscribers:
        await message.answer('Список подписчиков пуст.')
        await state.clear()
        return

    if album_messages and not build_album_media(album_messages):
        await message.answer('Не удалось собрать альбом для рассылки.')
        await state.clear()
        return

    status_msg = await message.answer('Отправляем рассылку подписчикам…')
    async with show_typing(message.chat.id):
        sent, failed = await send_broadcast_to_subscribers(message, subscribers, album_messages)

    await delete_message_safe(message.chat.id, status_msg.message_id)
    album_note = ' (альбом)' if album_messages else ''
    await message.answer(f'Рассылка{album_note} завершена. Успешно: {sent}, ошибок: {failed}.')
    await state.clear()


async def buffer_media_group_broadcast(message: Message, state: FSMContext) -> None:
    group_id = message.media_group_id
    if not group_id:
        return

    _broadcast_album_buffers.setdefault(group_id, []).append(message)

    existing_task = _broadcast_album_tasks.get(group_id)
    if existing_task and not existing_task.done():
        existing_task.cancel()

    async def process_album(group_key: str) -> None:
        try:
            await asyncio.sleep(BROADCAST_ALBUM_DELAY_SEC)
            album_messages = _broadcast_album_buffers.pop(group_key, [])
            if album_messages:
                await execute_broadcast(album_messages[0], state, album_messages=album_messages)
        except asyncio.CancelledError:
            return
        finally:
            _broadcast_album_tasks.pop(group_key, None)

    _broadcast_album_tasks[group_id] = asyncio.create_task(process_album(group_id))


def resolve_asset_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(BASE_DIR, path)


def get_welcome_photo() -> Optional[Union[FSInputFile, URLInputFile]]:
    if WELCOME_IMAGE_FILE:
        image_path = resolve_asset_path(WELCOME_IMAGE_FILE)
        if os.path.exists(image_path):
            return FSInputFile(image_path)
        logger.warning('Файл приветственного изображения не найден: %s', image_path)
    if WELCOME_IMAGE_URL:
        return URLInputFile(WELCOME_IMAGE_URL)
    return None


async def send_welcome(target: Message) -> None:
    keyboard = main_menu_keyboard()
    photo = get_welcome_photo()

    if photo:
        status_msg = await target.answer('Ну что, сейчас познакомимся 🍹🙌🏻 !')
        try:
            async with show_typing(target.chat.id, ChatAction.UPLOAD_PHOTO):
                await asyncio.sleep(1)
                await target.answer_photo(photo=photo, caption=WELCOME_TEXT, reply_markup=keyboard)
        finally:
            await delete_message_safe(target.chat.id, status_msg.message_id)
        return

    async with show_typing(target.chat.id):
        await target.answer(WELCOME_TEXT, reply_markup=keyboard)


async def send_main_menu(target: Message, text: str = 'Выберите действие:') -> None:
    await target.answer(text, reply_markup=main_menu_keyboard())


async def show_content_menu(target: Message) -> None:
    try:
        items = await run_with_status(target, 'Ща расскажем о себе!', read_content_items)
    except Exception:
        logger.exception('Ошибка при чтении контента из Google Sheets')
        await target.answer('Сейчас не удалось загрузить материалы. Попробуйте позже.')
        return

    if not items:
        await target.answer('Материалы о проекте скоро появятся. Загляните позже.')
        return

    builder = InlineKeyboardBuilder()
    for index, item in enumerate(items):
        builder.button(text=item.title, callback_data=f'content_{index}')
    builder.button(text='Назад в меню', callback_data='main_menu')
    builder.adjust(1)

    await target.answer('Материалы о проекте:', reply_markup=builder.as_markup())


async def show_content_item(callback: CallbackQuery, item: ContentItem) -> None:
    back_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='К списку материалов', callback_data='about_project')],
        [InlineKeyboardButton(text='Назад в меню', callback_data='main_menu')],
    ])

    if item.content_type == 'link':
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Открыть ссылку', url=item.body)],
            [InlineKeyboardButton(text='К списку материалов', callback_data='about_project')],
            [InlineKeyboardButton(text='Назад в меню', callback_data='main_menu')],
        ])
        await callback.message.answer(f'<b>{item.title}</b>\n\nСсылка на материал:', reply_markup=keyboard)
        return

    await callback.message.answer(f'<b>{item.title}</b>\n\n{item.body}', reply_markup=back_keyboard)


def build_dates_keyboard(dates: List[ExcursionDate]) -> Optional[InlineKeyboardMarkup]:
    available_dates = [item for item in dates if not item.is_full]
    if not available_dates:
        return None

    builder = InlineKeyboardBuilder()
    for item in available_dates:
        if item.max_slots is None:
            button_text = item.label
        else:
            free_slots = item.max_slots - item.booked
            button_text = f'{item.label} (осталось {free_slots})'
        builder.button(text=button_text, callback_data=f'date_{item.label}')
    builder.button(text='Назад в меню', callback_data='main_menu')
    builder.adjust(1)
    return builder.as_markup()


async def send_dates_menu(target: Message, message_text: str = 'Выберите удобную дату экскурсии:') -> bool:
    try:
        dates = await run_with_status(target, 'Загружаем доступные даты…', read_available_dates)
    except Exception:
        logger.exception('Ошибка при чтении дат из Google Sheets')
        await target.answer('Извините, сейчас невозможна загрузка доступных дат. Попробуйте позже.')
        return False

    keyboard = build_dates_keyboard(dates)
    if keyboard is None:
        await target.answer('Пока нет свободных дат. Пожалуйста, зайдите позже.')
        return False

    await target.answer(message_text, reply_markup=keyboard)
    return True


async def show_dates(callback: CallbackQuery, state: FSMContext, message_text: str = 'Выберите удобную дату экскурсии:') -> None:
    await callback.answer()
    await send_dates_menu(callback.message, message_text)


async def apply_profile_to_state(state: FSMContext, profile: UserProfile) -> None:
    await state.update_data(
        consent_pd_timestamp=profile.consent_pd_at,
        consent_mailing=profile.consent_mailing or 'declined',
    )


async def continue_booking_for_user(
    callback: CallbackQuery,
    state: FSMContext,
    user_id: int,
    username: str,
    message_text: str = 'Выберите удобную дату экскурсии:',
) -> None:
    await state.update_data(chosen_date=None, name=None)
    try:
        profile = await run_with_status(
            callback.message,
            'Подготавливаем запись…',
            get_user_profile,
            user_id,
        )
    except Exception:
        logger.exception('Ошибка при загрузке профиля пользователя')
        await callback.message.answer('Не удалось начать запись. Попробуйте позже.')
        await callback.answer()
        return

    if not user_has_completed_onboarding(profile):
        await start_booking_flow(callback, state)
        return

    await apply_profile_to_state(state, profile)
    await run_sync(upsert_user_profile, user_id, username)

    if user_needs_mailing_choice(profile):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Согласен на информационные рассылки', callback_data='mailing_yes')],
            [InlineKeyboardButton(text='Пропустить', callback_data='mailing_skip')],
        ])
        await callback.message.answer(
            'Хотите получать новости и материалы о проекте?',
            reply_markup=keyboard,
        )
        await callback.answer()
        return

    await show_dates(callback, state, message_text=message_text)


async def show_my_bookings(target: Message, user_id: int) -> None:
    try:
        bookings = await run_with_status(target, 'Загружаем ваши записи…', read_user_submissions, user_id)
    except Exception:
        logger.exception('Ошибка при чтении записей пользователя')
        await target.answer('Не удалось загрузить ваши записи. Попробуйте позже.')
        return

    if not bookings:
        await target.answer(
            'У вас пока нет записей на экскурсии.',
            reply_markup=back_to_menu_keyboard(),
        )
        return

    lines = ['<b>Мои записи на экскурсии:</b>']
    for index, booking in enumerate(bookings, start=1):
        phone_display = format_belarus_phone(booking.phone) if booking.phone else '—'
        lines.append(
            f'\n{index}. <b>{booking.chosen_date}</b>\n'
            f'Имя: {booking.name}\n'
            f'Телефон: {phone_display}'
        )

    await target.answer('\n'.join(lines), reply_markup=back_to_menu_keyboard())


async def start_booking_flow(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Мне 18+', callback_data='age_confirm')],
        [InlineKeyboardButton(text='Назад в меню', callback_data='main_menu')],
    ])
    await callback.message.answer(
        'Для записи необходимо подтвердить, что вам исполнилось 18 лет.',
        reply_markup=keyboard,
    )
    await callback.answer()


# ================== Хэндлеры ==================
@dp.message(Command(commands=['start']))
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await send_welcome(message)


@dp.message(Command(commands=['broadcast']))
async def cmd_broadcast(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await message.answer('Команда доступна только администраторам.')
        return

    await state.set_state(AdminStates.waiting_broadcast_content)
    await message.answer(
        'Отправьте сообщение для рассылки:\n'
        '• текст (HTML)\n'
        '• одно фото / видео / документ\n'
        '• альбом из нескольких фото или видео (выберите несколько и отправьте разом)\n\n'
        'Для отмены отправьте /cancel'
    )


@dp.message(Command(commands=['stats']))
async def cmd_stats(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer('Команда доступна только администраторам.')
        return

    try:
        dates = await run_with_status(message, 'Собираем статистику…', read_available_dates)
    except Exception:
        logger.exception('Ошибка при чтении статистики')
        await message.answer('Не удалось получить статистику.')
        return

    if not dates:
        await message.answer('Даты экскурсий пока не заданы.')
        return

    lines = ['<b>Статистика по датам:</b>']
    for item in dates:
        if item.max_slots is None:
            lines.append(f'• {item.label}: записано {item.booked}')
        else:
            lines.append(f'• {item.label}: {item.booked}/{item.max_slots}')
    await message.answer('\n'.join(lines))


@dp.message(Command(commands=['cancel']))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer('Действие отменено.')
    await send_main_menu(message)


@dp.callback_query(F.data == 'main_menu')
async def callback_main_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await send_main_menu(callback.message)
    await callback.answer()


@dp.callback_query(F.data == 'about_project')
async def callback_about_project(callback: CallbackQuery) -> None:
    await callback.answer()
    await show_content_menu(callback.message)


@dp.callback_query(F.data == 'book_excursion')
async def callback_book_excursion(callback: CallbackQuery, state: FSMContext) -> None:
    await continue_booking_for_user(
        callback,
        state,
        callback.from_user.id,
        callback.from_user.username or '',
    )


@dp.callback_query(F.data == 'my_bookings')
async def callback_my_bookings(callback: CallbackQuery) -> None:
    await callback.answer()
    await show_my_bookings(callback.message, callback.from_user.id)


@dp.callback_query(F.data.startswith('content_'))
async def callback_content_item(callback: CallbackQuery) -> None:
    await callback.answer()
    try:
        index = int(callback.data.removeprefix('content_'))
        items = await run_with_status(
            callback.message,
            'Открываем материал…',
            read_content_items,
        )
        item = items[index]
    except Exception:
        logger.exception('Ошибка при открытии материала')
        await callback.message.answer('Материал не найден. Попробуйте снова.')
        return

    await show_content_item(callback, item)


@dp.callback_query(F.data == 'age_confirm')
async def callback_age_confirm(callback: CallbackQuery, state: FSMContext) -> None:
    consent_ts = utc_now_iso()
    user_id = callback.from_user.id
    username = callback.from_user.username or ''
    await run_sync(upsert_user_profile, user_id, username, age_confirmed_at=consent_ts)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Согласен на обработку ПДн', callback_data='consent_pd_yes')],
        [InlineKeyboardButton(text='Не согласен', callback_data='consent_pd_no')],
    ])
    await callback.message.answer(
        f'Для продолжения ознакомьтесь с <a href="{PRIVACY_POLICY_URL}">политикой обработки персональных данных</a> и дайте согласие.',
        reply_markup=keyboard,
    )
    await callback.answer()


@dp.callback_query(F.data == 'consent_pd_no')
async def callback_consent_pd_no(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.answer('Без согласия на обработку персональных данных продолжить невозможно.')
    await send_main_menu(callback.message)
    await callback.answer()


@dp.callback_query(F.data == 'consent_pd_yes')
async def callback_consent_pd_yes(callback: CallbackQuery, state: FSMContext) -> None:
    consent_ts = utc_now_iso()
    user_id = callback.from_user.id
    username = callback.from_user.username or ''

    try:
        log_pd_consent(user_id, username, consent_ts)
    except Exception:
        logger.exception('Ошибка при записи локального согласия ПДн')

    try:
        profile = await run_with_status(
            callback.message,
            'Сохраняем согласие…😈',
            upsert_user_profile,
            user_id,
            username,
            consent_pd_at=consent_ts,
        )
    except Exception:
        logger.exception('Ошибка при сохранении согласия ПДн')
        await callback.message.answer('Не удалось сохранить согласие. Попробуйте позже.')
        await callback.answer()
        return

    await state.update_data(consent_pd_timestamp=consent_ts)

    if user_needs_mailing_choice(profile):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Согласен на информационные рассылки', callback_data='mailing_yes')],
            [InlineKeyboardButton(text='Пропустить', callback_data='mailing_skip')],
        ])
        await callback.message.answer(
            'Спасибо! Хотите получать новости и материалы о проекте?',
            reply_markup=keyboard,
        )
        await callback.answer()
        return

    await state.update_data(consent_mailing=profile.consent_mailing or 'declined')
    await show_dates(callback, state)


@dp.callback_query(F.data.in_({'mailing_yes', 'mailing_skip'}))
async def callback_mailing_choice(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    if 'consent_pd_timestamp' not in data:
        await callback.message.answer('Сначала подтвердите возраст и согласие на обработку ПДн.')
        await callback.answer()
        return

    user_id = callback.from_user.id
    username = callback.from_user.username or ''

    if callback.data == 'mailing_yes':
        mailing_value = utc_now_iso()
        try:
            await run_with_status(
                callback.message,
                'Сохраняем вашу подписку…',
                save_mailing_subscription,
                user_id,
                username,
                mailing_value,
            )
        except Exception:
            logger.exception('Ошибка при сохранении подписки на рассылку')
            await callback.message.answer('Не удалось сохранить подписку. Попробуйте позже.')
            await callback.answer()
            return
        await callback.message.answer('Вы подписались на информационные рассылки.')
    else:
        mailing_value = 'declined'
        try:
            await run_with_status(
                callback.message,
                'Сохраняем настройки…',
                save_mailing_subscription,
                user_id,
                username,
                mailing_value,
            )
        except Exception:
            logger.exception('Ошибка при сохранении настроек рассылки')
            await callback.message.answer('Не удалось сохранить настройки. Попробуйте позже.')
            await callback.answer()
            return
        await callback.message.answer('Вы пропустили подписку на рассылки.')

    await state.update_data(consent_mailing=mailing_value)
    await show_dates(callback, state)


@dp.callback_query(F.data.startswith('date_'))
async def callback_choose_date(callback: CallbackQuery, state: FSMContext) -> None:
    chosen_date = callback.data.removeprefix('date_')
    await callback.answer()

    try:
        dates_list = await run_with_status(
            callback.message,
            'Проверяем выбранную дату…',
            read_available_dates,
        )
        dates = {item.label: item for item in dates_list}
    except Exception:
        logger.exception('Ошибка при проверке доступности даты')
        await callback.message.answer('Не удалось проверить дату. Попробуйте снова.')
        return

    excursion = dates.get(chosen_date)
    if excursion is None or excursion.is_full:
        await callback.message.answer('К сожалению, на эту дату мест больше нет. Выберите другую дату.')
        await show_dates(callback, state)
        return

    await state.update_data(chosen_date=chosen_date)
    await callback.message.answer(f'Вы выбрали дату: {chosen_date}. Пожалуйста, введите своё имя.')
    await state.set_state(BookingStates.waiting_name)


@dp.message(BookingStates.waiting_name)
async def process_name(message: Message, state: FSMContext) -> None:
    name = (message.text or '').strip()
    if not name:
        await message.answer('Имя не может быть пустым. Пожалуйста, введите ваше имя ещё раз.')
        return

    await state.update_data(name=name)
    await state.set_state(BookingStates.waiting_phone)
    await message.answer(
        'Введите ваш телефон в белорусском формате.\n'
        'Например: +375291234567, 80291234567 или 291234567'
    )


@dp.message(BookingStates.waiting_phone)
async def process_phone(message: Message, state: FSMContext) -> None:
    phone = normalize_belarus_phone(message.text or '')
    if not phone:
        await message.answer(
            'Неверный формат телефона. Укажите белорусский номер, например:\n'
            '• +375291234567\n'
            '• 80291234567\n'
            '• 291234567'
        )
        return

    data = await state.get_data()
    chosen_date = data.get('chosen_date', '')

    user_id = message.from_user.id
    username = message.from_user.username or ''
    consent_pd_ts = data.get('consent_pd_timestamp', '')
    consent_mailing = data.get('consent_mailing', '')

    if not consent_pd_ts:
        try:
            profile = await run_with_status(
                message,
                'Проверяем ваш профиль…',
                get_user_profile,
                user_id,
            )
        except Exception:
            logger.exception('Ошибка при загрузке профиля')
            await message.answer('Не удалось завершить запись. Попробуйте позже.')
            return

        if profile and profile.consent_pd_at:
            consent_pd_ts = profile.consent_pd_at
            consent_mailing = profile.consent_mailing or 'declined'
        else:
            await message.answer('Сначала пройдите регистрацию через «Записаться на экскурсию».')
            await state.clear()
            return

    if not consent_mailing:
        consent_mailing = 'declined'

    name = data.get('name', '')
    submission_ts = utc_now_iso()
    phone_display = format_belarus_phone(phone)

    try:
        saved = await run_with_status(
            message,
            'Уже записываем вас на экскурсию…',
            save_user_booking,
            user_id,
            username,
            consent_pd_ts,
            consent_mailing,
            chosen_date,
            name,
            phone,
            submission_ts,
        )
    except Exception:
        logger.exception('Ошибка при сохранении заявки в Google Sheets')
        await message.answer('Извините, произошла ошибка при сохранении заявки. Попробуйте позже.')
        return

    if not saved:
        await message.answer('На выбранную дату мест уже нет. Выберите другую дату.')
        await state.set_state(None)
        await send_dates_menu(message, message_text='Выберите другую дату:')
        return

    await message.answer(
        f'Вы записаны на экскурсию <b>{chosen_date}</b>.\nИмя: {name}\nТелефон: {phone_display}',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='Записаться ещё', callback_data='new_submission')],
            [InlineKeyboardButton(text='В главное меню', callback_data='main_menu')],
        ]),
    )
    await state.set_state(None)


@dp.callback_query(F.data == 'new_submission')
async def callback_new_submission(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(chosen_date=None, name=None)
    await continue_booking_for_user(
        callback,
        state,
        callback.from_user.id,
        callback.from_user.username or '',
        message_text='Выберите новую дату для следующей заявки:',
    )


@dp.message(AdminStates.waiting_broadcast_content)
async def process_broadcast_content(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    if not is_broadcastable_message(message):
        await message.answer(
            'Отправьте текст, фото, видео, документ или аудио для рассылки.\n'
            'Для отмены — /cancel'
        )
        return

    if message.media_group_id:
        await buffer_media_group_broadcast(message, state)
        return

    await execute_broadcast(message, state)


@dp.message()
async def fallback_message(message: Message) -> None:
    await message.answer('Используйте /start, чтобы открыть главное меню.')


if __name__ == '__main__':
    logger.info('Запуск бота...')
    dp.run_polling(bot)
