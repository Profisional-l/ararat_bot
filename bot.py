import asyncio
import csv
import logging
import os
import re
import socket
import ssl
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set, TypeVar, Union

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
from aiogram.filters import Command, StateFilter
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
from requests.exceptions import ConnectionError as RequestsConnectionError

# ================== Настройки / конфигурация ==================
BOT_TOKEN = os.getenv('BOT_TOKEN', '').strip()
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '').strip()
PRIVACY_POLICY_FILE = os.getenv(
    'PRIVACY_POLICY_FILE',
    'docs/Положение_о_политике_в_отношении_обработки_персональных_данных.pdf',
).strip()
CONSENT_FORM_FILE = os.getenv(
    'CONSENT_FORM_FILE',
    'docs/Soglasie_na_obrabotku_personalnyh_dannyh_Платформа.pdf',
).strip()
CONSENT_DOCUMENT_VERSION = os.getenv('CONSENT_DOCUMENT_VERSION', '1').strip()
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'credentials.json').strip()
CONSENT_LOG_FILE = os.getenv('CONSENT_LOG_FILE', 'consent_pd_log.csv').strip()
ADMIN_IDS_RAW = os.getenv('ADMIN_IDS', '').strip()
WELCOME_TEXT = os.getenv(
    'WELCOME_TEXT',
    'Друзья! Приветствуем вас в чат-боте проекта «ARARAT открывает новые грани». '
    'Минск хранит множество легенд и интересных историй. Готовы увидеть город по-новому?',
).strip()
MAIN_MENU_TEXT = os.getenv(
    'MAIN_MENU_TEXT',
    'Вместе с экскурсоводом Анной Богдановой мы подготовили необычный маршрут по знаковым местам города. '
    'Интересные истории уже ждут своих слушателей, осталось только выбрать дату!',
).strip()
SELECT_DATE_TEXT = 'Выберите удобную дату посещения экскурсии'
MAILING_PROMPT_TEXT = 'Хотите первыми узнавать новости проекта и получать эксклюзивные материалы?'
WELCOME_IMAGE_URL = os.getenv('WELCOME_IMAGE_URL', '').strip()
WELCOME_IMAGE_FILE = os.getenv('WELCOME_IMAGE_FILE', 'img/1.jpg').strip()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def resolve_asset_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(BASE_DIR, path)


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


MONTHS_GENITIVE = (
    '',
    'января',
    'февраля',
    'марта',
    'апреля',
    'мая',
    'июня',
    'июля',
    'августа',
    'сентября',
    'октября',
    'ноября',
    'декабря',
)
WEEKDAYS_RU = (
    'понедельник',
    'вторник',
    'среда',
    'четверг',
    'пятница',
    'суббота',
    'воскресенье',
)
EXCURSION_DATETIME_FORMATS = (
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M',
    '%Y-%m-%d',
    '%d.%m.%Y %H:%M',
    '%d.%m.%Y %H:%M:%S',
    '%d.%m.%Y',
)
EXCURSION_TIME_FORMATS = ('%H:%M', '%H:%M:%S', '%H.%M')


def _parse_excursion_time(raw: str) -> Optional[datetime]:
    normalized = raw.strip().replace(' ', ':')
    if not normalized:
        return None
    for fmt in EXCURSION_TIME_FORMATS:
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    digits = re.sub(r'\D', '', normalized)
    if len(digits) == 4 and digits.isdigit():
        return datetime.strptime(f'{digits[:2]}:{digits[2:]}', '%H:%M')
    return None


def parse_excursion_datetime(date_raw: str, time_raw: str = '') -> Optional[datetime]:
    date_raw = date_raw.strip()
    time_raw = time_raw.strip()
    if not date_raw:
        return None

    combined = f'{date_raw} {time_raw}'.strip()
    for fmt in EXCURSION_DATETIME_FORMATS:
        try:
            return datetime.strptime(combined, fmt)
        except ValueError:
            continue

    if time_raw:
        time_part = _parse_excursion_time(time_raw)
        if time_part is None:
            return None
        for date_fmt in ('%Y-%m-%d', '%d.%m.%Y'):
            try:
                date_part = datetime.strptime(date_raw, date_fmt)
                return date_part.replace(hour=time_part.hour, minute=time_part.minute, second=0, microsecond=0)
            except ValueError:
                continue
    return None


def excursion_has_time(date_raw: str, time_raw: str = '') -> bool:
    if time_raw.strip():
        return True
    for fmt in EXCURSION_DATETIME_FORMATS:
        if '%H' not in fmt:
            continue
        try:
            datetime.strptime(date_raw.strip(), fmt)
            return True
        except ValueError:
            continue
    return False


def normalize_excursion_key(date_raw: str, time_raw: str = '') -> str:
    parsed = parse_excursion_datetime(date_raw, time_raw)
    if parsed is None:
        if time_raw.strip():
            return f'{date_raw.strip()} {time_raw.strip()}'
        return date_raw.strip()
    if excursion_has_time(date_raw, time_raw):
        return parsed.strftime('%Y-%m-%d %H:%M')
    return parsed.strftime('%Y-%m-%d')


def format_excursion_label(label: str) -> str:
    label = label.strip()
    if not label:
        return label

    if ' ' in label:
        date_part, time_part = label.split(None, 1)
    else:
        date_part, time_part = label, ''

    parsed = parse_excursion_datetime(date_part, time_part)
    if parsed is None:
        return label

    day = parsed.day
    month = MONTHS_GENITIVE[parsed.month]
    weekday = WEEKDAYS_RU[parsed.weekday()]
    if excursion_has_time(date_part, time_part):
        time_str = parsed.strftime('%H:%M')
        return f'{day} {month} ({weekday}, {time_str})'
    return f'{day} {month} ({weekday})'


def format_excursion_confirmation_date(label: str) -> str:
    label = label.strip()
    if not label:
        return label

    if ' ' in label:
        date_part, time_part = label.split(None, 1)
    else:
        date_part, time_part = label, ''

    parsed = parse_excursion_datetime(date_part, time_part)
    if parsed is None:
        return label
    return parsed.strftime('%d-%m-%Y')


def excursion_sort_key(label: str) -> datetime:
    parsed = parse_excursion_datetime(label)
    return parsed or datetime.max.replace(tzinfo=None)


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
        'SERVICE_ACCOUNT_FILE': SERVICE_ACCOUNT_FILE,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        logger.error('Не заданы обязательные переменные окружения: %s', ', '.join(missing))
        raise SystemExit('Установите обязательные переменные окружения перед запуском бота.')

    for env_name, file_path in (
        ('PRIVACY_POLICY_FILE', PRIVACY_POLICY_FILE),
        ('CONSENT_FORM_FILE', CONSENT_FORM_FILE),
    ):
        resolved = resolve_asset_path(file_path)
        if not os.path.exists(resolved):
            logger.error('Не найден файл %s: %s', env_name, resolved)
            raise SystemExit(f'Поместите PDF-документ по пути {resolved} или укажите другой путь в {env_name}.')


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
    display_label: str
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
SHEETS_RETRY_ATTEMPTS = 3
SHEETS_RETRY_DELAY_SEC = 1.5

_gspread_client: Optional[gspread.Client] = None
_spreadsheet = None

T = TypeVar('T')


def _reset_spreadsheet_cache() -> None:
    global _spreadsheet
    _spreadsheet = None


def with_sheets_retry(func: Callable[..., T]) -> Callable[..., T]:
    def wrapper(*args, **kwargs) -> T:
        last_error: Optional[Exception] = None
        for attempt in range(1, SHEETS_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except (RequestsConnectionError, socket.gaierror, TimeoutError, OSError) as exc:
                last_error = exc
                _reset_spreadsheet_cache()
                if attempt >= SHEETS_RETRY_ATTEMPTS:
                    break
                logger.warning(
                    'Сбой сети при обращении к Google Sheets (попытка %s/%s): %s',
                    attempt,
                    SHEETS_RETRY_ATTEMPTS,
                    exc,
                )
                time.sleep(SHEETS_RETRY_DELAY_SEC * attempt)
        raise last_error  # type: ignore[misc]

    return wrapper


def get_gspread_client() -> gspread.Client:
    global _gspread_client
    if _gspread_client is not None:
        return _gspread_client

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
    _gspread_client = gspread.authorize(credentials)
    return _gspread_client


@with_sheets_retry
def _open_spreadsheet():
    global _spreadsheet
    if _spreadsheet is not None:
        return _spreadsheet

    client = get_gspread_client()
    _spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return _spreadsheet


WORKSHEET_TEMPLATES = {
    'Dates': [
        ['date', 'max_slots', 'time'],
        ['2026-07-10', '15', '18:00'],
        ['2026-07-12', '20', '12:00'],
        ['2026-07-15', '15', '18:00'],
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
    return normalized in {'date', 'дата', 'title', 'заголовок', 'user_id', 'time', 'время'}


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
        date_raw = row[0].strip()
        if index == 0 and _is_header_row(date_raw):
            continue

        time_raw = row[2].strip() if len(row) > 2 else ''
        if _is_header_row(time_raw):
            time_raw = ''

        label = normalize_excursion_key(date_raw, time_raw)
        max_slots = _parse_max_slots(row[1]) if len(row) > 1 else None
        booked = booking_counts.get(label, 0)
        dates.append(
            ExcursionDate(
                label=label,
                display_label=format_excursion_label(label),
                max_slots=max_slots,
                booked=booked,
            )
        )

    dates.sort(key=lambda item: excursion_sort_key(item.label))
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


def user_declined_mailing(profile: Optional[UserProfile]) -> bool:
    if not profile:
        return False
    return (profile.consent_mailing or '').strip() == 'declined'


def user_is_mailing_subscriber(profile: Optional[UserProfile]) -> bool:
    if not profile:
        return False
    value = (profile.consent_mailing or '').strip()
    return bool(value) and value != 'declined'


def log_pd_consent(user_id: int, username: str, timestamp: str) -> None:
    file_exists = os.path.exists(CONSENT_LOG_FILE)
    with open(CONSENT_LOG_FILE, mode='a', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['user_id', 'username', 'consent_pd_timestamp', 'document_version'])
        writer.writerow([user_id, username or '', timestamp, CONSENT_DOCUMENT_VERSION])
    logger.info('Согласие ПДн записано локально в %s', CONSENT_LOG_FILE)


def consent_pd_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text='📋 Политика конфиденциальности', callback_data='show_privacy_policy')
    builder.button(text='📝 Форма согласия', callback_data='show_consent_form')
    builder.button(text='✅ Даю согласие', callback_data='consent_pd_yes')
    builder.button(text='❌ Не согласен', callback_data='consent_pd_no')
    builder.adjust(2, 1, 1)
    return builder.as_markup()


async def send_legal_pdf(target: Message, file_path: str, caption: str) -> bool:
    resolved = resolve_asset_path(file_path)
    if not os.path.exists(resolved):
        logger.error('PDF-документ не найден: %s', resolved)
        await target.answer('Документ временно недоступен. Попробуйте позже.')
        return False

    async with show_typing(target.chat.id, ChatAction.UPLOAD_DOCUMENT):
        await target.answer_document(document=FSInputFile(resolved), caption=caption)
    return True


# ================== UI helpers ==================
def build_main_menu_keyboard(profile: Optional[UserProfile] = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text='Записаться на экскурсию', callback_data='book_excursion')],
        [InlineKeyboardButton(text='Мои записи', callback_data='my_bookings')],
        [InlineKeyboardButton(text='О проекте', callback_data='about_project')],
    ]
    if profile and user_declined_mailing(profile):
        rows.append([InlineKeyboardButton(text='Подписаться на рассылку', callback_data='mailing_subscribe')])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return build_main_menu_keyboard()


async def resolve_main_menu_keyboard(user_id: Optional[int] = None) -> InlineKeyboardMarkup:
    if user_id is None:
        return main_menu_keyboard()

    try:
        profile = await run_sync(get_user_profile, user_id)
    except Exception:
        logger.exception('Ошибка при загрузке профиля для меню')
        return main_menu_keyboard()

    return build_main_menu_keyboard(profile)


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
    user_id = target.from_user.id if target.from_user else None
    keyboard = await resolve_main_menu_keyboard(user_id)
    photo = get_welcome_photo()

    if photo:
        status_msg = await target.answer('Ну что, сейчас познакомимся 🍹🙌🏻 !')
        try:
            async with show_typing(target.chat.id, ChatAction.UPLOAD_PHOTO):
                await asyncio.sleep(1)
                await target.answer_photo(photo=photo, caption=WELCOME_TEXT)
        finally:
            await delete_message_safe(target.chat.id, status_msg.message_id)
        await target.answer(MAIN_MENU_TEXT, reply_markup=keyboard)
        return

    async with show_typing(target.chat.id):
        await target.answer(WELCOME_TEXT)
        await target.answer(MAIN_MENU_TEXT, reply_markup=keyboard)


async def send_main_menu(
    target: Message,
    text: str = MAIN_MENU_TEXT,
    user_id: Optional[int] = None,
) -> None:
    resolved_user_id = user_id or (target.from_user.id if target.from_user else None)
    keyboard = await resolve_main_menu_keyboard(resolved_user_id)
    await target.answer(text, reply_markup=keyboard)


async def send_main_menu_to_chat(chat_id: int, text: str = MAIN_MENU_TEXT, user_id: Optional[int] = None) -> None:
    keyboard = await resolve_main_menu_keyboard(user_id)
    await bot.send_message(chat_id, text, reply_markup=keyboard)


async def navigate_to_main_menu(event: Union[Message, CallbackQuery], state: FSMContext) -> None:
    await state.clear()
    if isinstance(event, CallbackQuery):
        await event.answer()
        user_id = event.from_user.id
        if event.message:
            await send_main_menu(event.message, user_id=user_id)
        else:
            await send_main_menu_to_chat(user_id, user_id=user_id)
        return

    await send_main_menu(event)


async def show_content_menu(target: Message) -> None:
    try:
        items = await run_with_status(target, 'Сейчас расскажем о себе!', read_content_items)
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
            button_text = item.display_label
        else:
            free_slots = item.max_slots - item.booked
            button_text = f'{item.display_label} (Осталось: {free_slots})'
        builder.button(text=button_text, callback_data=f'date_{item.label}')
    builder.button(text='Назад в меню', callback_data='main_menu')
    builder.adjust(1)
    return builder.as_markup()


async def send_dates_menu(target: Message, message_text: str = SELECT_DATE_TEXT) -> bool:
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


async def show_dates(callback: CallbackQuery, state: FSMContext, message_text: str = SELECT_DATE_TEXT) -> None:
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
    message_text: str = SELECT_DATE_TEXT,
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
        await callback.message.answer(MAILING_PROMPT_TEXT, reply_markup=keyboard)
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
        date_display = format_excursion_label(booking.chosen_date)
        lines.append(
            f'\n{index}. <b>{date_display}</b>\n'
            f'Имя: {booking.name}\n'
            f'Телефон: {phone_display}'
        )

    await target.answer('\n'.join(lines), reply_markup=back_to_menu_keyboard())


async def ensure_consent_pd_in_state(state: FSMContext, user_id: int) -> bool:
    data = await state.get_data()
    if data.get('consent_pd_timestamp'):
        return True

    try:
        profile = await run_sync(get_user_profile, user_id)
    except Exception:
        logger.exception('Ошибка при загрузке профиля для подтверждения ПДн')
        return False

    if profile and profile.consent_pd_at:
        await state.update_data(consent_pd_timestamp=profile.consent_pd_at)
        return True

    return False


async def finish_mailing_choice(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    if data.get('mailing_standalone'):
        await state.update_data(mailing_standalone=None)
        await callback.answer()
        await send_main_menu(callback.message, user_id=callback.from_user.id)
        return

    await show_dates(callback, state)


async def start_booking_flow(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Мне 18+', callback_data='age_confirm')],
        [InlineKeyboardButton(text='Назад в меню', callback_data='main_menu')],
    ])
    await callback.message.answer(
        'Для продолжения подтвердите, что вам исполнилось 18 лет.',
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
            lines.append(f'• {item.display_label}: записано {item.booked}')
        else:
            lines.append(f'• {item.display_label}: {item.booked}/{item.max_slots}')
    await message.answer('\n'.join(lines))


@dp.message(Command(commands=['cancel']))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer('Действие отменено.')
    await send_main_menu(message)


@dp.callback_query(F.data == 'main_menu', StateFilter('*'))
async def callback_main_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await navigate_to_main_menu(callback, state)


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

    await callback.message.answer(
        'Пожалуйста, ознакомьтесь с политикой обработки персональных данных и дайте согласие.',
        reply_markup=consent_pd_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == 'show_privacy_policy')
async def callback_show_privacy_policy(callback: CallbackQuery) -> None:
    await callback.answer()
    await send_legal_pdf(
        callback.message,
        PRIVACY_POLICY_FILE,
        'Положение о политике в отношении обработки персональных данных',
    )


@dp.callback_query(F.data == 'show_consent_form')
async def callback_show_consent_form(callback: CallbackQuery) -> None:
    await callback.answer()
    await send_legal_pdf(
        callback.message,
        CONSENT_FORM_FILE,
        'Согласие на обработку персональных данных',
    )


@dp.callback_query(F.data == 'consent_pd_no')
async def callback_consent_pd_no(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.answer('Без согласия на обработку персональных данных продолжить невозможно.')
    await send_main_menu(callback.message, user_id=callback.from_user.id)
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
            'Сохраняем согласие…',
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
        await callback.message.answer(MAILING_PROMPT_TEXT, reply_markup=keyboard)
        await callback.answer()
        return

    await state.update_data(consent_mailing=profile.consent_mailing or 'declined')
    await show_dates(callback, state)


@dp.callback_query(F.data == 'mailing_subscribe')
async def callback_mailing_subscribe(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    user_id = callback.from_user.id

    try:
        profile = await run_with_status(
            callback.message,
            'Загружаем профиль…',
            get_user_profile,
            user_id,
        )
    except Exception:
        logger.exception('Ошибка при загрузке профиля для подписки на рассылку')
        await callback.message.answer('Не удалось открыть подписку. Попробуйте позже.')
        return

    if not user_has_completed_onboarding(profile):
        await callback.message.answer('Сначала пройдите регистрацию через «Записаться на экскурсию».')
        return

    if user_is_mailing_subscriber(profile):
        await callback.message.answer(
            'Вы уже подписаны на рассылку.',
            reply_markup=back_to_menu_keyboard(),
        )
        return

    if not user_declined_mailing(profile):
        await send_main_menu(callback.message, user_id=callback.from_user.id)
        return

    await state.update_data(
        consent_pd_timestamp=profile.consent_pd_at,
        mailing_standalone=True,
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='Согласен на информационные рассылки', callback_data='mailing_yes')],
        [InlineKeyboardButton(text='Назад в меню', callback_data='main_menu')],
    ])
    await callback.message.answer(MAILING_PROMPT_TEXT, reply_markup=keyboard)


@dp.callback_query(F.data.in_({'mailing_yes', 'mailing_skip'}))
async def callback_mailing_choice(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    if not await ensure_consent_pd_in_state(state, user_id):
        await callback.message.answer('Сначала подтвердите возраст и согласие на обработку ПДн.')
        await callback.answer()
        return

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
        await callback.message.answer(
            'Благодарим за вашу подписку! Теперь вы будете получать новости о проекте и дополнительные материалы.'
        )
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
        await callback.message.answer(
            'Вы отказались от подписки. При желании вы сможете вернуться и оформить ее позже.'
        )

    await state.update_data(consent_mailing=mailing_value)
    await finish_mailing_choice(callback, state)


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
    date_display = excursion.display_label
    await callback.message.answer(
        f'Вы выбрали дату: <b>{date_display}</b>\nПожалуйста, введите свое имя'
    )
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
        'Введите ваш телефон.\n'
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

    date_display = format_excursion_confirmation_date(chosen_date)
    await message.answer(
        f'Вы успешно записаны на экскурсию, которая пройдет {date_display}.\n'
        f'Имя: {name}\nТелефон: {phone_display}',
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
async def fallback_message(message: Message, state: FSMContext) -> None:
    await navigate_to_main_menu(message, state)


if __name__ == '__main__':
    logger.info('Запуск бота...')
    dp.run_polling(bot)
