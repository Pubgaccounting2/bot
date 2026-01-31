import asyncio
import json
import logging
import os
import sqlite3
import shutil
import tempfile
import random
from contextlib import asynccontextmanager, suppress

import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Any, Iterable, Dict, List, Tuple
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BufferedInputFile
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramNetworkError
from dotenv import load_dotenv

# =============================================================================
# CONFIGURATION
# =============================================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required")

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "8513112712"))
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Europe/Amsterdam")
DEFAULT_REMINDER_TIME = os.getenv("DEFAULT_REMINDER_TIME", "21:00")
DB_PATH = os.getenv("DB_PATH", "addiction_support_bot.db")
ANTIFLOOD_DELAY = float(os.getenv("ANTIFLOOD_DELAY", "0.3"))  # seconds
SCHEDULER_TICK_SECONDS = int(os.getenv("SCHEDULER_TICK_SECONDS", "60"))

# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# TEXTS AND MESSAGES
# =============================================================================

TEXTS = {
    "welcome_preview": (
        "Здравствуйте.\n\n"
        "Это инструмент для ежедневного самоотслеживания и поддержки "
        "в работе с зависимостями.\n\n"
        "• Все данные хранятся локально\n"
        "• Вы можете выбрать одну или несколько зависимостей для отслеживания\n"
        "• Бот не собирает личную информацию и не передаёт данные третьим лицам\n\n"
        "Этот бот не заменяет профессиональную помощь врача или психотерапевта."
    ),
    "privacy_info": (
        "О конфиденциальности:\n\n"
        "• Данные хранятся в локальной базе на сервере бота\n"
        "• Ваш Telegram ID используется только для идентификации\n"
        "• Никакие данные не передаются третьим лицам\n"
        "• Вы можете удалить все свои данные в любой момент через настройки"
    ),
    "select_addictions": (
        "Выберите типы зависимостей, которые хотите отслеживать.\n"
        "Можно выбрать несколько. Нажмите на пункт для выбора/отмены."
    ),
    "select_reminder_time": (
        "Выберите время для ежедневного напоминания о заполнении отчёта."
    ),
    "onboarding_complete": (
        "Настройка завершена.\n\n"
        "Вы будете получать напоминания в выбранное время. "
        "Всё взаимодействие происходит через кнопки ниже."
    ),
    "main_menu": "Главное меню",
    "daily_report_intro": "Ежедневный отчёт",
    "daily_report_question": "Сегодня по «{addiction}» было…",
    "craving_question": "Уровень тяги сегодня?",
    "need_support_question": "Нужна поддержка прямо сейчас?",
    "report_saved": (
        "Отчёт сохранён. Спасибо, что отметили.\n\n"
        "Эти данные помогут видеть динамику со временем."
    ),
    "report_already_filled": "Отчёт за сегодня уже заполнен.",
    "relapse_support": (
        "Срыв — это не конец пути.\n\n"
        "Сейчас важно:\n"
        "1. Сделать паузу\n"
        "2. Медленно подышать (4 секунды вдох, 4 задержка, 6 выдох)\n"
        "3. Составить план на ближайший час\n\n"
        "Если нужна дополнительная поддержка, воспользуйтесь кнопкой ниже."
    ),
    "emergency_help": (
        "Экстренная поддержка\n\n"
        "1. Сделайте паузу. Дышите медленно.\n"
        "2. Уберите доступ к триггеру, если это возможно.\n"
        "3. Свяжитесь с человеком, которому доверяете.\n"
        "4. Если вы в опасности — обратитесь в экстренные службы вашего региона.\n\n"
        "Помните: этот бот не заменяет профессиональную помощь."
    ),
    "breathing_exercise": (
        "Дыхательное упражнение (2 минуты)\n\n"
        "Следуйте ритму:\n"
        "• Вдох — 4 секунды\n"
        "• Задержка — 4 секунды\n"
        "• Выдох — 6 секунд\n\n"
        "Повторите 8-10 циклов. Сосредоточьтесь на дыхании."
    ),
    "pause_90_seconds": (
        "Пауза 90 секунд\n\n"
        "Исследования показывают, что интенсивность тяги снижается "
        "примерно через 90 секунд, если не подкреплять её действием.\n\n"
        "Просто подождите. Наблюдайте за ощущениями, не действуя."
    ),
    "ten_minute_plan": (
        "План на 10 минут\n\n"
        "Займите себя чем-то на ближайшие 10 минут:\n"
        "• Выйдите на короткую прогулку\n"
        "• Выпейте стакан воды\n"
        "• Позвоните кому-то\n"
        "• Сделайте 10 приседаний\n"
        "• Примите душ\n\n"
        "Цель — переключить внимание и дать тяге ослабнуть."
    ),
    "cognitive_reframe": (
        "Когнитивная переоценка\n\n"
        "Задайте себе вопросы:\n"
        "• Что я чувствую прямо сейчас?\n"
        "• Это чувство — факт или интерпретация?\n"
        "• Как я буду себя чувствовать через час, если поддамся?\n"
        "• Что бы я сказал другу в такой ситуации?\n\n"
        "Запишите ответы мысленно или на бумаге."
    ),
    "progress_title": "Ваш прогресс",
    "no_data": "Пока нет данных для отображения.",
    "plan_title": "План на сегодня",
    "tools_title": "Инструменты",
    "settings_title": "Настройки",
    "delete_confirm": (
        "Вы уверены, что хотите удалить все свои данные?\n"
        "Это действие необратимо."
    ),
    "data_deleted": "Все ваши данные удалены.",
    "admin_menu": "Панель администратора",
    "broadcast_confirm": "Подтвердите отправку рассылки всем пользователям.",
    "broadcast_sent": "Рассылка завершена. Отправлено: {sent}, ошибок: {errors}.",
}

# Supportive notification messages (calm, respectful tone)
SUPPORT_MESSAGES = [
    "Если день был тяжёлым, отметьте это. Данные нужны, чтобы видеть динамику.",
    "Небольшая отметка сегодня — это вклад в завтрашний день.",
    "Отслеживание помогает замечать закономерности. Заполните отчёт, когда будет удобно.",
    "Каждый день — это данные. Даже сложные дни важны для понимания процесса.",
    "Напоминание заполнить ежедневный отчёт. Это займёт меньше минуты.",
    "Отметка за сегодня поможет увидеть прогресс со временем.",
    "Регулярность отслеживания важнее идеальных результатов.",
    "Если сегодня был срыв — это тоже информация. Отметьте и двигайтесь дальше.",
    "Ваши данные — ваш инструмент. Заполните отчёт за сегодня.",
    "Даже в сложные дни отметка помогает сохранять осознанность.",
    "Трекинг — это не оценка, а наблюдение. Заполните отчёт.",
    "Один день за раз. Отметьте, как прошёл сегодняшний.",
    "Напоминание: ежедневный отчёт. Без осуждения, просто данные.",
    "Отслеживание само по себе — форма заботы о себе.",
    "Время для ежедневной отметки. Это часть процесса.",
]

# Addiction types
ADDICTION_TYPES = {
    "alcohol": "Алкоголь",
    "nicotine": "Никотин",
    "gambling": "Азартные игры",
    "porn": "Порно / компульсивное сексуальное поведение",
    "social_media": "Соцсети / скроллинг",
    "food": "Еда / переедание",
    "substances": "Психоактивные вещества",
    "other": "Другое",
}

# Daily goals
DAILY_GOALS = [
    "Держаться 24 часа",
    "Избегать известных триггеров",
    "Позвонить близкому человеку",
    "Прогулка минимум 20 минут",
    "Лечь спать вовремя",
    "Пить достаточно воды",
    "Не оставаться в одиночестве",
]

# Common triggers
COMMON_TRIGGERS = [
    "Стресс на работе",
    "Конфликты в отношениях",
    "Одиночество",
    "Скука",
    "Усталость",
    "Определённое время суток",
    "Определённые места",
    "Определённые люди",
    "Финансовые проблемы",
    "Праздники / выходные",
]

# Reasons to stay sober
REASONS_LIST = [
    "Здоровье",
    "Семья",
    "Работа / карьера",
    "Финансы",
    "Самоуважение",
    "Отношения",
    "Физическая форма",
    "Ясность мышления",
    "Будущие цели",
    "Дети",
]

# Reminder times
REMINDER_TIMES = ["07:00", "09:00", "12:00", "18:00", "21:00", "23:00"]

# =============================================================================
# FSM STATES
# =============================================================================

class OnboardingStates(StatesGroup):
    viewing_preview = State()
    selecting_addictions = State()
    selecting_time = State()
    entering_custom_time = State()


class DailyReportStates(StatesGroup):
    answering_addiction = State()
    answering_craving = State()
    answering_support = State()


class SettingsStates(StatesGroup):
    main = State()
    changing_addictions = State()
    changing_time = State()
    confirming_delete = State()


class AdminStates(StatesGroup):
    main = State()
    broadcast_text = State()
    broadcast_confirm = State()
    viewing_templates = State()
    adding_template = State()


class ProgressStates(StatesGroup):
    viewing = State()


class PlanStates(StatesGroup):
    main = State()
    selecting_goal = State()
    selecting_triggers = State()


class ToolsStates(StatesGroup):
    main = State()
    selecting_reasons = State()


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def _row_to_dict(row: Optional[sqlite3.Row]) -> Dict[str, Any]:
    return dict(row) if row is not None else {}


class Database:
    """Simple async SQLite wrapper with a single connection + lock.

    Why a single connection?
    - SQLite has best reliability with a single writer.
    - We serialize DB access with an asyncio.Lock to avoid 'database is locked' issues.

    NOTE: For higher throughput, you can move to Postgres.
    """

    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self.conn is not None:
            return
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = sqlite3.Row

        # Pragmas tuned for typical bot workloads
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")
        await self.conn.execute("PRAGMA foreign_keys=ON;")
        await self.conn.execute("PRAGMA busy_timeout=5000;")
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn is None:
            return
        await self.conn.close()
        self.conn = None

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    @asynccontextmanager
    async def locked(self):
        if self.conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            yield self.conn

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        if self.conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            cur = await self.conn.execute(sql, tuple(params))
            row = await cur.fetchone()
            await cur.close()
            return row

    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> List[sqlite3.Row]:
        if self.conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            cur = await self.conn.execute(sql, tuple(params))
            rows = await cur.fetchall()
            await cur.close()
            return rows

    async def execute(self, sql: str, params: Iterable[Any] = (), *, commit: bool = True) -> None:
        if self.conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            await self.conn.execute(sql, tuple(params))
            if commit:
                await self.conn.commit()

    async def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]], *, commit: bool = True) -> None:
        if self.conn is None:
            raise RuntimeError("Database is not connected")
        async with self._lock:
            await self.conn.executemany(sql, [tuple(p) for p in seq_of_params])
            if commit:
                await self.conn.commit()


db = Database(DB_PATH)


async def _ensure_column(conn: aiosqlite.Connection, table: str, column: str, ddl_fragment: str) -> None:
    """Add a column if missing (SQLite-friendly migration)."""
    cur = await conn.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    await cur.close()
    existing = {r[1] for r in rows}  # (cid, name, type, notnull, dflt_value, pk)
    if column not in existing:
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_fragment}")


async def init_db() -> None:
    """Initialize database tables + small migrations + indexes."""
    await db.connect()

    async with db.locked() as conn:
        # Users table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_onboarded INTEGER DEFAULT 0,
                timezone TEXT DEFAULT 'Europe/Amsterdam',
                reminder_time TEXT DEFAULT '21:00',
                support_enabled INTEGER DEFAULT 1,
                support_frequency INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_active TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Addictions reference table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS addictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL
            )
        """)

        # User addictions (many-to-many)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_addictions (
                user_id INTEGER,
                addiction_code TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, addiction_code)
            )
        """)

        # Daily logs
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                date TEXT,
                addiction_code TEXT,
                status TEXT,
                craving_level TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, date, addiction_code)
            )
        """)

        # User settings (for triggers, goals, reasons)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER,
                key TEXT,
                value TEXT,
                PRIMARY KEY (user_id, key)
            )
        """)

        # Notifications log
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                notification_type TEXT,
                date TEXT,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Admin templates
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS notification_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Broadcast log
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT,
                sent_count INTEGER,
                error_count INTEGER,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Migrations for older DBs (if any)
        await _ensure_column(conn, "users", "support_enabled", "support_enabled INTEGER DEFAULT 1")
        await _ensure_column(conn, "users", "support_frequency", "support_frequency INTEGER DEFAULT 1")
        await _ensure_column(conn, "users", "timezone", "timezone TEXT DEFAULT 'Europe/Amsterdam'")
        await _ensure_column(conn, "users", "reminder_time", "reminder_time TEXT DEFAULT '21:00'")
        await _ensure_column(conn, "users", "is_onboarded", "is_onboarded INTEGER DEFAULT 0")
        await _ensure_column(conn, "users", "last_active", "last_active TEXT DEFAULT CURRENT_TIMESTAMP")

        # Populate addictions reference
        for code, name in ADDICTION_TYPES.items():
            await conn.execute(
                "INSERT OR IGNORE INTO addictions (code, name) VALUES (?, ?)",
                (code, name),
            )

        # Populate default templates (de-duplicate by a unique index on text)
        await conn.execute("""
            DELETE FROM notification_templates
            WHERE id NOT IN (
                SELECT MIN(id) FROM notification_templates
                GROUP BY text
            )
        """)
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_templates_text_unique ON notification_templates(text)")
        for msg in SUPPORT_MESSAGES:
            await conn.execute(
                "INSERT OR IGNORE INTO notification_templates (text) VALUES (?)",
                (msg,),
            )

        # De-duplicate notifications and then enforce uniqueness to support INSERT OR IGNORE reliably
        await conn.execute("""
            DELETE FROM notifications_log
            WHERE id NOT IN (
                SELECT MIN(id) FROM notifications_log
                GROUP BY user_id, notification_type, date
            )
        """)
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_unique ON notifications_log(user_id, notification_type, date)"
        )

        # Helpful indexes
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_logs_user_date ON daily_logs(user_id, date)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_onboarded ON users(is_onboarded)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_addictions_user ON user_addictions(user_id)")
        await conn.commit()

    logger.info("Database initialized successfully")


async def get_or_create_user(user_id: int, username: str = None, first_name: str = None) -> Dict[str, Any]:
    """Get or create user record."""
    await db.connect()

    async with db.locked() as conn:
        cur = await conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()

        now_iso = datetime.now().isoformat()

        if row:
            await conn.execute(
                "UPDATE users SET last_active = ?, username = ?, first_name = ? WHERE user_id = ?",
                (now_iso, username, first_name, user_id),
            )
            await conn.commit()
            return dict(row)

        await conn.execute(
            "INSERT INTO users (user_id, username, first_name, last_active) VALUES (?, ?, ?, ?)",
            (user_id, username, first_name, now_iso),
        )
        await conn.commit()

        cur = await conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return _row_to_dict(row)


async def set_user_onboarded(user_id: int, value: bool = True) -> None:
    await db.execute(
        "UPDATE users SET is_onboarded = ? WHERE user_id = ?",
        (1 if value else 0, user_id),
    )


async def set_user_reminder_time(user_id: int, time_str: str) -> None:
    await db.execute(
        "UPDATE users SET reminder_time = ? WHERE user_id = ?",
        (time_str, user_id),
    )


async def set_user_support_settings(user_id: int, enabled: bool = None, frequency: int = None) -> None:
    """Update support notification settings."""
    await db.connect()
    async with db.locked() as conn:
        if enabled is not None:
            await conn.execute(
                "UPDATE users SET support_enabled = ? WHERE user_id = ?",
                (1 if enabled else 0, user_id),
            )
        if frequency is not None:
            await conn.execute(
                "UPDATE users SET support_frequency = ? WHERE user_id = ?",
                (frequency, user_id),
            )
        await conn.commit()


async def get_user_timezone(user_id: int) -> str:
    row = await db.fetchone("SELECT timezone FROM users WHERE user_id = ?", (user_id,))
    tz_str = row["timezone"] if row else DEFAULT_TIMEZONE
    if not tz_str:
        return DEFAULT_TIMEZONE
    return tz_str


async def get_user_addictions(user_id: int) -> List[str]:
    rows = await db.fetchall(
        "SELECT addiction_code FROM user_addictions WHERE user_id = ?",
        (user_id,),
    )
    return [r["addiction_code"] for r in rows]


async def toggle_user_addiction(user_id: int, addiction_code: str) -> bool:
    """Toggle addiction selection. Returns True if added, False if removed."""
    await db.connect()
    async with db.locked() as conn:
        cur = await conn.execute(
            "SELECT 1 FROM user_addictions WHERE user_id = ? AND addiction_code = ?",
            (user_id, addiction_code),
        )
        exists = (await cur.fetchone()) is not None
        await cur.close()

        if exists:
            await conn.execute(
                "DELETE FROM user_addictions WHERE user_id = ? AND addiction_code = ?",
                (user_id, addiction_code),
            )
            await conn.commit()
            return False

        await conn.execute(
            "INSERT INTO user_addictions (user_id, addiction_code) VALUES (?, ?)",
            (user_id, addiction_code),
        )
        await conn.commit()
        return True


async def clear_user_addictions(user_id: int) -> None:
    await db.execute("DELETE FROM user_addictions WHERE user_id = ?", (user_id,))


async def upsert_daily_log(user_id: int, date: str, addiction_code: str, status: str, craving_level: str = None) -> None:
    await db.connect()
    async with db.locked() as conn:
        await conn.execute("""
            INSERT INTO daily_logs (user_id, date, addiction_code, status, craving_level)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, date, addiction_code)
            DO UPDATE SET status = excluded.status, craving_level = excluded.craving_level
        """, (user_id, date, addiction_code, status, craving_level))
        await conn.commit()


async def get_today_logs(user_id: int, date: str) -> Dict[str, Dict[str, Optional[str]]]:
    rows = await db.fetchall(
        "SELECT addiction_code, status, craving_level FROM daily_logs WHERE user_id = ? AND date = ?",
        (user_id, date),
    )
    return {
        r["addiction_code"]: {"status": r["status"], "craving_level": r["craving_level"]}
        for r in rows
    }


async def get_logs_for_period(user_id: int, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    rows = await db.fetchall("""
        SELECT date, addiction_code, status, craving_level
        FROM daily_logs
        WHERE user_id = ? AND date >= ? AND date <= ?
        ORDER BY date DESC
    """, (user_id, start_date, end_date))
    return [dict(r) for r in rows]


async def get_streak(user_id: int, addiction_code: str) -> int:
    rows = await db.fetchall("""
        SELECT date, status
        FROM daily_logs
        WHERE user_id = ? AND addiction_code = ?
        ORDER BY date DESC
    """, (user_id, addiction_code))

    streak = 0
    for r in rows:
        if r["status"] == "clean":
            streak += 1
        else:
            break
    return streak


async def get_user_setting(user_id: int, key: str) -> Optional[str]:
    row = await db.fetchone(
        "SELECT value FROM user_settings WHERE user_id = ? AND key = ?",
        (user_id, key),
    )
    return row["value"] if row else None


async def set_user_setting(user_id: int, key: str, value: str) -> None:
    await db.connect()
    async with db.locked() as conn:
        await conn.execute("""
            INSERT INTO user_settings (user_id, key, value) VALUES (?, ?, ?)
            ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value
        """, (user_id, key, value))
        await conn.commit()


async def log_notification(user_id: int, notification_type: str, date: str) -> None:
    """Log sent notification (idempotent)."""
    await db.execute(
        "INSERT OR IGNORE INTO notifications_log (user_id, notification_type, date) VALUES (?, ?, ?)",
        (user_id, notification_type, date),
    )


async def was_notification_sent(user_id: int, notification_type: str, date: str) -> bool:
    row = await db.fetchone(
        "SELECT 1 FROM notifications_log WHERE user_id = ? AND notification_type = ? AND date = ?",
        (user_id, notification_type, date),
    )
    return row is not None


async def delete_user_data(user_id: int) -> None:
    await db.connect()
    async with db.locked() as conn:
        await conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        await conn.execute("DELETE FROM user_addictions WHERE user_id = ?", (user_id,))
        await conn.execute("DELETE FROM daily_logs WHERE user_id = ?", (user_id,))
        await conn.execute("DELETE FROM user_settings WHERE user_id = ?", (user_id,))
        await conn.execute("DELETE FROM notifications_log WHERE user_id = ?", (user_id,))
        await conn.commit()


async def get_all_users() -> List[Dict[str, Any]]:
    rows = await db.fetchall("SELECT * FROM users")
    return [dict(r) for r in rows]


async def get_users_for_reminder() -> List[Dict[str, Any]]:
    rows = await db.fetchall("""
        SELECT user_id, reminder_time, timezone, support_enabled, support_frequency
        FROM users
        WHERE is_onboarded = 1
    """)
    return [dict(r) for r in rows]


async def get_admin_stats() -> Dict[str, int]:
    await db.connect()
    async with db.locked() as conn:
        cur = await conn.execute("SELECT COUNT(*) as total FROM users")
        total_users = (await cur.fetchone())["total"]
        await cur.close()

        week_ago_dt = datetime.now() - timedelta(days=7)
        week_ago_iso = week_ago_dt.isoformat()
        week_ago_date = week_ago_dt.strftime("%Y-%m-%d")

        cur = await conn.execute(
            "SELECT COUNT(*) as active FROM users WHERE last_active >= ?",
            (week_ago_iso,),
        )
        active_users = (await cur.fetchone())["active"]
        await cur.close()

        cur = await conn.execute("SELECT COUNT(*) as total FROM daily_logs")
        total_logs = (await cur.fetchone())["total"]
        await cur.close()

        cur = await conn.execute(
            "SELECT COUNT(*) as recent FROM daily_logs WHERE date >= ?",
            (week_ago_date,),
        )
        recent_logs = (await cur.fetchone())["recent"]
        await cur.close()

        return {
            "total_users": int(total_users),
            "active_users_7d": int(active_users),
            "total_logs": int(total_logs),
            "logs_7d": int(recent_logs),
        }


async def get_notification_templates() -> List[Dict[str, Any]]:
    rows = await db.fetchall("SELECT * FROM notification_templates ORDER BY id")
    return [dict(r) for r in rows]


async def toggle_template(template_id: int) -> bool:
    await db.connect()
    async with db.locked() as conn:
        await conn.execute(
            "UPDATE notification_templates SET is_active = NOT is_active WHERE id = ?",
            (template_id,),
        )
        cur = await conn.execute(
            "SELECT is_active FROM notification_templates WHERE id = ?",
            (template_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        await conn.commit()
        return bool(row["is_active"]) if row else False


async def add_template(text: str) -> None:
    await db.execute("INSERT INTO notification_templates (text) VALUES (?)", (text,))


async def log_broadcast(text: str, sent_count: int, error_count: int) -> None:
    await db.execute(
        "INSERT INTO broadcast_log (text, sent_count, error_count) VALUES (?, ?, ?)",
        (text, sent_count, error_count),
    )


async def export_user_data(user_id: int) -> Dict[str, Any]:
    """Export all user data as dictionary."""
    await db.connect()

    user_row = await db.fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = _row_to_dict(user_row)

    addiction_rows = await db.fetchall("SELECT addiction_code FROM user_addictions WHERE user_id = ?", (user_id,))
    addictions = [r["addiction_code"] for r in addiction_rows]

    log_rows = await db.fetchall("SELECT * FROM daily_logs WHERE user_id = ?", (user_id,))
    logs = [dict(r) for r in log_rows]

    setting_rows = await db.fetchall("SELECT * FROM user_settings WHERE user_id = ?", (user_id,))
    settings = {r["key"]: r["value"] for r in setting_rows}

    return {
        "user": user,
        "addictions": addictions,
        "daily_logs": logs,
        "settings": settings,
        "exported_at": datetime.now().isoformat(),
    }



async def backup_database_copy() -> str:
    """Create a consistent backup copy of the database and return its path.

    Uses SQLite backup API (safe with WAL mode).
    """
    await db.connect()
    tmp_dir = tempfile.mkdtemp(prefix="db_export_")
    dst = os.path.join(tmp_dir, "database_export.sqlite")

    async with db.lock:
        if db.conn is None:
            raise RuntimeError("Database is not connected")

        await db.conn.commit()

        target = await aiosqlite.connect(dst)
        try:
            await db.conn.backup(target)
            await target.commit()
        finally:
            await target.close()

    return dst


# =============================================================================
# KEYBOARD BUILDERS
# =============================================================================

def build_main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    """Build main menu keyboard."""
    buttons = [
        [InlineKeyboardButton(text="Ежедневный отчёт", callback_data="menu:daily_report")],
        [InlineKeyboardButton(text="Мой прогресс", callback_data="menu:progress")],
        [InlineKeyboardButton(text="План на сегодня", callback_data="menu:plan")],
        [InlineKeyboardButton(text="Инструменты", callback_data="menu:tools")],
        [InlineKeyboardButton(text="Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton(text="Экстренная помощь", callback_data="menu:emergency")],
    ]
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="Админ-панель", callback_data="menu:admin")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_welcome_keyboard() -> InlineKeyboardMarkup:
    """Build welcome screen keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Продолжить", callback_data="onboard:continue")],
        [InlineKeyboardButton(text="О конфиденциальности", callback_data="onboard:privacy")],
        [InlineKeyboardButton(text="Экстренная помощь", callback_data="menu:emergency")],
    ])


def build_addiction_selection_keyboard(selected: list) -> InlineKeyboardMarkup:
    """Build addiction selection keyboard with checkmarks."""
    buttons = []
    for code, name in ADDICTION_TYPES.items():
        mark = "●" if code in selected else "○"
        buttons.append([
            InlineKeyboardButton(
                text=f"{mark} {name}",
                callback_data=f"addiction:toggle:{code}"
            )
        ])
    
    buttons.append([
        InlineKeyboardButton(text="Назад", callback_data="onboard:back"),
        InlineKeyboardButton(text="Готово", callback_data="addiction:done")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_time_selection_keyboard() -> InlineKeyboardMarkup:
    """Build reminder time selection keyboard."""
    buttons = []
    row = []
    for i, time_str in enumerate(REMINDER_TIMES):
        row.append(InlineKeyboardButton(text=time_str, callback_data=f"time:{time_str}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="time:back")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_daily_report_keyboard(addiction_name: str) -> InlineKeyboardMarkup:
    """Build daily report status selection keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Без срыва", callback_data="report:status:clean")],
        [InlineKeyboardButton(text="Срыв", callback_data="report:status:relapse")],
        [InlineKeyboardButton(text="Сложно сказать", callback_data="report:status:unclear")],
        [InlineKeyboardButton(text="Отмена", callback_data="report:cancel")],
    ])


def build_craving_keyboard() -> InlineKeyboardMarkup:
    """Build craving level selection keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Низкий", callback_data="report:craving:low"),
            InlineKeyboardButton(text="Средний", callback_data="report:craving:medium"),
            InlineKeyboardButton(text="Высокий", callback_data="report:craving:high"),
        ],
        [InlineKeyboardButton(text="Пропустить", callback_data="report:craving:skip")],
    ])


def build_need_support_keyboard() -> InlineKeyboardMarkup:
    """Build support needed keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Да", callback_data="report:support:yes"),
            InlineKeyboardButton(text="Нет", callback_data="report:support:no"),
        ],
    ])


def build_report_summary_keyboard() -> InlineKeyboardMarkup:
    """Build report summary keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить", callback_data="report:edit")],
        [InlineKeyboardButton(text="К истории", callback_data="menu:progress")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_relapse_support_keyboard() -> InlineKeyboardMarkup:
    """Build relapse support keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Экстренная поддержка", callback_data="menu:emergency")],
        [InlineKeyboardButton(text="Дыхание 2 минуты", callback_data="tool:breathing")],
        [InlineKeyboardButton(text="Продолжить отчёт", callback_data="report:continue")],
    ])


def build_emergency_keyboard() -> InlineKeyboardMarkup:
    """Build emergency help keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Дыхание 2 минуты", callback_data="tool:breathing")],
        [InlineKeyboardButton(text="План на 10 минут", callback_data="tool:ten_minutes")],
        [InlineKeyboardButton(text="Пауза 90 секунд", callback_data="tool:pause")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_progress_keyboard() -> InlineKeyboardMarkup:
    """Build progress menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Последние 7 дней", callback_data="progress:7days")],
        [InlineKeyboardButton(text="Серии без срыва", callback_data="progress:streaks")],
        [InlineKeyboardButton(text="Календарь (14 дней)", callback_data="progress:calendar")],
        [InlineKeyboardButton(text="Экспорт данных", callback_data="progress:export")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_plan_keyboard() -> InlineKeyboardMarkup:
    """Build daily plan keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Цель на день", callback_data="plan:goal")],
        [InlineKeyboardButton(text="Если тянет", callback_data="plan:coping")],
        [InlineKeyboardButton(text="Мои триггеры", callback_data="plan:triggers")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_goal_selection_keyboard(selected: str = None) -> InlineKeyboardMarkup:
    """Build goal selection keyboard."""
    buttons = []
    for goal in DAILY_GOALS:
        mark = "●" if goal == selected else "○"
        buttons.append([
            InlineKeyboardButton(
                text=f"{mark} {goal}",
                callback_data=f"goal:select:{DAILY_GOALS.index(goal)}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="menu:plan")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_coping_keyboard() -> InlineKeyboardMarkup:
    """Build coping strategies keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Дыхание 2 минуты", callback_data="tool:breathing")],
        [InlineKeyboardButton(text="План на 10 минут", callback_data="tool:ten_minutes")],
        [InlineKeyboardButton(text="Переключение внимания", callback_data="tool:distraction")],
        [InlineKeyboardButton(text="Мои причины", callback_data="tool:reasons")],
        [InlineKeyboardButton(text="Назад", callback_data="menu:plan")],
    ])


def build_triggers_keyboard(selected: list) -> InlineKeyboardMarkup:
    """Build triggers selection keyboard."""
    buttons = []
    for i, trigger in enumerate(COMMON_TRIGGERS):
        mark = "●" if str(i) in selected else "○"
        buttons.append([
            InlineKeyboardButton(
                text=f"{mark} {trigger}",
                callback_data=f"trigger:toggle:{i}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="Сохранить", callback_data="trigger:save"),
        InlineKeyboardButton(text="Назад", callback_data="menu:plan")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_tools_keyboard() -> InlineKeyboardMarkup:
    """Build tools menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Дыхание", callback_data="tool:breathing")],
        [InlineKeyboardButton(text="Пауза 90 секунд", callback_data="tool:pause")],
        [InlineKeyboardButton(text="Когнитивная переоценка", callback_data="tool:cognitive")],
        [InlineKeyboardButton(text="Список причин", callback_data="tool:reasons")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_reasons_keyboard(selected: list) -> InlineKeyboardMarkup:
    """Build reasons selection keyboard."""
    buttons = []
    for i, reason in enumerate(REASONS_LIST):
        mark = "●" if str(i) in selected else "○"
        buttons.append([
            InlineKeyboardButton(
                text=f"{mark} {reason}",
                callback_data=f"reason:toggle:{i}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="Сохранить", callback_data="reason:save"),
        InlineKeyboardButton(text="Назад", callback_data="menu:tools")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_settings_keyboard() -> InlineKeyboardMarkup:
    """Build settings menu keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои зависимости", callback_data="settings:addictions")],
        [InlineKeyboardButton(text="Время напоминаний", callback_data="settings:reminder_time")],
        [InlineKeyboardButton(text="Уведомления поддержки", callback_data="settings:support")],
        [InlineKeyboardButton(text="Удалить все данные", callback_data="settings:delete")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_support_settings_keyboard(enabled: bool, frequency: int) -> InlineKeyboardMarkup:
    """Build support notification settings keyboard."""
    status = "Включены" if enabled else "Выключены"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"Уведомления: {status}",
            callback_data="settings:support:toggle"
        )],
        [
            InlineKeyboardButton(
                text=f"{'●' if frequency == 1 else '○'} 1 раз/день",
                callback_data="settings:support:freq:1"
            ),
            InlineKeyboardButton(
                text=f"{'●' if frequency == 2 else '○'} 2 раза/день",
                callback_data="settings:support:freq:2"
            ),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="menu:settings")],
    ])


def build_delete_confirm_keyboard() -> InlineKeyboardMarkup:
    """Build delete confirmation keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Да, удалить", callback_data="settings:delete:confirm"),
            InlineKeyboardButton(text="Отмена", callback_data="menu:settings"),
        ],
    ])


def build_back_keyboard(callback_data: str) -> InlineKeyboardMarkup:
    """Build simple back button keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=callback_data)],
    ])


def build_admin_keyboard() -> InlineKeyboardMarkup:
    """Build admin panel keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="Выгрузить базу", callback_data="admin:export")],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="Шаблоны уведомлений", callback_data="admin:templates")],
        [InlineKeyboardButton(text="Диагностика планировщика", callback_data="admin:scheduler")],
        [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
    ])


def build_templates_keyboard(templates: list, page: int = 0) -> InlineKeyboardMarkup:
    """Build templates management keyboard."""
    buttons = []
    items_per_page = 5
    start = page * items_per_page
    end = start + items_per_page
    
    for template in templates[start:end]:
        status = "●" if template["is_active"] else "○"
        text = template["text"][:30] + "..." if len(template["text"]) > 30 else template["text"]
        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {text}",
                callback_data=f"template:toggle:{template['id']}"
            )
        ])
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀", callback_data=f"template:page:{page-1}"))
    if end < len(templates):
        nav_buttons.append(InlineKeyboardButton(text="▶", callback_data=f"template:page:{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton(text="Добавить шаблон", callback_data="template:add")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="menu:admin")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_broadcast_confirm_keyboard() -> InlineKeyboardMarkup:
    """Build broadcast confirmation keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Отправить", callback_data="broadcast:confirm"),
            InlineKeyboardButton(text="Отмена", callback_data="menu:admin"),
        ],
    ])


# =============================================================================
# ANTIFLOOD MIDDLEWARE
# =============================================================================

class AntiFloodMiddleware:
    """Simple anti-flood middleware for callbacks."""
    
    def __init__(self, delay: float = ANTIFLOOD_DELAY):
        self.delay = delay
        self.last_callback: dict[int, float] = {}
    
    def check(self, user_id: int) -> bool:
        """Returns True if callback should be processed, False if too fast."""
        now = time.time()
        last = self.last_callback.get(user_id, 0)
        
        if now - last < self.delay:
            return False
        
        self.last_callback[user_id] = now
        return True


antiflood = AntiFloodMiddleware()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def is_admin(user_id: int) -> bool:
    """Check if user is admin."""
    return user_id == ADMIN_USER_ID



async def get_user_date(user_id: int) -> str:
    """Get current date for user in their timezone."""
    tz_str = await get_user_timezone(user_id)
    try:
        tz = ZoneInfo(tz_str)
    except Exception:
        tz = ZoneInfo(DEFAULT_TIMEZONE)
    return datetime.now(tz).strftime("%Y-%m-%d")



async def get_user_now(user_id: int) -> datetime:
    """Current datetime in user's timezone."""
    tz_str = await get_user_timezone(user_id)
    try:
        tz = ZoneInfo(tz_str)
    except Exception:
        tz = ZoneInfo(DEFAULT_TIMEZONE)
    return datetime.now(tz)


def format_streak_text(addiction_code: str, streak: int) -> str:
    """Format streak text."""
    name = ADDICTION_TYPES.get(addiction_code, addiction_code)
    if streak == 0:
        return f"{name}: начните сегодня"
    elif streak == 1:
        return f"{name}: 1 день"
    elif streak < 5:
        return f"{name}: {streak} дня"
    else:
        return f"{name}: {streak} дней"


def format_calendar(logs: list, addictions: list, days: int = 14, today=None) -> str:
    """Format calendar view."""
    if today is None:
        today = datetime.now(ZoneInfo(DEFAULT_TIMEZONE)).date()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    dates.reverse()
    
    logs_by_date = {}
    for log in logs:
        date = log["date"]
        if date not in logs_by_date:
            logs_by_date[date] = {}
        logs_by_date[date][log["addiction_code"]] = log["status"]
    
    lines = ["Последние 14 дней:", ""]
    for date in dates:
        day_str = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m")
        statuses = []
        for addiction in addictions:
            if date in logs_by_date and addiction in logs_by_date[date]:
                status = logs_by_date[date][addiction]
                if status == "clean":
                    statuses.append("•")
                elif status == "relapse":
                    statuses.append("×")
                else:
                    statuses.append("?")
            else:
                statuses.append("-")
        lines.append(f"{day_str}: {' '.join(statuses)}")
    
    lines.append("")
    lines.append("Обозначения: • чисто, × срыв, ? неясно, - нет данных")
    
    return "\n".join(lines)


# =============================================================================
# BOT SETUP
# =============================================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# =============================================================================
# COMMAND HANDLERS
# =============================================================================

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Handle /start command."""
    user = await get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name
    )
    
    await state.clear()
    
    if user["is_onboarded"]:
        # Show main menu
        await message.answer(
            TEXTS["main_menu"],
            reply_markup=build_main_menu_keyboard(is_admin(message.from_user.id))
        )
    else:
        # Show welcome preview
        await state.set_state(OnboardingStates.viewing_preview)
        await message.answer(
            TEXTS["welcome_preview"],
            reply_markup=build_welcome_keyboard()
        )


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    """Health check command."""
    await message.answer("Бот работает.")


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    """Admin panel command."""
    if not is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return
    
    await state.set_state(AdminStates.main)
    await message.answer(TEXTS["admin_menu"], reply_markup=build_admin_keyboard())


@router.message(Command("menu"))
async def cmd_menu(message: Message, state: FSMContext):
    """Show main menu."""
    user = await get_or_create_user(message.from_user.id)
    await state.clear()
    
    if user["is_onboarded"]:
        await message.answer(
            TEXTS["main_menu"],
            reply_markup=build_main_menu_keyboard(is_admin(message.from_user.id))
        )
    else:
        await state.set_state(OnboardingStates.viewing_preview)
        await message.answer(
            TEXTS["welcome_preview"],
            reply_markup=build_welcome_keyboard()
        )


# =============================================================================
# ONBOARDING HANDLERS
# =============================================================================

@router.callback_query(F.data == "onboard:continue")
async def onboard_continue(callback: CallbackQuery, state: FSMContext):
    """Continue to addiction selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(OnboardingStates.selecting_addictions)
    await state.update_data(selected_addictions=[])
    
    await callback.message.edit_text(
        TEXTS["select_addictions"],
        reply_markup=build_addiction_selection_keyboard([])
    )
    await callback.answer()


@router.callback_query(F.data == "onboard:privacy")
async def onboard_privacy(callback: CallbackQuery, state: FSMContext):
    """Show privacy info."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await callback.message.edit_text(
        TEXTS["privacy_info"],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="onboard:back_to_welcome")]
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "onboard:back_to_welcome")
async def onboard_back_to_welcome(callback: CallbackQuery, state: FSMContext):
    """Back to welcome screen."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(OnboardingStates.viewing_preview)
    await callback.message.edit_text(
        TEXTS["welcome_preview"],
        reply_markup=build_welcome_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "onboard:back")
async def onboard_back(callback: CallbackQuery, state: FSMContext):
    """Back from addiction selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(OnboardingStates.viewing_preview)
    await callback.message.edit_text(
        TEXTS["welcome_preview"],
        reply_markup=build_welcome_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data.startswith("addiction:toggle:"))
async def toggle_addiction_callback(callback: CallbackQuery, state: FSMContext):
    """Toggle addiction selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    code = callback.data.split(":")[-1]
    data = await state.get_data()
    selected = data.get("selected_addictions", [])
    
    if code in selected:
        selected.remove(code)
    else:
        selected.append(code)
    
    await state.update_data(selected_addictions=selected)
    
    await callback.message.edit_reply_markup(
        reply_markup=build_addiction_selection_keyboard(selected)
    )
    await callback.answer()


@router.callback_query(F.data == "addiction:done")
async def addiction_done(callback: CallbackQuery, state: FSMContext):
    """Complete addiction selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    data = await state.get_data()
    selected = data.get("selected_addictions", [])
    
    if not selected:
        await callback.answer("Выберите хотя бы одну зависимость для отслеживания.")
        return
    
    # Save to database
    user_id = callback.from_user.id
    await clear_user_addictions(user_id)
    for code in selected:
        await toggle_user_addiction(user_id, code)
    
    await state.set_state(OnboardingStates.selecting_time)
    await callback.message.edit_text(
        TEXTS["select_reminder_time"],
        reply_markup=build_time_selection_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data.startswith("time:"))
async def select_time_callback(callback: CallbackQuery, state: FSMContext):
    """Handle time selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    action = callback.data.split(":", 1)[-1]
    
    if action == "back":
        data = await state.get_data()
        selected = data.get("selected_addictions", [])
        await state.set_state(OnboardingStates.selecting_addictions)
        await callback.message.edit_text(
            TEXTS["select_addictions"],
            reply_markup=build_addiction_selection_keyboard(selected)
        )
        await callback.answer()
        return
    
    # It's a time value
    time_str = action
    user_id = callback.from_user.id
    
    await set_user_reminder_time(user_id, time_str)
    await set_user_onboarded(user_id, True)
    
    await state.clear()
    await callback.message.edit_text(
        TEXTS["onboarding_complete"],
        reply_markup=build_main_menu_keyboard(is_admin(user_id))
    )
    await callback.answer()


# =============================================================================
# MAIN MENU HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:main")
async def menu_main(callback: CallbackQuery, state: FSMContext):
    """Show main menu."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.clear()
    try:
        await callback.message.edit_text(
            TEXTS["main_menu"],
            reply_markup=build_main_menu_keyboard(is_admin(callback.from_user.id))
        )
    except TelegramBadRequest:
        await callback.message.answer(
            TEXTS["main_menu"],
            reply_markup=build_main_menu_keyboard(is_admin(callback.from_user.id))
        )
    await callback.answer()


@router.callback_query(F.data == "menu:emergency")
async def menu_emergency(callback: CallbackQuery, state: FSMContext):
    """Show emergency help."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    try:
        await callback.message.edit_text(
            TEXTS["emergency_help"],
            reply_markup=build_emergency_keyboard()
        )
    except TelegramBadRequest:
        await callback.message.answer(
            TEXTS["emergency_help"],
            reply_markup=build_emergency_keyboard()
        )
    await callback.answer()


# =============================================================================
# DAILY REPORT HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:daily_report")
async def menu_daily_report(callback: CallbackQuery, state: FSMContext):
    """Start daily report."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    today = await get_user_date(user_id)
    addictions = await get_user_addictions(user_id)
    
    if not addictions:
        await callback.answer("Сначала выберите зависимости в настройках.")
        return
    
    today_logs = await get_today_logs(user_id, today)
    
    # Check if all addictions logged
    if all(a in today_logs for a in addictions):
        # Show summary
        summary_lines = [TEXTS["report_already_filled"], ""]
        for code in addictions:
            name = ADDICTION_TYPES.get(code, code)
            log = today_logs.get(code, {})
            status = log.get("status", "")
            status_text = {"clean": "без срыва", "relapse": "срыв", "unclear": "неясно"}.get(status, "-")
            craving = log.get("craving_level", "")
            craving_text = {"low": "низкая", "medium": "средняя", "high": "высокая"}.get(craving, "-")
            summary_lines.append(f"{name}: {status_text}, тяга: {craving_text}")
        
        await callback.message.edit_text(
            "\n".join(summary_lines),
            reply_markup=build_report_summary_keyboard()
        )
        await callback.answer()
        return
    
    # Start report flow
    await state.set_state(DailyReportStates.answering_addiction)
    await state.update_data(
        report_date=today,
        addictions=addictions,
        current_index=0,
        logs={}
    )
    
    first_addiction = addictions[0]
    await callback.message.edit_text(
        TEXTS["daily_report_question"].format(addiction=ADDICTION_TYPES.get(first_addiction, first_addiction)),
        reply_markup=build_daily_report_keyboard(first_addiction)
    )
    await callback.answer()


@router.callback_query(F.data == "report:edit")
async def report_edit(callback: CallbackQuery, state: FSMContext):
    """Edit today's report."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    today = await get_user_date(user_id)
    addictions = await get_user_addictions(user_id)
    
    if not addictions:
        await callback.answer("Нет выбранных зависимостей.")
        return
    
    await state.set_state(DailyReportStates.answering_addiction)
    await state.update_data(
        report_date=today,
        addictions=addictions,
        current_index=0,
        logs={}
    )
    
    first_addiction = addictions[0]
    await callback.message.edit_text(
        TEXTS["daily_report_question"].format(addiction=ADDICTION_TYPES.get(first_addiction, first_addiction)),
        reply_markup=build_daily_report_keyboard(first_addiction)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("report:status:"))
async def report_status(callback: CallbackQuery, state: FSMContext):
    """Handle addiction status answer."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    status = callback.data.split(":")[-1]
    data = await state.get_data()
    
    addictions = data.get("addictions", [])
    current_index = data.get("current_index", 0)
    logs = data.get("logs", {})
    
    if current_index >= len(addictions):
        await callback.answer("Ошибка состояния")
        return
    
    current_addiction = addictions[current_index]
    logs[current_addiction] = {"status": status}
    
    # Check for relapse
    if status == "relapse":
        await state.update_data(logs=logs, pending_relapse=True)
        await callback.message.edit_text(
            TEXTS["relapse_support"],
            reply_markup=build_relapse_support_keyboard()
        )
        await callback.answer()
        return
    
    # Move to next addiction or craving
    next_index = current_index + 1
    
    if next_index < len(addictions):
        await state.update_data(logs=logs, current_index=next_index)
        next_addiction = addictions[next_index]
        await callback.message.edit_text(
            TEXTS["daily_report_question"].format(addiction=ADDICTION_TYPES.get(next_addiction, next_addiction)),
            reply_markup=build_daily_report_keyboard(next_addiction)
        )
    else:
        # All addictions answered, ask craving level
        await state.set_state(DailyReportStates.answering_craving)
        await state.update_data(logs=logs)
        await callback.message.edit_text(
            TEXTS["craving_question"],
            reply_markup=build_craving_keyboard()
        )
    
    await callback.answer()


@router.callback_query(F.data == "report:continue")
async def report_continue(callback: CallbackQuery, state: FSMContext):
    """Continue report after relapse acknowledgment."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    data = await state.get_data()
    addictions = data.get("addictions", [])
    current_index = data.get("current_index", 0)
    logs = data.get("logs", {})
    
    next_index = current_index + 1
    
    if next_index < len(addictions):
        await state.update_data(current_index=next_index, pending_relapse=False)
        await state.set_state(DailyReportStates.answering_addiction)
        next_addiction = addictions[next_index]
        await callback.message.edit_text(
            TEXTS["daily_report_question"].format(addiction=ADDICTION_TYPES.get(next_addiction, next_addiction)),
            reply_markup=build_daily_report_keyboard(next_addiction)
        )
    else:
        await state.set_state(DailyReportStates.answering_craving)
        await callback.message.edit_text(
            TEXTS["craving_question"],
            reply_markup=build_craving_keyboard()
        )
    
    await callback.answer()


@router.callback_query(F.data.startswith("report:craving:"))
async def report_craving(callback: CallbackQuery, state: FSMContext):
    """Handle craving level answer."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    craving = callback.data.split(":")[-1]
    data = await state.get_data()
    logs = data.get("logs", {})
    
    # Apply craving to all logs
    craving_value = craving if craving != "skip" else None
    for code in logs:
        logs[code]["craving_level"] = craving_value
    
    await state.update_data(logs=logs)
    await state.set_state(DailyReportStates.answering_support)
    
    await callback.message.edit_text(
        TEXTS["need_support_question"],
        reply_markup=build_need_support_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data.startswith("report:support:"))
async def report_support(callback: CallbackQuery, state: FSMContext):
    """Handle support need answer and save report."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    needs_support = callback.data.split(":")[-1] == "yes"
    data = await state.get_data()
    logs = data.get("logs", {})
    report_date = data.get("report_date")
    user_id = callback.from_user.id
    
    # Save all logs
    for code, log_data in logs.items():
        await upsert_daily_log(
            user_id,
            report_date,
            code,
            log_data.get("status"),
            log_data.get("craving_level")
        )
    
    await state.clear()
    
    if needs_support:
        await callback.message.edit_text(
            TEXTS["emergency_help"],
            reply_markup=build_emergency_keyboard()
        )
    else:
        await callback.message.edit_text(
            TEXTS["report_saved"],
            reply_markup=build_main_menu_keyboard(is_admin(user_id))
        )
    
    await callback.answer()


@router.callback_query(F.data == "report:cancel")
async def report_cancel(callback: CallbackQuery, state: FSMContext):
    """Cancel report."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.clear()
    await callback.message.edit_text(
        TEXTS["main_menu"],
        reply_markup=build_main_menu_keyboard(is_admin(callback.from_user.id))
    )
    await callback.answer()


# =============================================================================
# PROGRESS HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:progress")
async def menu_progress(callback: CallbackQuery, state: FSMContext):
    """Show progress menu."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(ProgressStates.viewing)
    await callback.message.edit_text(
        TEXTS["progress_title"],
        reply_markup=build_progress_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "progress:7days")
async def progress_7days(callback: CallbackQuery, state: FSMContext):
    """Show last 7 days stats."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    today = (await get_user_now(user_id)).date()
    week_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    
    addictions = await get_user_addictions(user_id)
    logs = await get_logs_for_period(user_id, week_ago, today_str)
    
    if not logs:
        text = TEXTS["no_data"]
    else:
        lines = ["Последние 7 дней:", ""]
        
        # Count by addiction
        stats = {}
        for code in addictions:
            stats[code] = {"clean": 0, "relapse": 0, "unclear": 0}
        
        for log in logs:
            code = log["addiction_code"]
            status = log["status"]
            if code in stats and status in stats[code]:
                stats[code][status] += 1
        
        for code, counts in stats.items():
            name = ADDICTION_TYPES.get(code, code)
            lines.append(f"{name}:")
            lines.append(f"  Чисто: {counts['clean']} дней")
            lines.append(f"  Срывы: {counts['relapse']}")
            lines.append(f"  Неясно: {counts['unclear']}")
            lines.append("")
        
        text = "\n".join(lines)
    
    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:progress")
    )
    await callback.answer()


@router.callback_query(F.data == "progress:streaks")
async def progress_streaks(callback: CallbackQuery, state: FSMContext):
    """Show current streaks."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    addictions = await get_user_addictions(user_id)
    
    if not addictions:
        text = TEXTS["no_data"]
    else:
        lines = ["Серии без срыва:", ""]
        for code in addictions:
            streak = await get_streak(user_id, code)
            lines.append(format_streak_text(code, streak))
        text = "\n".join(lines)
    
    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:progress")
    )
    await callback.answer()


@router.callback_query(F.data == "progress:calendar")
async def progress_calendar(callback: CallbackQuery, state: FSMContext):
    """Show calendar view."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    today = (await get_user_now(user_id)).date()
    two_weeks_ago = (today - timedelta(days=14)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    
    addictions = await get_user_addictions(user_id)
    logs = await get_logs_for_period(user_id, two_weeks_ago, today_str)
    
    if not addictions:
        text = TEXTS["no_data"]
    else:
        text = format_calendar(logs, addictions)
    
    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:progress")
    )
    await callback.answer()


@router.callback_query(F.data == "progress:export")
async def progress_export(callback: CallbackQuery, state: FSMContext):
    """Export user data."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    data = await export_user_data(user_id)
    
    json_content = json.dumps(data, ensure_ascii=False, indent=2)
    file = BufferedInputFile(
        json_content.encode("utf-8"),
        filename=f"my_data_{user_id}.json"
    )
    
    await callback.message.answer_document(file, caption="Ваши данные")
    await callback.answer()


# =============================================================================
# PLAN HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:plan")
async def menu_plan(callback: CallbackQuery, state: FSMContext):
    """Show daily plan menu."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(PlanStates.main)
    await callback.message.edit_text(
        TEXTS["plan_title"],
        reply_markup=build_plan_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "plan:goal")
async def plan_goal(callback: CallbackQuery, state: FSMContext):
    """Select daily goal."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    current_goal = await get_user_setting(user_id, "daily_goal")
    
    await state.set_state(PlanStates.selecting_goal)
    await callback.message.edit_text(
        "Выберите цель на сегодня:",
        reply_markup=build_goal_selection_keyboard(current_goal)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("goal:select:"))
async def goal_select(callback: CallbackQuery, state: FSMContext):
    """Handle goal selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    index = int(callback.data.split(":")[-1])
    goal = DAILY_GOALS[index] if 0 <= index < len(DAILY_GOALS) else None
    
    if goal:
        await set_user_setting(callback.from_user.id, "daily_goal", goal)
        await callback.message.edit_text(
            f"Цель на сегодня установлена:\n\n{goal}",
            reply_markup=build_back_keyboard("menu:plan")
        )
    
    await callback.answer()


@router.callback_query(F.data == "plan:coping")
async def plan_coping(callback: CallbackQuery, state: FSMContext):
    """Show coping strategies."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await callback.message.edit_text(
        "Что делать, если тянет:",
        reply_markup=build_coping_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "plan:triggers")
async def plan_triggers(callback: CallbackQuery, state: FSMContext):
    """Select personal triggers."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    saved = await get_user_setting(user_id, "triggers")
    selected = saved.split(",") if saved else []
    
    await state.set_state(PlanStates.selecting_triggers)
    await state.update_data(selected_triggers=selected)
    
    await callback.message.edit_text(
        "Отметьте ваши актуальные триггеры:",
        reply_markup=build_triggers_keyboard(selected)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("trigger:toggle:"))
async def trigger_toggle(callback: CallbackQuery, state: FSMContext):
    """Toggle trigger selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    index = callback.data.split(":")[-1]
    data = await state.get_data()
    selected = data.get("selected_triggers", [])
    
    if index in selected:
        selected.remove(index)
    else:
        selected.append(index)
    
    await state.update_data(selected_triggers=selected)
    await callback.message.edit_reply_markup(reply_markup=build_triggers_keyboard(selected))
    await callback.answer()


@router.callback_query(F.data == "trigger:save")
async def trigger_save(callback: CallbackQuery, state: FSMContext):
    """Save selected triggers."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    data = await state.get_data()
    selected = data.get("selected_triggers", [])
    
    await set_user_setting(callback.from_user.id, "triggers", ",".join(selected))
    
    await state.set_state(PlanStates.main)
    await callback.message.edit_text(
        "Триггеры сохранены.",
        reply_markup=build_back_keyboard("menu:plan")
    )
    await callback.answer()


# =============================================================================
# TOOLS HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:tools")
async def menu_tools(callback: CallbackQuery, state: FSMContext):
    """Show tools menu."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(ToolsStates.main)
    await callback.message.edit_text(
        TEXTS["tools_title"],
        reply_markup=build_tools_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "tool:breathing")
async def tool_breathing(callback: CallbackQuery, state: FSMContext):
    """Show breathing exercise."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await callback.message.edit_text(
        TEXTS["breathing_exercise"],
        reply_markup=build_back_keyboard("menu:tools")
    )
    await callback.answer()


@router.callback_query(F.data == "tool:pause")
async def tool_pause(callback: CallbackQuery, state: FSMContext):
    """Show 90 second pause."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await callback.message.edit_text(
        TEXTS["pause_90_seconds"],
        reply_markup=build_back_keyboard("menu:tools")
    )
    await callback.answer()


@router.callback_query(F.data == "tool:ten_minutes")
async def tool_ten_minutes(callback: CallbackQuery, state: FSMContext):
    """Show 10 minute plan."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await callback.message.edit_text(
        TEXTS["ten_minute_plan"],
        reply_markup=build_back_keyboard("menu:tools")
    )
    await callback.answer()


@router.callback_query(F.data == "tool:cognitive")
async def tool_cognitive(callback: CallbackQuery, state: FSMContext):
    """Show cognitive reframe."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await callback.message.edit_text(
        TEXTS["cognitive_reframe"],
        reply_markup=build_back_keyboard("menu:tools")
    )
    await callback.answer()


@router.callback_query(F.data == "tool:distraction")
async def tool_distraction(callback: CallbackQuery, state: FSMContext):
    """Show distraction tips."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    text = (
        "Переключение внимания\n\n"
        "Быстрые способы переключиться:\n"
        "• Выйдите из помещения на 5 минут\n"
        "• Умойтесь холодной водой\n"
        "• Позвоните кому-нибудь\n"
        "• Включите музыку\n"
        "• Сделайте 20 приседаний\n"
        "• Напишите список дел на завтра"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:tools")
    )
    await callback.answer()


@router.callback_query(F.data == "tool:reasons")
async def tool_reasons(callback: CallbackQuery, state: FSMContext):
    """Show reasons selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    saved = await get_user_setting(user_id, "reasons")
    selected = saved.split(",") if saved else []
    
    await state.set_state(ToolsStates.selecting_reasons)
    await state.update_data(selected_reasons=selected)
    
    await callback.message.edit_text(
        "Выберите причины, ради которых вы работаете над собой:",
        reply_markup=build_reasons_keyboard(selected)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("reason:toggle:"))
async def reason_toggle(callback: CallbackQuery, state: FSMContext):
    """Toggle reason selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    index = callback.data.split(":")[-1]
    data = await state.get_data()
    selected = data.get("selected_reasons", [])
    
    if index in selected:
        selected.remove(index)
    else:
        selected.append(index)
    
    await state.update_data(selected_reasons=selected)
    await callback.message.edit_reply_markup(reply_markup=build_reasons_keyboard(selected))
    await callback.answer()


@router.callback_query(F.data == "reason:save")
async def reason_save(callback: CallbackQuery, state: FSMContext):
    """Save selected reasons."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    data = await state.get_data()
    selected = data.get("selected_reasons", [])
    
    await set_user_setting(callback.from_user.id, "reasons", ",".join(selected))
    
    # Show saved reasons
    if selected:
        reasons_text = "\n".join([f"• {REASONS_LIST[int(i)]}" for i in selected if i.isdigit() and int(i) < len(REASONS_LIST)])
        text = f"Ваши причины:\n\n{reasons_text}"
    else:
        text = "Причины сохранены."
    
    await state.set_state(ToolsStates.main)
    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:tools")
    )
    await callback.answer()


# =============================================================================
# SETTINGS HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:settings")
async def menu_settings(callback: CallbackQuery, state: FSMContext):
    """Show settings menu."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(SettingsStates.main)
    await callback.message.edit_text(
        TEXTS["settings_title"],
        reply_markup=build_settings_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "settings:addictions")
async def settings_addictions(callback: CallbackQuery, state: FSMContext):
    """Change addiction selection."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    selected = await get_user_addictions(user_id)
    
    await state.set_state(SettingsStates.changing_addictions)
    await state.update_data(selected_addictions=selected)
    
    await callback.message.edit_text(
        TEXTS["select_addictions"],
        reply_markup=build_addiction_selection_keyboard(selected)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("addiction:toggle:"), StateFilter(SettingsStates.changing_addictions))
async def settings_toggle_addiction(callback: CallbackQuery, state: FSMContext):
    """Toggle addiction in settings."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    code = callback.data.split(":")[-1]
    data = await state.get_data()
    selected = data.get("selected_addictions", [])
    
    if code in selected:
        selected.remove(code)
    else:
        selected.append(code)
    
    await state.update_data(selected_addictions=selected)
    await callback.message.edit_reply_markup(reply_markup=build_addiction_selection_keyboard(selected))
    await callback.answer()


@router.callback_query(F.data == "addiction:done", StateFilter(SettingsStates.changing_addictions))
async def settings_addiction_done(callback: CallbackQuery, state: FSMContext):
    """Save addiction selection in settings."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    data = await state.get_data()
    selected = data.get("selected_addictions", [])
    
    if not selected:
        await callback.answer("Выберите хотя бы одну зависимость.")
        return
    
    user_id = callback.from_user.id
    await clear_user_addictions(user_id)
    for code in selected:
        await toggle_user_addiction(user_id, code)
    
    await state.set_state(SettingsStates.main)
    await callback.message.edit_text(
        "Выбор сохранён.\n\n" + TEXTS["settings_title"],
        reply_markup=build_settings_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "onboard:back", StateFilter(SettingsStates.changing_addictions))
async def settings_addiction_back(callback: CallbackQuery, state: FSMContext):
    """Back from addiction selection in settings."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(SettingsStates.main)
    await callback.message.edit_text(
        TEXTS["settings_title"],
        reply_markup=build_settings_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "settings:reminder_time")
async def settings_reminder_time(callback: CallbackQuery, state: FSMContext):
    """Change reminder time."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(SettingsStates.changing_time)
    await callback.message.edit_text(
        TEXTS["select_reminder_time"],
        reply_markup=build_time_selection_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data.startswith("time:"), StateFilter(SettingsStates.changing_time))
async def settings_time_select(callback: CallbackQuery, state: FSMContext):
    """Handle time selection in settings."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    action = callback.data.split(":", 1)[-1]
    
    if action == "back":
        await state.set_state(SettingsStates.main)
        await callback.message.edit_text(
            TEXTS["settings_title"],
            reply_markup=build_settings_keyboard()
        )
        await callback.answer()
        return
    
    time_str = action
    await set_user_reminder_time(callback.from_user.id, time_str)
    
    await state.set_state(SettingsStates.main)
    await callback.message.edit_text(
        f"Время напоминаний установлено: {time_str}\n\n" + TEXTS["settings_title"],
        reply_markup=build_settings_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "settings:support")
async def settings_support(callback: CallbackQuery, state: FSMContext):
    """Support notification settings."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    row = await db.fetchone(
        "SELECT support_enabled, support_frequency FROM users WHERE user_id = ?",
        (user_id,),
    )

    enabled = bool(row["support_enabled"]) if row else True
    frequency = int(row["support_frequency"]) if row else 1
    
    await callback.message.edit_text(
        "Настройки уведомлений поддержки:",
        reply_markup=build_support_settings_keyboard(enabled, frequency)
    )
    await callback.answer()


@router.callback_query(F.data == "settings:support:toggle")
async def settings_support_toggle(callback: CallbackQuery, state: FSMContext):
    """Toggle support notifications."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    row = await db.fetchone(
        "SELECT support_enabled, support_frequency FROM users WHERE user_id = ?",
        (user_id,),
    )

    enabled = (not bool(row["support_enabled"])) if row else False
    frequency = int(row["support_frequency"]) if row else 1

    await set_user_support_settings(user_id, enabled=enabled)
    
    await callback.message.edit_reply_markup(
        reply_markup=build_support_settings_keyboard(enabled, frequency)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("settings:support:freq:"))
async def settings_support_frequency(callback: CallbackQuery, state: FSMContext):
    """Set support frequency."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    frequency = int(callback.data.split(":")[-1])
    user_id = callback.from_user.id
    
    await set_user_support_settings(user_id, frequency=frequency)

    row = await db.fetchone(
        "SELECT support_enabled FROM users WHERE user_id = ?",
        (user_id,),
    )

    enabled = bool(row["support_enabled"]) if row else True
    
    await callback.message.edit_reply_markup(
        reply_markup=build_support_settings_keyboard(enabled, frequency)
    )
    await callback.answer()


@router.callback_query(F.data == "settings:delete")
async def settings_delete(callback: CallbackQuery, state: FSMContext):
    """Confirm data deletion."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(SettingsStates.confirming_delete)
    await callback.message.edit_text(
        TEXTS["delete_confirm"],
        reply_markup=build_delete_confirm_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "settings:delete:confirm")
async def settings_delete_confirm(callback: CallbackQuery, state: FSMContext):
    """Execute data deletion."""
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await delete_user_data(callback.from_user.id)
    await state.clear()
    
    await callback.message.edit_text(
        TEXTS["data_deleted"] + "\n\nДля начала работы используйте /start",
        reply_markup=None
    )
    await callback.answer()


# =============================================================================
# ADMIN HANDLERS
# =============================================================================

@router.callback_query(F.data == "menu:admin")
async def menu_admin(callback: CallbackQuery, state: FSMContext):
    """Show admin menu."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(AdminStates.main)
    await callback.message.edit_text(
        TEXTS["admin_menu"],
        reply_markup=build_admin_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "admin:stats")
async def admin_stats(callback: CallbackQuery, state: FSMContext):
    """Show admin statistics."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    stats = await get_admin_stats()
    text = (
        f"Статистика:\n\n"
        f"Всего пользователей: {stats['total_users']}\n"
        f"Активных за 7 дней: {stats['active_users_7d']}\n"
        f"Всего отчётов: {stats['total_logs']}\n"
        f"Отчётов за 7 дней: {stats['logs_7d']}"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:admin")
    )
    await callback.answer()


@router.callback_query(F.data == "admin:export")
async def admin_export(callback: CallbackQuery, state: FSMContext):
    """Export database for admin."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return

    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return

    tmp_path = None
    try:
        tmp_path = await backup_database_copy()
        file = FSInputFile(tmp_path, filename="database_export.sqlite")
        await callback.message.answer_document(file, caption="Экспорт базы данных")
        await callback.answer()
    except Exception as e:
        logger.error(f"Export error: {e}")
        await callback.answer("Ошибка при экспорте базы данных.")
    finally:
        if tmp_path:
            with suppress(Exception):
                shutil.rmtree(os.path.dirname(tmp_path), ignore_errors=True)


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    """Start broadcast flow."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(AdminStates.broadcast_text)
    await callback.message.edit_text(
        "Введите текст рассылки:",
        reply_markup=build_back_keyboard("menu:admin")
    )
    await callback.answer()


@router.message(StateFilter(AdminStates.broadcast_text))
async def admin_broadcast_text(message: Message, state: FSMContext):
    """Receive broadcast text."""
    if not is_admin(message.from_user.id):
        return
    
    text = message.text
    if not text:
        await message.answer("Пожалуйста, введите текст.")
        return
    
    await state.update_data(broadcast_text=text)
    await state.set_state(AdminStates.broadcast_confirm)
    
    users = await get_all_users()
    
    await message.answer(
        f"Предпросмотр рассылки:\n\n{text}\n\n"
        f"Будет отправлено {len(users)} пользователям.",
        reply_markup=build_broadcast_confirm_keyboard()
    )


@router.callback_query(F.data == "broadcast:confirm")
async def admin_broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    """Execute broadcast."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return

    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return

    data = await state.get_data()
    text = data.get("broadcast_text", "")

    if not text:
        await callback.answer("Текст рассылки пуст.")
        return

    users = await get_all_users()
    total = len(users)
    sent = 0
    errors = 0

    await callback.message.edit_text(f"Рассылка запущена...\n0/{total}")

    for i, user in enumerate(users, start=1):
        uid = int(user["user_id"])
        try:
            await bot.send_message(uid, text)
            sent += 1
        except TelegramRetryAfter as e:
            wait_s = int(getattr(e, "retry_after", 1)) + 1
            await asyncio.sleep(wait_s)
            try:
                await bot.send_message(uid, text)
                sent += 1
            except (TelegramForbiddenError, TelegramBadRequest) as e2:
                logger.warning(f"Broadcast error for user {uid}: {e2}")
                errors += 1
            except Exception as e2:
                logger.error(f"Broadcast unexpected error for user {uid}: {e2}")
                errors += 1
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"Broadcast error for user {uid}: {e}")
            errors += 1
        except TelegramNetworkError as e:
            logger.warning(f"Broadcast network error for user {uid}: {e}")
            errors += 1
        except Exception as e:
            logger.error(f"Broadcast unexpected error for user {uid}: {e}")
            errors += 1

        # Gentle rate limiting (Telegram global limits ~30 msg/sec)
        await asyncio.sleep(0.05)

        # Progress update (rare to avoid edit limits)
        if i % 250 == 0:
            with suppress(TelegramBadRequest):
                await callback.message.edit_text(
                    f"Рассылка: {i}/{total}\nУспешно: {sent}\nОшибки: {errors}"
                )

    await log_broadcast(text, sent, errors)

    await state.set_state(AdminStates.main)
    await callback.message.edit_text(
        TEXTS["broadcast_sent"].format(sent=sent, errors=errors),
        reply_markup=build_admin_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data == "admin:templates")
async def admin_templates(callback: CallbackQuery, state: FSMContext):
    """Show notification templates."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    templates = await get_notification_templates()
    await state.set_state(AdminStates.viewing_templates)
    await state.update_data(templates_page=0)
    
    await callback.message.edit_text(
        "Шаблоны уведомлений (● - активен, ○ - неактивен):",
        reply_markup=build_templates_keyboard(templates, 0)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("template:toggle:"))
async def admin_template_toggle(callback: CallbackQuery, state: FSMContext):
    """Toggle template status."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    template_id = int(callback.data.split(":")[-1])
    await toggle_template(template_id)
    
    data = await state.get_data()
    page = data.get("templates_page", 0)
    templates = await get_notification_templates()
    
    await callback.message.edit_reply_markup(
        reply_markup=build_templates_keyboard(templates, page)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("template:page:"))
async def admin_template_page(callback: CallbackQuery, state: FSMContext):
    """Change templates page."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    page = int(callback.data.split(":")[-1])
    await state.update_data(templates_page=page)
    
    templates = await get_notification_templates()
    
    await callback.message.edit_reply_markup(
        reply_markup=build_templates_keyboard(templates, page)
    )
    await callback.answer()


@router.callback_query(F.data == "template:add")
async def admin_template_add(callback: CallbackQuery, state: FSMContext):
    """Add new template."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return
    
    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return
    
    await state.set_state(AdminStates.adding_template)
    await callback.message.edit_text(
        "Введите текст нового шаблона уведомления:",
        reply_markup=build_back_keyboard("admin:templates")
    )
    await callback.answer()


@router.message(StateFilter(AdminStates.adding_template))
async def admin_template_text(message: Message, state: FSMContext):
    """Receive new template text."""
    if not is_admin(message.from_user.id):
        return
    
    text = message.text
    if not text:
        await message.answer("Пожалуйста, введите текст шаблона.")
        return
    
    await add_template(text)
    
    templates = await get_notification_templates()
    await state.set_state(AdminStates.viewing_templates)
    
    await message.answer(
        "Шаблон добавлен.\n\nШаблоны уведомлений:",
        reply_markup=build_templates_keyboard(templates, 0)
    )


@router.callback_query(F.data == "admin:scheduler")
async def admin_scheduler(callback: CallbackQuery, state: FSMContext):
    """Show scheduler diagnostics."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.")
        return

    if not antiflood.check(callback.from_user.id):
        await callback.answer()
        return

    users = await get_users_for_reminder()

    # Scheduler diagnostics
    job = None
    running = False
    next_run_str = "—"
    try:
        running = bool(getattr(scheduler, "running", False))
        job = scheduler.get_job("scheduler_tick") if scheduler else None
        next_run = getattr(job, "next_run_time", None)
        next_run_str = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "—"
    except Exception:
        pass

    # Approximate pending notifications (based on each user's local time)
    pending = 0
    enabled_users = 0

    for user in users:
        if not int(user.get("support_enabled", 1) or 1):
            continue

        enabled_users += 1

        tz_str = user.get("timezone") or DEFAULT_TIMEZONE
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = ZoneInfo(DEFAULT_TIMEZONE)

        now_u = datetime.now(tz)
        current_minutes = now_u.hour * 60 + now_u.minute

        reminder_time = user.get("reminder_time") or DEFAULT_REMINDER_TIME
        frequency = int(user.get("support_frequency", 1) or 1)

        for _, time_str in _support_times(reminder_time, frequency):
            target_minutes = _parse_hhmm(time_str)
            if target_minutes is None:
                continue
            if target_minutes > current_minutes:
                pending += 1

    now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
    text = (
        f"Диагностика планировщика:\n\n"
        f"Текущее время (бот): {now.strftime('%Y-%m-%d %H:%M:%S')} ({DEFAULT_TIMEZONE})\n"
        f"Планировщик: {'работает' if running else 'остановлен'}\n"
        f"Интервал тика: {SCHEDULER_TICK_SECONDS} сек.\n"
        f"Следующий запуск: {next_run_str}\n\n"
        f"Пользователей с включёнными уведомлениями: {enabled_users}\n"
        f"Ожидается уведомлений до конца дня (оценка): {pending}\n"
    )

    await callback.message.edit_text(
        text,
        reply_markup=build_back_keyboard("menu:admin")
    )
    await callback.answer()


# =============================================================================
# CATCH-ALL FOR UNKNOWN CALLBACKS
# =============================================================================

@router.callback_query()
async def unknown_callback(callback: CallbackQuery, state: FSMContext):
    """Handle unknown or outdated callbacks."""
    logger.warning(f"Unknown callback: {callback.data} from user {callback.from_user.id}")
    
    await state.clear()
    
    user = await get_or_create_user(callback.from_user.id)
    
    if user["is_onboarded"]:
        try:
            await callback.message.edit_text(
                "Состояние устарело. Возврат в главное меню.\n\n" + TEXTS["main_menu"],
                reply_markup=build_main_menu_keyboard(is_admin(callback.from_user.id))
            )
        except TelegramBadRequest:
            await callback.message.answer(
                TEXTS["main_menu"],
                reply_markup=build_main_menu_keyboard(is_admin(callback.from_user.id))
            )
    else:
        await callback.message.answer(
            "Используйте /start для начала работы."
        )
    
    await callback.answer()


# =============================================================================
# =============================================================================
# SCHEDULER
# =============================================================================

scheduler = AsyncIOScheduler(timezone=DEFAULT_TIMEZONE)


def _parse_hhmm(value: str) -> Optional[int]:
    try:
        h, m = value.split(":")
        hh = int(h)
        mm = int(m)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh * 60 + mm
        return None
    except Exception:
        return None


def _minutes_diff(a: int, b: int) -> int:
    d = abs(a - b)
    return min(d, 1440 - d)


def _support_times(reminder_time: str, frequency: int) -> List[Tuple[str, str]]:
    """Return list of (notification_type, time_str) for a user."""
    times: List[Tuple[str, str]] = []

    base = reminder_time or DEFAULT_REMINDER_TIME
    times.append(("reminder", base))

    if int(frequency or 1) >= 2:
        extra = "12:00" if base != "12:00" else "18:00"
        if extra != base:
            times.append(("reminder2", extra))

    return times


async def _send_support_message(user_id: int, text: str) -> None:
    try:
        await bot.send_message(
            user_id,
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Заполнить отчёт", callback_data="menu:daily_report")],
                [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
            ]),
        )
    except TelegramRetryAfter as e:
        # Respect Telegram flood limits
        await asyncio.sleep(int(getattr(e, "retry_after", 1)) + 1)
        await bot.send_message(
            user_id,
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Заполнить отчёт", callback_data="menu:daily_report")],
                [InlineKeyboardButton(text="В меню", callback_data="menu:main")],
            ]),
        )


async def scheduler_tick() -> None:
    """Check due notifications and send them."""
    try:
        users = await get_users_for_reminder()

        # Active templates
        templates = [t for t in (await get_notification_templates()) if t.get("is_active")]
        if not templates:
            templates = [{"text": msg} for msg in SUPPORT_MESSAGES[:5]]

        for user in users:
            user_id = int(user["user_id"])
            if not int(user.get("support_enabled", 1) or 1):
                continue

            tz_str = user.get("timezone") or DEFAULT_TIMEZONE
            try:
                tz = ZoneInfo(tz_str)
            except Exception:
                tz = ZoneInfo(DEFAULT_TIMEZONE)

            now = datetime.now(tz)
            current_minutes = now.hour * 60 + now.minute
            date_str = now.strftime("%Y-%m-%d")

            reminder_time = user.get("reminder_time") or DEFAULT_REMINDER_TIME
            frequency = int(user.get("support_frequency", 1) or 1)

            for notif_type, time_str in _support_times(reminder_time, frequency):
                target_minutes = _parse_hhmm(time_str)
                if target_minutes is None:
                    continue

                # 1-minute tolerance
                if _minutes_diff(current_minutes, target_minutes) > 1:
                    continue

                if await was_notification_sent(user_id, notif_type, date_str):
                    continue

                template = random.choice(templates)
                text = template.get("text") or random.choice(SUPPORT_MESSAGES)

                try:
                    await _send_support_message(user_id, text)
                    await log_notification(user_id, notif_type, date_str)
                    logger.info(f"Sent {notif_type} to user {user_id}")
                except (TelegramForbiddenError, TelegramBadRequest) as e:
                    logger.warning(f"Failed to send {notif_type} to {user_id}: {e}")
                except TelegramNetworkError as e:
                    logger.warning(f"Network error while sending {notif_type} to {user_id}: {e}")
                except Exception as e:
                    logger.error(f"Scheduler error for user {user_id}: {e}")

                # Gentle rate limiting for bursts
                await asyncio.sleep(0.05)

    except Exception as e:
        logger.error(f"Scheduler tick error: {e}")


async def _on_startup() -> None:
    await init_db()

    # Start background scheduler
    scheduler.add_job(
        scheduler_tick,
        "interval",
        seconds=SCHEDULER_TICK_SECONDS,
        id="scheduler_tick",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started")


async def _on_shutdown() -> None:
    with suppress(Exception):
        scheduler.shutdown(wait=False)
    with suppress(Exception):
        await db.close()
    logger.info("Shutdown complete")


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    """Main entry point."""
    logger.info("Starting bot...")

    dp.startup.register(_on_startup)
    dp.shutdown.register(_on_shutdown)

    logger.info("Bot is running")
    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types(),
        handle_signals=True,
        close_bot_session=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
