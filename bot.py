#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GameSwap Spain Bot
Bot para intercambio de juegos entre gamers

Версия (исправленная):
- Регистрация + добавление игр + поиск + каталог + профиль
- Контакт владельца ВСЕГДА через tg://user?id=<user_id> (работает без @username)
- Swap: выбираем свою игру -> вводим название полученной игры -> выбираем ТОЧНУЮ карточку (игра+владелец) -> отправляем запрос
  ✅ Это решает ситуацию, когда одна и та же игра есть у нескольких пользователей
  ✅ Не требуется @username вообще

Feedback (rating + comment + photos) tras swap completado
ADMIN minimal + Ban guard — как было

ВАЖНО:
- Удали старый swap-поток по @username (в этой версии его нет).
"""

import os
import logging
import html
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from database import Database

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ----------------------------
# Conversation states
# ----------------------------
REGISTRATION_NAME, REGISTRATION_CITY = range(2)
ADD_GAME_TITLE, ADD_GAME_PLATFORM, ADD_GAME_CONDITION, ADD_GAME_PHOTO, ADD_GAME_LOOKING = range(5)
SEARCH_QUERY = 0

# Swap flow states (НОВЫЙ поток без @username)
SWAP_SELECT_OWN, SWAP_INPUT_OTHER_TITLE, SWAP_SELECT_OTHER_GAME, SWAP_CONFIRM = range(4)

# Feedback flow states
FB_TEXT, FB_PHOTOS = range(2)

# ----------------------------
# DB
# ----------------------------
db = Database()

# ----------------------------
# Helpers
# ----------------------------
def env(name: str) -> str | None:
    v = os.getenv(name)
    if not v:
        return None
    return v.strip().strip('"').strip("'")


def publish_target_chat_id() -> str | int | None:
    v = env("CHANNEL_CHAT_ID") or env("GROUP_CHAT_ID")
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return v


async def safe_publish_text(context: ContextTypes.DEFAULT_TYPE, text: str, *, parse_mode: str | None = None) -> None:
    chat_id = publish_target_chat_id()
    if not chat_id:
        logger.warning("Publish skipped: CHANNEL_CHAT_ID/GROUP_CHAT_ID not set")
        return
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except Exception:
        logger.exception("Failed to publish text to %r", chat_id)


async def safe_publish_photo(context: ContextTypes.DEFAULT_TYPE, photo_file_id: str, caption: str, *, parse_mode: str | None = None) -> None:
    chat_id = publish_target_chat_id()
    if not chat_id:
        logger.warning("Publish skipped: CHANNEL_CHAT_ID/GROUP_CHAT_ID not set")
        return
    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=photo_file_id,
            caption=caption,
            parse_mode=parse_mode,
        )
    except Exception:
        logger.exception("Failed to publish photo to %r", chat_id)


def fmt_game(g: dict) -> str:
    return f"{g['title']} ({g['platform']}, {g['condition']})"


def user_has_username(u: dict) -> bool:
    return bool((u.get("username") or "").strip())


def user_label(u: dict) -> str:
    """
    Безопасный лейбл для текста.
    Если есть @username -> @username
    Если нет -> display_name
    (ID публично НЕ показываем)
    """
    if user_has_username(u):
        un = (u.get("username") or "").strip()
        if not un.startswith("@"):
            un = "@" + un
        return un
    return (u.get("display_name") or "Usuario").strip()


def user_contact_url(u: dict) -> str | None:
    """
    Универсальная ссылка для контакта, работает даже без username.
    """
    try:
        uid = int(u.get("user_id") or 0)
        if uid <= 0:
            return None
        return f"tg://user?id={uid}"
    except Exception:
        return None


def user_contact_button(u: dict, label: str = "💬 Escribir al dueño") -> InlineKeyboardMarkup | None:
    url = user_contact_url(u)
    if not url:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, url=url)]])


def stars_label(n: int) -> str:
    n = max(1, min(5, int(n)))
    return "⭐" * n + "☆" * (5 - n)


def _fb_key(swap_id: int, to_user_id: int) -> str:
    return f"fb:{int(swap_id)}:{int(to_user_id)}"


# ----------------------------
# Admin helpers + ban guard
# ----------------------------
def admin_id() -> int:
    try:
        return int(env("ADMIN_ID") or "0")
    except Exception:
        return 0


def is_admin_user(user_id: int) -> bool:
    return int(user_id) == int(admin_id())


async def banned_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    True => пользователь забанен и мы уже ответили.
    """
    try:
        uid = int(update.effective_user.id)
        u = db.get_user(uid)
        if u and int(u.get("is_banned") or 0) == 1:
            if update.message:
                await update.message.reply_text("🚫 Tu cuenta está bloqueada por el administrador.")
            elif update.callback_query:
                await update.callback_query.answer("🚫 Bloqueado.", show_alert=True)
            return True
    except Exception:
        pass
    return False


def _admin_users_state(context: ContextTypes.DEFAULT_TYPE) -> dict:
    st = context.user_data.get("admin_users_state")
    if not isinstance(st, dict):
        st = {"offset": 0, "only_banned": False, "query": ""}
        context.user_data["admin_users_state"] = st
    return st


def _fmt_user_line(u: dict) -> str:
    ban = "🚫" if int(u.get("is_banned") or 0) == 1 else "✅"
    uname = (u.get("username") or "").strip()
    uname_line = f"@{uname}" if uname else "—"
    return (
        f"{ban} {u.get('user_id')}  {uname_line} | {u.get('display_name','')} | {u.get('city','')} | "
        f"⭐{float(u.get('rating') or 0.0):.1f} ({int(u.get('rating_count') or 0)}) | 🔄{int(u.get('total_swaps') or 0)}"
    )


async def _admin_render_users_page(update: Update, context: ContextTypes.DEFAULT_TYPE, *, edit: bool = False) -> None:
    st = _admin_users_state(context)
    limit = 10
    offset = int(st.get("offset") or 0)
    only_banned = bool(st.get("only_banned"))
    query = (st.get("query") or "").strip()

    total = db.admin_count_users(only_banned=only_banned, query=query)
    users = db.admin_list_users(limit=limit, offset=offset, only_banned=only_banned, query=query)

    header = "👮 ADMIN — USERS\n"
    header += f"Filtro: {'SOLO BANEADOS' if only_banned else 'TODOS'}\n"
    header += f"Buscar: {query if query else '—'}\n"
    if total == 0:
        header += "Mostrando: 0\n\n"
    else:
        header += f"Mostrando: {offset+1}-{min(offset+len(users), total)} de {total}\n\n"

    if not users:
        text = header + "No hay usuarios con este filtro."
    else:
        lines = "\n".join(_fmt_user_line(u) for u in users)
        text = header + lines

    kb = []
    kb.append(
        [
            InlineKeyboardButton("🔁 Toggle banned filter", callback_data="adm_users_toggle_banned"),
            InlineKeyboardButton("🔍 Clear search", callback_data="adm_users_clear_search"),
        ]
    )

    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data="adm_users_prev"))
    if offset + limit < total:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data="adm_users_next"))
    if nav:
        kb.append(nav)

    kb.append([InlineKeyboardButton("ℹ️ Help", callback_data="adm_users_help")])

    markup = InlineKeyboardMarkup(kb)

    if update.callback_query and edit:
        await update.callback_query.edit_message_text(text=text, reply_markup=markup)
    else:
        if update.message:
            await update.message.reply_text(text=text, reply_markup=markup)
        elif update.effective_chat:
            await update.effective_chat.send_message(text=text, reply_markup=markup)


# ============================
# MAIN COMMANDS
# ============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = db.get_user(user_id)

    if user:
        await update.message.reply_text(
            f"👋 ¡Bienvenid@ de nuevo, {user['display_name']}! 🎮\n\n"
            f"📍 Tu ubicación: {user['city']}\n"
            f"⭐ Valoración: {float(user['rating'] or 0.0):.1f}/5.0\n"
            f"🔄 Intercambios completados: {int(user['total_swaps'] or 0)}\n\n"
            "Usa estos comandos:\n"
            "/add - añadir un juego\n"
            "/mygames - mis juegos\n"
            "/search - buscar juego\n"
            "/catalog - ver catálogo completo\n"
            "/profile - mi perfil\n"
            "/swap - confirmar intercambio\n"
            "/help - ayuda"
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "🎮 ¡Hola! Bienvenid@ a GameSwap Spain\n\n"
        "Aquí puedes intercambiar juegos físicos con otros jugadores sin gastar dinero.\n\n"
        "📝 ¡Vamos a registrarte!\n\n"
        "¿Cómo te llamas? (o escribe tu nick)"
    )
    return REGISTRATION_NAME


async def registration_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["display_name"] = (update.message.text or "").strip()

    keyboard = [
        ["Madrid", "Barcelona"],
        ["Valencia", "Sevilla"],
        ["Bilbao", "Málaga"],
        ["Otra ciudad 📝"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        f"¡Perfecto, {context.user_data['display_name']}! 👍\n\n"
        "📍 ¿En qué ciudad vives?",
        reply_markup=reply_markup,
    )
    return REGISTRATION_CITY


async def registration_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = (update.message.text or "").strip()

    if city == "Otra ciudad 📝":
        await update.message.reply_text("Escribe el nombre de tu ciudad:", reply_markup=ReplyKeyboardRemove())
        return REGISTRATION_CITY

    user_id = update.effective_user.id
    username = update.effective_user.username or ""  # пустая строка, если нет
    display_name = context.user_data.get("display_name", "SinNombre")

    db.create_user(user_id, username, display_name, city)

    await update.message.reply_text(
        "✅ ¡Registro completado!\n\n"
        f"👤 Nombre: {display_name}\n"
        f"📍 Ciudad: {city}\n\n"
        "Ahora puedes:\n"
        "/add — añadir juego para intercambio\n"
        "/search — buscar juego\n"
        "/catalog — ver todos los juegos disponibles\n"
        "/swap — confirmar intercambio\n"
        "/help — obtener ayuda",
        reply_markup=ReplyKeyboardRemove(),
    )

    await safe_publish_text(
        context,
        text=(
            "👋 ¡Nuevo miembro!\n\n"
            f"👤 {html.escape(display_name)} ({html.escape(city)}) se ha unido a GameSwap Spain\n"
            f"Total de usuarios: {db.get_total_users()}"
        ),
        parse_mode=None,
    )

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text("❌ Operación cancelada.", reply_markup=ReplyKeyboardRemove())
    context.user_data.pop("swap_offered_game_id", None)
    context.user_data.pop("swap_other_user_id", None)
    context.user_data.pop("swap_requested_game_id", None)
    context.user_data.pop("swap_other_title", None)
    return ConversationHandler.END


# ============================
# ADD GAME
# ============================
async def add_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return ConversationHandler.END

    user_id = update.effective_user.id
    user = db.get_user(user_id)

    if not user:
        await update.message.reply_text("⚠️ Primero debes registrarte.\nEscribe /start")
        return ConversationHandler.END

    await update.message.reply_text(
        "🎮 Añadiendo nuevo juego\n\n"
        "Escribe el título completo del juego:\n"
        "(ejemplo: God of War Ragnarök)\n\n"
        "O escribe /cancel para cancelar"
    )
    return ADD_GAME_TITLE


async def add_game_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["game_title"] = (update.message.text or "").strip()

    keyboard = [
        [InlineKeyboardButton("🎮 PS5", callback_data="platform_ps5")],
        [InlineKeyboardButton("🎮 PS4", callback_data="platform_ps4")],
        [InlineKeyboardButton("🎮 Xbox Series X|S", callback_data="platform_xboxsx")],
        [InlineKeyboardButton("🎮 Xbox One", callback_data="platform_xboxone")],
        [InlineKeyboardButton("🎮 Nintendo Switch", callback_data="platform_switch")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"📝 Juego: {context.user_data['game_title']}\n\n¿En qué plataforma está?",
        reply_markup=reply_markup,
    )
    return ADD_GAME_PLATFORM


async def add_game_platform(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    platform_map = {
        "platform_ps5": "PS5",
        "platform_ps4": "PS4",
        "platform_xboxsx": "Xbox Series X|S",
        "platform_xboxone": "Xbox One",
        "platform_switch": "Nintendo Switch",
    }

    context.user_data["game_platform"] = platform_map.get(query.data, "Unknown")

    keyboard = [
        [InlineKeyboardButton("⭐ Excelente (como nuevo)", callback_data="condition_excellent")],
        [InlineKeyboardButton("👍 Bueno (pequeños arañazos)", callback_data="condition_good")],
        [InlineKeyboardButton("👌 Aceptable (funciona)", callback_data="condition_fair")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"📝 Juego: {context.user_data['game_title']}\n"
        f"🎮 Plataforma: {context.user_data['game_platform']}\n\n"
        "¿En qué estado está el disco?",
        reply_markup=reply_markup,
    )
    return ADD_GAME_CONDITION


async def add_game_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    condition_map = {
        "condition_excellent": "Excelente",
        "condition_good": "Bueno",
        "condition_fair": "Aceptable",
    }
    context.user_data["game_condition"] = condition_map.get(query.data, "Bueno")

    await query.edit_message_text(
        f"📝 Juego: {context.user_data['game_title']}\n"
        f"🎮 Plataforma: {context.user_data['game_platform']}\n"
        f"⭐ Estado: {context.user_data['game_condition']}\n\n"
        "📸 Sube una foto del disco (con caja si la tienes)\n"
        "O escribe /skip para omitir"
    )
    return ADD_GAME_PHOTO


async def add_game_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and update.message.text.strip().lower() in {"/skip", "skip"}:
        context.user_data["game_photo"] = None
    elif update.message and update.message.photo:
        photo = update.message.photo[-1]
        context.user_data["game_photo"] = photo.file_id
    else:
        await update.message.reply_text("❌ Envía una foto o escribe /skip")
        return ADD_GAME_PHOTO

    await update.message.reply_text(
        f"📝 Juego: {context.user_data['game_title']}\n"
        f"🎮 Plataforma: {context.user_data['game_platform']}\n"
        f"⭐ Estado: {context.user_data['game_condition']}\n\n"
        "🔄 ¿Qué juego buscas a cambio?\n"
        "(escribe el título o por ejemplo: «cualquier RPG», «cualquier shooter», etc.)\n\n"
        "O escribe «cualquiera» si te da igual"
    )
    return ADD_GAME_LOOKING


async def add_game_looking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["game_looking_for"] = (update.message.text or "").strip()

    user_id = update.effective_user.id
    db.add_game(
        user_id=user_id,
        title=context.user_data["game_title"],
        platform=context.user_data["game_platform"],
        condition=context.user_data["game_condition"],
        photo_url=context.user_data.get("game_photo"),
        looking_for=context.user_data["game_looking_for"],
    )

    user = db.get_user(user_id) or {}

    await update.message.reply_text(
        "✅ ¡Juego añadido al catálogo!\n\n"
        f"🎮 {context.user_data['game_title']}\n"
        f"📱 {context.user_data['game_platform']}\n"
        f"⭐ {context.user_data['game_condition']}\n"
        f"🔄 Busco: {context.user_data['game_looking_for']}\n\n"
        "Tus juegos → /mygames\n"
        "Añadir otro → /add"
    )

    owner_line = user_label(user)
    contact_url = user_contact_url(user)
    contact_line = f"tg://user?id={int(user.get('user_id') or user_id)}" if contact_url else ""

    message_text = (
        "🆕 ¡NUEVO JUEGO EN EL CATÁLOGO!\n\n"
        f"🎮 {html.escape(context.user_data['game_title'])}\n"
        f"📱 {html.escape(context.user_data['game_platform'])}\n"
        f"⭐ Estado: {html.escape(context.user_data['game_condition'])}\n"
        f"🔄 Busca: {html.escape(context.user_data['game_looking_for'])}\n\n"
        f"👤 Propietario: {html.escape(owner_line)}\n"
        f"📍 Ciudad: {html.escape(user.get('city',''))}\n"
        f"⭐ Valoración: {float(user.get('rating') or 0.0):.1f} ({int(user.get('total_swaps') or 0)} intercambios)\n\n"
        + (f"💬 Contactar: {contact_line}" if contact_line else "💬 Contactar: (sin link)")
    )

    photo_id = context.user_data.get("game_photo")
    if photo_id:
        await safe_publish_photo(context, photo_file_id=photo_id, caption=message_text, parse_mode=None)
    else:
        await safe_publish_text(context, text=message_text, parse_mode=None)

    return ConversationHandler.END


# ============================
# MY GAMES
# ============================
async def my_games(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return

    user_id = update.effective_user.id
    user = db.get_user(user_id)

    if not user:
        await update.message.reply_text("⚠️ Primero regístrate → /start")
        return

    games = db.get_user_games(user_id)
    if not games:
        await update.message.reply_text("📦 Todavía no tienes juegos en el catálogo.\n\nAñade uno → /add")
        return

    message = f"🎮 TUS JUEGOS ({len(games)}):\n\n"
    for i, game in enumerate(games, 1):
        message += (
            f"✅ {i}. {game['title']}\n"
            f"   📱 {game['platform']}  |  ⭐ {game['condition']}\n"
            f"   🔄 Busco: {game['looking_for']}\n"
            f"   📅 Añadido: {str(game['created_date'])[:10]}\n\n"
        )

    message += "Para eliminar un juego escribe:\n/remove [número]"
    await update.message.reply_text(message)


# ============================
# SEARCH
# ============================
async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return ConversationHandler.END

    await update.message.reply_text(
        "🔍 BUSCAR JUEGO\n\n"
        "Escribe el nombre del juego que estás buscando:\n"
        "(ejemplo: Elden Ring)\n\n"
        "O escribe /cancel para cancelar"
    )
    return SEARCH_QUERY


async def search_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = (update.message.text or "").strip()
    user_id = update.effective_user.id

    results = db.search_games(q)
    if not results:
        await update.message.reply_text(
            f"😔 No se encontró «{q}» en el catálogo.\n\n"
            "Prueba con:\n"
            "• Otro nombre o forma de escribirlo\n"
            "• /catalog — ver todo el catálogo\n"
            "• /add — añade tu juego, ¡quizá alguien lo esté buscando!"
        )
        return ConversationHandler.END

    shown = 0
    for game in results:
        if int(game["user_id"]) == int(user_id):
            continue

        owner = db.get_user(int(game["user_id"]))
        if not owner:
            continue

        text = (
            f"🎮 {game['title']}\n"
            f"📱 {game['platform']}  |  ⭐ {game['condition']}\n"
            f"🔄 Busca: {game['looking_for']}\n"
            f"👤 Dueño: {owner.get('display_name','Usuario')} ({owner.get('city','')})\n"
            f"⭐ {float(owner.get('rating') or 0.0):.1f}/5.0  ({int(owner.get('total_swaps') or 0)} intercambios)\n"
        )

        markup = user_contact_button(owner, "💬 Escribir al dueño")
        await update.message.reply_text(text, reply_markup=markup)

        shown += 1
        if shown >= 10:
            break

    if shown == 0:
        await update.message.reply_text("No hay juegos de otros usuarios que coincidan con tu búsqueda.")
        return ConversationHandler.END

    if len(results) > shown:
        await update.message.reply_text(f"… y {len(results) - shown} resultados más (refina búsqueda)")

    return ConversationHandler.END


# ============================
# CATALOG
# ============================
async def catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return

    user_id = update.effective_user.id
    games = db.get_all_active_games()

    if not games:
        await update.message.reply_text("📦 El catálogo está vacío por ahora.\n\n¡Sé el primero! → /add")
        return

    platforms: dict[str, list] = {}
    for game in games:
        if int(game["user_id"]) == int(user_id):
            continue
        platforms.setdefault(game["platform"], []).append(game)

    message = f"📚 CATÁLOGO COMPLETO ({len(games)} juegos)\n\n"
    for platform, games_list in platforms.items():
        message += f"🎮 {platform} ({len(games_list)}):\n"
        for game in games_list[:5]:
            owner = db.get_user(int(game["user_id"]))
            if owner:
                # В каталоге не показываем @username и не показываем ID
                message += f" • {game['title']} (dueño: {owner.get('display_name','Usuario')})\n"
        if len(games_list) > 5:
            message += f"   … y otros {len(games_list) - 5}\n"
        message += "\n"

    message += "Para buscar un juego concreto usa:\n/search [nombre]\n"
    await update.message.reply_text(message)


# ============================
# PROFILE
# ============================
async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return

    user_id = update.effective_user.id
    user = db.get_user(user_id)

    if not user:
        await update.message.reply_text("⚠️ Primero regístrate → /start")
        return

    games_count = len(db.get_user_games(user_id))

    summary = db.get_user_feedback_summary(user_id)
    rating = summary.get("rating", float(user.get("rating") or 0.0))
    rating_count = summary.get("rating_count", 0)

    uname = (user.get("username") or "").strip()
    uname_line = f"@{uname}" if uname else "— (sin username)"

    message = (
        "👤 TU PERFIL\n\n"
        f"Nombre: {user['display_name']}\n"
        f"Usuario: {uname_line}\n"
        f"ID: {int(user.get('user_id') or 0)}\n"
        f"📍 Ciudad: {user['city']}\n"
        f"⭐ Valoración: {float(rating):.1f}/5.0 ({int(rating_count)} votos)\n"
        f"🔄 Intercambios completados: {int(user['total_swaps'] or 0)}\n"
        f"🎮 Juegos activos: {games_count}\n"
        f"📅 En GameSwap desde: {str(user['registered_date'])[:10]}\n\n"
        "Comandos útiles:\n"
        "/mygames — ver mis juegos\n"
        "/add — añadir juego\n"
        "/search — buscar juego\n"
        "/swap — confirmar intercambio"
    )

    await update.message.reply_text(message)


# ============================
# SWAP FLOW (НОВЫЙ: по названию игры, без @username)
# ============================
async def swap_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return ConversationHandler.END

    user_id = update.effective_user.id
    user = db.get_user(user_id)
    if not user:
        await update.message.reply_text("⚠️ Primero debes registrarte → /start")
        return ConversationHandler.END

    my_games = db.get_user_games(user_id)
    if not my_games:
        await update.message.reply_text("📦 No tienes juegos activos. Añade uno → /add")
        return ConversationHandler.END

    keyboard = [[InlineKeyboardButton(fmt_game(g), callback_data=f"swap_offer:{g['game_id']}")] for g in my_games[:20]]

    await update.message.reply_text(
        "🔄 CONFIRMAR INTERCAMBIO\n\n"
        "Paso 1/3 — Elige el juego que TÚ entregaste:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return SWAP_SELECT_OWN


async def swap_select_own(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, game_id_str = query.data.split(":")
    game_id = int(game_id_str)

    g = db.get_game(game_id)
    if not g or int(g["user_id"]) != int(update.effective_user.id):
        await query.edit_message_text("❌ Ese juego no existe o no es tuyo.")
        return ConversationHandler.END

    context.user_data["swap_offered_game_id"] = game_id
    context.user_data.pop("swap_other_user_id", None)
    context.user_data.pop("swap_requested_game_id", None)
    context.user_data.pop("swap_other_title", None)

    await query.edit_message_text(
        "Paso 2/3 — Escribe el nombre del juego que RECIBISTE.\n\n"
        "Ejemplo: GTA, Elden Ring, Mario...\n"
        "Cancelar: /cancel"
    )
    return SWAP_INPUT_OTHER_TITLE


async def swap_input_other_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = (update.message.text or "").strip()
    if not q:
        await update.message.reply_text("❌ Escribe un nombre de juego o /cancel.")
        return SWAP_INPUT_OTHER_TITLE

    my_id = int(update.effective_user.id)
    context.user_data["swap_other_title"] = q

    results = db.search_games(q)
    # убираем свои игры
    results = [g for g in results if int(g["user_id"]) != my_id]

    if not results:
        await update.message.reply_text(
            f"😔 No encontré «{q}» en el catálogo.\n"
            "Escribe otro nombre o /cancel."
        )
        return SWAP_INPUT_OTHER_TITLE

    kb = []
    shown = 0
    for g in results:
        owner = db.get_user(int(g["user_id"]))
        if not owner:
            continue

        # Укорачиваем подпись кнопки, чтобы Telegram не ругался
        owner_name = owner.get("display_name", "Usuario")
        city = owner.get("city", "")
        btn = f"{g['title']} | {g['platform']} | {owner_name} {('('+city+')') if city else ''}"
        kb.append([InlineKeyboardButton(btn[:60], callback_data=f"swap_take:{int(g['game_id'])}")])

        shown += 1
        if shown >= 10:
            break

    kb.append([InlineKeyboardButton("❌ Cancelar", callback_data="swap_cancel_flow")])

    await update.message.reply_text(
        "Paso 3/3 — Elige la tarjeta EXACTA (juego + dueño):",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return SWAP_SELECT_OTHER_GAME


async def swap_select_other_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "swap_cancel_flow":
        await query.edit_message_text("❌ Intercambio cancelado.")
        return ConversationHandler.END

    _, game_id_str = query.data.split(":")
    requested_game_id = int(game_id_str)

    offered_game_id = context.user_data.get("swap_offered_game_id")
    if not offered_game_id:
        await query.edit_message_text("❌ Sesión caducada. Empieza de nuevo: /swap")
        return ConversationHandler.END

    offered = db.get_game(int(offered_game_id))
    requested = db.get_game(int(requested_game_id))

    if not offered or not requested:
        await query.edit_message_text("❌ Juego no encontrado.")
        return ConversationHandler.END

    if int(offered["user_id"]) != int(update.effective_user.id):
        await query.edit_message_text("❌ El juego ofrecido no es tuyo.")
        return ConversationHandler.END

    if int(requested["user_id"]) == int(update.effective_user.id):
        await query.edit_message_text("❌ No puedes intercambiar contigo mismo.")
        return ConversationHandler.END

    other_user_id = int(requested["user_id"])
    context.user_data["swap_other_user_id"] = other_user_id
    context.user_data["swap_requested_game_id"] = int(requested_game_id)

    owner = db.get_user(other_user_id) or {"user_id": other_user_id, "display_name": "Usuario"}

    confirm_text = (
        "🔄 CONFIRMAR INTERCAMBIO\n\n"
        f"Tú das:  🎮 {fmt_game(offered)}\n"
        f"Tú recibes: 🎮 {fmt_game(requested)}\n\n"
        f"Con: {owner.get('display_name','Usuario')} ({owner.get('city','')})\n\n"
        "¿Enviar solicitud de confirmación?"
    )
    keyboard = [
        [InlineKeyboardButton("✅ Enviar solicitud", callback_data="swap_send")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="swap_cancel")],
    ]
    await query.edit_message_text(confirm_text, reply_markup=InlineKeyboardMarkup(keyboard))
    return SWAP_CONFIRM


async def swap_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "swap_cancel":
        await query.edit_message_text("❌ Intercambio cancelado.")
        return ConversationHandler.END

    offered_game_id = context.user_data.get("swap_offered_game_id")
    requested_game_id = context.user_data.get("swap_requested_game_id")

    if not offered_game_id or not requested_game_id:
        await query.edit_message_text("❌ Sesión caducada. Empieza de nuevo: /swap")
        return ConversationHandler.END

    offered = db.get_game(int(offered_game_id))
    requested = db.get_game(int(requested_game_id))
    if not offered or not requested:
        await query.edit_message_text("❌ Juego no encontrado.")
        return ConversationHandler.END

    initiator_id = int(update.effective_user.id)
    recipient_id = int(requested["user_id"])

    created = db.create_swap_request(
        user1_id=initiator_id,
        user2_id=recipient_id,
        game1_id=int(offered_game_id),
        game2_id=int(requested_game_id),
    )
    if not created:
        await query.edit_message_text("❌ No se pudo crear la solicitud. (¿Juegos cambiaron o no están activos?)")
        return ConversationHandler.END

    swap_id, code = created
    initiator = db.get_user(initiator_id) or {"user_id": initiator_id, "display_name": "Usuario"}

    await query.edit_message_text(
        "✅ Solicitud enviada.\n\n"
        f"📌 Código: {code}\n"
        "El otro usuario debe confirmarlo en el bot."
    )

    msg = (
        "🔔 SOLICITUD DE INTERCAMBIO\n\n"
        f"{initiator.get('display_name','Usuario')} propone:\n\n"
        f"Él/ella te da: 🎮 {fmt_game(offered)}\n"
        f"Y quiere: 🎮 {fmt_game(requested)}\n\n"
        f"📌 Código: {code}\n\n"
        "¿Confirmas que el intercambio se realizó?"
    )
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Confirmar", callback_data=f"swap_accept:{swap_id}")],
            [InlineKeyboardButton("❌ Rechazar", callback_data=f"swap_reject:{swap_id}")],
        ]
    )

    try:
        await context.bot.send_message(chat_id=recipient_id, text=msg, reply_markup=kb)
    except Exception:
        logger.exception("Failed to notify recipient about swap %s", swap_id)

    return ConversationHandler.END


# ----------------------------
# Swap accept/reject callbacks
# ----------------------------
async def swap_accept_or_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    action, swap_id_str = query.data.split(":")
    swap_id = int(swap_id_str)

    swap = db.get_swap(swap_id)
    if not swap:
        await query.edit_message_text("❌ Este intercambio ya no existe.")
        return

    user_id = int(update.effective_user.id)
    if user_id != int(swap["user2_id"]):
        await query.edit_message_text("❌ Solo el segundo participante puede confirmar/rechazar.")
        return

    if swap.get("status") != "pending":
        await query.edit_message_text(f"ℹ️ Este intercambio ya está en estado: {swap.get('status')}")
        return

    if action == "swap_reject":
        db.set_swap_status(swap_id, "rejected")
        await query.edit_message_text("❌ Has rechazado el intercambio.")
        try:
            await context.bot.send_message(
                chat_id=int(swap["user1_id"]),
                text="❌ Tu solicitud de intercambio fue rechazada.",
            )
        except Exception:
            logger.exception("Failed to notify initiator about rejection")
        return

    ok, err = db.complete_swap(swap_id, confirmer_user_id=user_id)
    if not ok:
        await query.edit_message_text(f"❌ No se pudo completar: {err}")
        return

    await query.edit_message_text("✅ Intercambio confirmado. ¡Listo!")

    try:
        await context.bot.send_message(
            chat_id=int(swap["user1_id"]),
            text="✅ Tu intercambio fue confirmado. Los juegos cambiaron de dueño.",
        )
    except Exception:
        logger.exception("Failed to notify initiator after swap completion")

    try:
        g1 = db.get_game(int(swap["game1_id"]))
        g2 = db.get_game(int(swap["game2_id"]))
        u1 = db.get_user(int(swap["user1_id"]))
        u2 = db.get_user(int(swap["user2_id"]))
        if g1 and g2 and u1 and u2:
            await safe_publish_text(
                context,
                text=(
                    "🔄 Intercambio completado\n\n"
                    f"{u1.get('display_name','Usuario')} ↔ {u2.get('display_name','Usuario')}\n"
                    f"🎮 {g1['title']} ⇄ 🎮 {g2['title']}"
                ),
                parse_mode=None,
            )
    except Exception:
        logger.exception("Failed to publish swap completion")

    await start_feedback_for_user(
        context=context,
        rater_user_id=int(swap["user1_id"]),
        ratee_user_id=int(swap["user2_id"]),
        swap_id=swap_id,
    )
    await start_feedback_for_user(
        context=context,
        rater_user_id=int(swap["user2_id"]),
        ratee_user_id=int(swap["user1_id"]),
        swap_id=swap_id,
    )


# ============================
# FEEDBACK FLOW
# ============================
async def start_feedback_for_user(
    context: ContextTypes.DEFAULT_TYPE,
    rater_user_id: int,
    ratee_user_id: int,
    swap_id: int,
):
    ratee = db.get_user(ratee_user_id) or {"user_id": ratee_user_id, "display_name": "Usuario"}
    text = (
        "⭐ VALORACIÓN DEL INTERCAMBIO\n\n"
        f"Valora a {ratee.get('display_name','Usuario')}.\n"
        "Elige estrellas:"
    )
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(stars_label(i), callback_data=f"fb_stars:{swap_id}:{ratee_user_id}:{i}")] for i in range(5, 0, -1)]
        + [[InlineKeyboardButton("Omitir", callback_data=f"fb_skip:{swap_id}:{ratee_user_id}")]]
    )
    await context.bot.send_message(chat_id=rater_user_id, text=text, reply_markup=kb)


async def fb_stars_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    action = parts[0]
    swap_id = int(parts[1])
    ratee_user_id = int(parts[2])
    rater_user_id = int(update.effective_user.id)

    if action == "fb_skip":
        await query.edit_message_text("👍 Ok, sin valoración.")
        return ConversationHandler.END

    stars = int(parts[3])
    key = _fb_key(swap_id, ratee_user_id)

    context.chat_data[key] = {
        "swap_id": swap_id,
        "from_user_id": rater_user_id,
        "to_user_id": ratee_user_id,
        "stars": stars,
        "comment": None,
        "photos": [],
    }
    context.chat_data["fb_active_key"] = key

    await query.edit_message_text(
        f"⭐ Has elegido: {stars_label(stars)}\n\n"
        "Ahora escribe un comentario corto (o escribe /skip para omitir):"
    )
    return FB_TEXT


async def fb_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = context.chat_data.get("fb_active_key")
    if not key or key not in context.chat_data:
        await update.message.reply_text("❌ Sesión de valoración caducada.")
        return ConversationHandler.END

    text = (update.message.text or "").strip()
    if text.lower() in {"/skip", "skip"}:
        context.chat_data[key]["comment"] = None
    else:
        context.chat_data[key]["comment"] = text[:800]

    await update.message.reply_text(
        "📸 Puedes enviar hasta 3 fotos como prueba (una por mensaje).\n"
        "Cuando termines, escribe /done.\n"
        "O escribe /skip para no enviar fotos."
    )
    return FB_PHOTOS


async def fb_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = context.chat_data.get("fb_active_key")
    if not key or key not in context.chat_data:
        await update.message.reply_text("❌ Sesión de valoración caducada.")
        return ConversationHandler.END

    session = context.chat_data[key]

    if update.message.text:
        cmd = update.message.text.strip().lower()
        if cmd in {"/skip", "skip", "/done", "done"}:
            return await fb_finish(update, context)
        await update.message.reply_text("Envía una foto, o escribe /done para terminar.")
        return FB_PHOTOS

    if not update.message.photo:
        await update.message.reply_text("Envía una foto, o escribe /done.")
        return FB_PHOTOS

    if len(session["photos"]) >= 3:
        await update.message.reply_text("Ya tienes 3 fotos. Escribe /done para terminar.")
        return FB_PHOTOS

    file_id = update.message.photo[-1].file_id
    session["photos"].append(file_id)

    await update.message.reply_text(f"✅ Foto añadida ({len(session['photos'])}/3). Envía otra o escribe /done.")
    return FB_PHOTOS


async def fb_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = context.chat_data.get("fb_active_key")
    if not key or key not in context.chat_data:
        await update.message.reply_text("❌ Sesión de valoración caducada.")
        return ConversationHandler.END

    session = context.chat_data[key]

    feedback_id = db.add_feedback(
        swap_id=int(session["swap_id"]),
        from_user_id=int(session["from_user_id"]),
        to_user_id=int(session["to_user_id"]),
        stars=int(session["stars"]),
        comment=session.get("comment"),
    )

    if feedback_id:
        for p in session.get("photos", []):
            db.add_feedback_photo(int(feedback_id), p)
        await update.message.reply_text("✅ ¡Gracias! Valoración guardada.")
    else:
        await update.message.reply_text("ℹ️ No se pudo guardar (¿ya valoraste este intercambio?).")

    context.chat_data.pop(key, None)
    context.chat_data.pop("fb_active_key", None)
    return ConversationHandler.END


# ============================
# ADMIN COMMANDS
# ============================
async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    query = ""
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) == 2:
            query = parts[1].strip()

    st = _admin_users_state(context)
    st["offset"] = 0
    st["query"] = query

    await _admin_render_users_page(update, context, edit=False)


async def admin_users_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        await update.callback_query.answer("No access", show_alert=True)
        return

    q = update.callback_query
    await q.answer()

    st = _admin_users_state(context)
    limit = 10

    if q.data == "adm_users_help":
        await q.edit_message_text(
            "👮 ADMIN USERS HELP\n\n"
            "Команды:\n"
            "/admin_users [query] — список пользователей\n"
            "/admin_user <id|@username> — карточка\n"
            "/admin_ban <id|@username> [reason]\n"
            "/admin_unban <id|@username>\n"
            "/admin_games <id|@username>\n"
            "/admin_remove_game <game_id>\n"
            "/admin_swaps [pending|completed|rejected]\n"
            "/admin_stats\n\n"
            "Кнопки тут:\n"
            "Toggle banned filter — показать только бан\n"
            "Clear search — сбросить поиск\n"
            "Prev/Next — листать страницы"
        )
        return

    if q.data == "adm_users_toggle_banned":
        st["only_banned"] = not bool(st.get("only_banned"))
        st["offset"] = 0
        await _admin_render_users_page(update, context, edit=True)
        return

    if q.data == "adm_users_clear_search":
        st["query"] = ""
        st["offset"] = 0
        await _admin_render_users_page(update, context, edit=True)
        return

    if q.data == "adm_users_prev":
        st["offset"] = max(0, int(st.get("offset") or 0) - limit)
        await _admin_render_users_page(update, context, edit=True)
        return

    if q.data == "adm_users_next":
        st["offset"] = int(st.get("offset") or 0) + limit
        await _admin_render_users_page(update, context, edit=True)
        return


async def admin_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.split(maxsplit=1)
    if len(parts) != 2:
        await update.message.reply_text("Usage: /admin_user <user_id|@username>")
        return

    ref = parts[1].strip()
    u = db.admin_get_user(ref)
    if not u:
        await update.message.reply_text("❌ Usuario no encontrado.")
        return

    uname = (u.get("username") or "").strip()
    uname_line = f"@{uname}" if uname else "—"

    text = (
        "👮 ADMIN — USER\n\n"
        f"ID: {u['user_id']}\n"
        f"Username: {uname_line}\n"
        f"Name: {u.get('display_name','')}\n"
        f"City: {u.get('city','')}\n"
        f"Banned: {int(u.get('is_banned') or 0)}\n"
        f"Rating: {float(u.get('rating') or 0.0):.1f} ({int(u.get('rating_count') or 0)} votes)\n"
        f"Swaps: {int(u.get('total_swaps') or 0)}\n"
        f"Registered: {str(u.get('registered_date',''))[:19]}\n"
    )
    await update.message.reply_text(text)


async def admin_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.split(maxsplit=2)
    if len(parts) < 2:
        await update.message.reply_text("Usage: /admin_ban <user_id|@username> [reason]")
        return

    ref = parts[1].strip()
    reason = parts[2].strip() if len(parts) == 3 else None

    ok = db.admin_ban_user(ref, reason=reason)
    await update.message.reply_text("✅ Banned." if ok else "❌ Failed to ban (user not found?).")


async def admin_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.split(maxsplit=1)
    if len(parts) != 2:
        await update.message.reply_text("Usage: /admin_unban <user_id|@username>")
        return

    ref = parts[1].strip()
    ok = db.admin_unban_user(ref)
    await update.message.reply_text("✅ Unbanned." if ok else "❌ Failed to unban (user not found?).")


async def admin_games(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.split(maxsplit=1)
    if len(parts) != 2:
        await update.message.reply_text("Usage: /admin_games <user_id|@username>")
        return

    ref = parts[1].strip()
    games = db.admin_list_user_games(ref, include_removed=True, limit=50)
    if not games:
        await update.message.reply_text("No games found.")
        return

    msg = "👮 ADMIN — USER GAMES\n\n"
    for g in games:
        msg += (
            f"#{g['game_id']}  [{g.get('status','')}]\n"
            f"🎮 {g['title']}\n"
            f"📱 {g['platform']} | ⭐ {g['condition']}\n"
            f"🔄 {g['looking_for']}\n"
            f"📅 {str(g.get('created_date',''))[:10]}\n\n"
        )
        if len(msg) > 3800:
            msg += "… (truncated)\n"
            break

    msg += "Remove game: /admin_remove_game <game_id>"
    await update.message.reply_text(msg)


async def admin_remove_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    if not update.message or not update.message.text:
        return

    parts = update.message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip().isdigit():
        await update.message.reply_text("Usage: /admin_remove_game <game_id>")
        return

    gid = int(parts[1].strip())
    ok = db.admin_remove_game(gid)
    await update.message.reply_text("✅ Game removed." if ok else "❌ Game not found / not removed.")


async def admin_swaps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    status = None
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) == 2:
            status = parts[1].strip().lower()

    swaps = db.admin_list_swaps(status=status, limit=20, offset=0)
    if not swaps:
        await update.message.reply_text("No swaps found.")
        return

    msg = "👮 ADMIN — SWAPS\n\n"
    for s in swaps:
        msg += (
            f"#{s['swap_id']} [{s.get('status','')}]\n"
            f"u1={s.get('user1_id')}  u2={s.get('user2_id')}\n"
            f"g1={s.get('game1_id')}  g2={s.get('game2_id')}\n"
            f"code={s.get('code')}\n"
            f"created={str(s.get('created_date',''))[:19]}  updated={str(s.get('updated_date',''))[:19]}\n\n"
        )
        if len(msg) > 3800:
            msg += "… (truncated)\n"
            break

    await update.message.reply_text(msg)


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_user(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ No access.")
        return

    st = db.admin_get_stats()
    msg = (
        "📊 ADMIN STATS\n\n"
        f"👥 Users total: {st.get('users_total')}\n"
        f"🚫 Users banned: {st.get('users_banned')}\n"
        f"🎮 Games active: {st.get('games_active')}\n"
        f"⏳ Swaps pending: {st.get('swaps_pending')}\n"
        f"✅ Swaps completed: {st.get('swaps_completed')}\n"
        f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )
    await update.message.reply_text(msg)


# ============================
# HELP
# ============================
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 AYUDA DE GAMESWAP SPAIN\n\n"
        "🎮 COMANDOS PRINCIPALES:\n"
        "/start     — registro / inicio\n"
        "/add       — añadir juego\n"
        "/mygames   — mis juegos\n"
        "/search    — buscar juego\n"
        "/catalog   — catálogo completo\n"
        "/profile   — mi perfil\n"
        "/swap      — confirmar intercambio\n"
        "/help      — esta ayuda\n\n"
        "ℹ️ Nota: Para contactar, el bot usa enlace directo (funciona incluso sin @username).\n"
    )
    await update.message.reply_text(help_text)


# ============================
# STATS (ADMIN old command)
# ============================
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id_val = int(env("ADMIN_ID") or "0")
    if int(update.effective_user.id) != admin_id_val:
        return

    total_users = db.get_total_users()
    total_games = db.get_total_games()
    total_swaps = db.get_total_swaps()

    message = (
        "📊 ESTADÍSTICAS GAMESWAP\n\n"
        f"👥 Usuarios totales: {total_users}\n"
        f"🎮 Juegos activos: {total_games}\n"
        f"🔄 Intercambios completados: {total_swaps}\n"
        f"📅 Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )
    await update.message.reply_text(message)


# ============================
# BOOT
# ============================
def main():
    token = env("BOT_TOKEN")
    if not token:
        logger.error("❌ BOT_TOKEN no está configurado")
        return

    application = Application.builder().token(token).build()

    registration_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REGISTRATION_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, registration_name)],
            REGISTRATION_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, registration_city)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    add_game_handler = ConversationHandler(
        entry_points=[CommandHandler("add", add_game)],
        states={
            ADD_GAME_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_game_title)],
            ADD_GAME_PLATFORM: [CallbackQueryHandler(add_game_platform, pattern="^platform_")],
            ADD_GAME_CONDITION: [CallbackQueryHandler(add_game_condition, pattern="^condition_")],
            ADD_GAME_PHOTO: [
                MessageHandler(filters.PHOTO, add_game_photo),
                CommandHandler("skip", add_game_photo),
            ],
            ADD_GAME_LOOKING: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_game_looking)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    search_handler = ConversationHandler(
        entry_points=[CommandHandler("search", search)],
        states={SEARCH_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_query)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    swap_handler = ConversationHandler(
        entry_points=[CommandHandler("swap", swap_start)],
        states={
            SWAP_SELECT_OWN: [CallbackQueryHandler(swap_select_own, pattern="^swap_offer:")],
            SWAP_INPUT_OTHER_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, swap_input_other_title)],
            SWAP_SELECT_OTHER_GAME: [CallbackQueryHandler(swap_select_other_game, pattern="^(swap_take:|swap_cancel_flow$)")],
            SWAP_CONFIRM: [CallbackQueryHandler(swap_confirm, pattern="^(swap_send|swap_cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    feedback_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(fb_stars_or_skip, pattern="^(fb_stars|fb_skip):")],
        states={
            FB_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, fb_text),
                CommandHandler("skip", fb_text),
            ],
            FB_PHOTOS: [
                MessageHandler(filters.PHOTO, fb_photos),
                MessageHandler(filters.TEXT & ~filters.COMMAND, fb_photos),
                CommandHandler("done", fb_photos),
                CommandHandler("skip", fb_photos),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    application.add_handler(registration_handler)
    application.add_handler(add_game_handler)
    application.add_handler(search_handler)
    application.add_handler(swap_handler)

    application.add_handler(CommandHandler("mygames", my_games))
    application.add_handler(CommandHandler("catalog", catalog))
    application.add_handler(CommandHandler("profile", profile))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", stats))

    application.add_handler(CallbackQueryHandler(swap_accept_or_reject, pattern="^(swap_accept|swap_reject):"))
    application.add_handler(feedback_handler)

    # --- ADMIN minimal set
    application.add_handler(CommandHandler("admin_users", admin_users))
    application.add_handler(CallbackQueryHandler(admin_users_buttons, pattern="^adm_users_"))
    application.add_handler(CommandHandler("admin_user", admin_user))
    application.add_handler(CommandHandler("admin_ban", admin_ban))
    application.add_handler(CommandHandler("admin_unban", admin_unban))
    application.add_handler(CommandHandler("admin_games", admin_games))
    application.add_handler(CommandHandler("admin_remove_game", admin_remove_game))
    application.add_handler(CommandHandler("admin_swaps", admin_swaps))
    application.add_handler(CommandHandler("admin_stats", admin_stats))

    logger.info("🤖 Bot iniciado (polling)")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
