#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GameSwap Spain Bot
Bot para intercambio de juegos entre gamers

Versión: tu código + Swap (Variant A) + Feedback (rating + comment + photos) tras swap completado.
Requiere database.py (con tablas swap_feedback y swap_feedback_photos + add_feedback() que ya aplica rating).

✅ Fixes importantes en esta versión:
- Feedback: NO vuelve a llamar apply_user_rating() (ya se hace dentro de db.add_feedback()).
- Feedback: se guarda por usuario (chat_data) y NO pisa feedback de otros swaps.
- Feedback: soporta varios swaps en paralelo y evita mezclar sesiones.
- /skip y /done funcionan correctamente (sin pasar a handler equivocado).
- Botones de estrellas usan label consistente.
"""

import os
import logging
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

# Swap flow states
SWAP_SELECT_OWN, SWAP_SEARCH_OTHER, SWAP_SELECT_OTHER, SWAP_CONFIRM = range(4)

# Feedback flow states (after swap completed)
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


async def safe_publish_text(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    chat_id = publish_target_chat_id()
    if not chat_id:
        logger.warning("Publish skipped: CHANNEL_CHAT_ID/GROUP_CHAT_ID not set")
        return
    try:
        await context.bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        logger.exception("Failed to publish text to %r", chat_id)


async def safe_publish_photo(context: ContextTypes.DEFAULT_TYPE, photo_file_id: str, caption: str) -> None:
    chat_id = publish_target_chat_id()
    if not chat_id:
        logger.warning("Publish skipped: CHANNEL_CHAT_ID/GROUP_CHAT_ID not set")
        return
    try:
        await context.bot.send_photo(chat_id=chat_id, photo=photo_file_id, caption=caption)
    except Exception:
        logger.exception("Failed to publish photo to %r", chat_id)


def fmt_game(g: dict) -> str:
    return f"{g['title']} ({g['platform']}, {g['condition']})"


def fmt_user(u: dict) -> str:
    username = (u.get("username") or "SinUsuario").strip()
    if not username.startswith("@"):
        username = "@" + username
    return username


def stars_label(n: int) -> str:
    n = max(1, min(5, int(n)))
    return "⭐" * n + "☆" * (5 - n)


def _fb_key(swap_id: int, to_user_id: int) -> str:
    return f"fb:{int(swap_id)}:{int(to_user_id)}"


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
            f"⭐ Valoración: {user['rating']:.1f}/5.0\n"
            f"🔄 Intercambios completados: {user['total_swaps']}\n\n"
            f"Usa estos comandos:\n"
            f"/add - añadir un juego\n"
            f"/mygames - mis juegos\n"
            f"/search - buscar juego\n"
            f"/catalog - ver catálogo completo\n"
            f"/profile - mi perfil\n"
            f"/swap - confirmar intercambio\n"
            f"/help - ayuda"
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
    context.user_data["display_name"] = update.message.text.strip()

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
    city = update.message.text.strip()

    if city == "Otra ciudad 📝":
        await update.message.reply_text("Escribe el nombre de tu ciudad:", reply_markup=ReplyKeyboardRemove())
        return REGISTRATION_CITY

    user_id = update.effective_user.id
    username = update.effective_user.username or "SinUsuario"
    display_name = context.user_data.get("display_name", "SinNombre")

    db.create_user(user_id, username, display_name, city)

    await update.message.reply_text(
        f"✅ ¡Registro completado!\n\n"
        f"👤 Nombre: {display_name}\n"
        f"📍 Ciudad: {city}\n\n"
        f"Ahora puedes:\n"
        f"/add — añadir juego para intercambio\n"
        f"/search — buscar juego\n"
        f"/catalog — ver todos los juegos disponibles\n"
        f"/swap — confirmar intercambio\n"
        f"/help — obtener ayuda",
        reply_markup=ReplyKeyboardRemove(),
    )

    await safe_publish_text(
        context,
        text=(
            "👋 ¡Nuevo miembro!\n\n"
            f"👤 {display_name} ({city}) se ha unido a GameSwap Spain\n"
            f"Total de usuarios: {db.get_total_users()}"
        ),
    )

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Operación cancelada.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ============================
# ADD GAME
# ============================
async def add_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    context.user_data["game_title"] = update.message.text.strip()

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

    context.user_data["game_platform"] = platform_map[query.data]

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
    context.user_data["game_condition"] = condition_map[query.data]

    await query.edit_message_text(
        f"📝 Juego: {context.user_data['game_title']}\n"
        f"🎮 Plataforma: {context.user_data['game_platform']}\n"
        f"⭐ Estado: {context.user_data['game_condition']}\n\n"
        "📸 Sube una foto del disco (con caja si la tienes)\n"
        "O escribe /skip para omitir"
    )
    return ADD_GAME_PHOTO


async def add_game_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip().lower() in {"/skip", "skip"}:
        context.user_data["game_photo"] = None
    elif update.message.photo:
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
    context.user_data["game_looking_for"] = update.message.text.strip()

    user_id = update.effective_user.id
    db.add_game(
        user_id=user_id,
        title=context.user_data["game_title"],
        platform=context.user_data["game_platform"],
        condition=context.user_data["game_condition"],
        photo_url=context.user_data.get("game_photo"),
        looking_for=context.user_data["game_looking_for"],
    )

    user = db.get_user(user_id)

    await update.message.reply_text(
        "✅ ¡Juego añadido al catálogo!\n\n"
        f"🎮 {context.user_data['game_title']}\n"
        f"📱 {context.user_data['game_platform']}\n"
        f"⭐ {context.user_data['game_condition']}\n"
        f"🔄 Busco: {context.user_data['game_looking_for']}\n\n"
        "Tus juegos → /mygames\n"
        "Añadir otro → /add"
    )

    message_text = (
        "🆕 ¡NUEVO JUEGO EN EL CATÁLOGO!\n\n"
        f"🎮 {context.user_data['game_title']}\n"
        f"📱 {context.user_data['game_platform']}\n"
        f"⭐ Estado: {context.user_data['game_condition']}\n"
        f"🔄 Busca: {context.user_data['game_looking_for']}\n\n"
        f"👤 Propietario: @{user['username']}\n"
        f"📍 Ciudad: {user['city']}\n"
        f"⭐ Valoración: {user['rating']:.1f} ({user['total_swaps']} intercambios)\n\n"
        f"💬 Contactar: @{user['username']}"
    )

    photo_id = context.user_data.get("game_photo")
    if photo_id:
        await safe_publish_photo(context, photo_file_id=photo_id, caption=message_text)
    else:
        await safe_publish_text(context, text=message_text)

    return ConversationHandler.END


# ============================
# MY GAMES
# ============================
async def my_games(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await update.message.reply_text(
        "🔍 BUSCAR JUEGO\n\n"
        "Escribe el nombre del juego que estás buscando:\n"
        "(ejemplo: Elden Ring)\n\n"
        "O escribe /cancel para cancelar"
    )
    return SEARCH_QUERY


async def search_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.message.text.strip()
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

    message = f"🔍 RESULTADOS PARA: «{q}»\nEncontrados: {len(results)}\n\n"

    shown = 0
    for game in results:
        if game["user_id"] == user_id:
            continue
        owner = db.get_user(game["user_id"])
        message += (
            f"🎮 {game['title']}\n"
            f"📱 {game['platform']}  |  ⭐ {game['condition']}\n"
            f"🔄 Busca: {game['looking_for']}\n"
            f"👤 @{owner['username']} ({owner['city']})\n"
            f"⭐ {owner['rating']:.1f}/5.0  ({owner['total_swaps']} intercambios)\n"
            f"💬 Contacto: @{owner['username']}\n\n"
        )
        shown += 1
        if len(message) > 3800:
            break

    if shown == 0:
        await update.message.reply_text("No hay juegos de otros usuarios que coincidan con tu búsqueda.")
        return ConversationHandler.END

    if len(results) > shown:
        message += f"… y {len(results) - shown} resultados más"

    await update.message.reply_text(message)
    return ConversationHandler.END


# ============================
# CATALOG
# ============================
async def catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    games = db.get_all_active_games()

    if not games:
        await update.message.reply_text("📦 El catálogo está vacío por ahora.\n\n¡Sé el primero! → /add")
        return

    platforms: dict[str, list] = {}
    for game in games:
        if game["user_id"] == user_id:
            continue
        platforms.setdefault(game["platform"], []).append(game)

    message = f"📚 CATÁLOGO COMPLETO ({len(games)} juegos)\n\n"
    for platform, games_list in platforms.items():
        message += f"🎮 {platform} ({len(games_list)}):\n"
        for game in games_list[:5]:
            owner = db.get_user(game["user_id"])
            message += f" • {game['title']} (@{owner['username']})\n"
        if len(games_list) > 5:
            message += f"   … y otros {len(games_list) - 5}\n"
        message += "\n"

    message += "Para buscar un juego concreto usa:\n/search [nombre]"
    await update.message.reply_text(message)


# ============================
# PROFILE
# ============================
async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = db.get_user(user_id)

    if not user:
        await update.message.reply_text("⚠️ Primero regístrate → /start")
        return

    games_count = len(db.get_user_games(user_id))

    summary = db.get_user_feedback_summary(user_id)
    rating = summary.get("rating", float(user.get("rating") or 0.0))
    rating_count = summary.get("rating_count", 0)

    message = (
        "👤 TU PERFIL\n\n"
        f"Nombre: {user['display_name']}\n"
        f"Usuario: @{user['username']}\n"
        f"📍 Ciudad: {user['city']}\n"
        f"⭐ Valoración: {rating:.1f}/5.0 ({rating_count} votos)\n"
        f"🔄 Intercambios completados: {user['total_swaps']}\n"
        f"🎮 Juegos activos: {games_count}\n"
        f"📅 En GameSwap desde: {user['registered_date'][:10]}\n\n"
        "Comandos útiles:\n"
        "/mygames — ver mis juegos\n"
        "/add — añadir juego\n"
        "/search — buscar juego\n"
        "/swap — confirmar intercambio"
    )

    await update.message.reply_text(message)


# ============================
# SWAP FLOW (Variant A)
# ============================
async def swap_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    if not user:
        await update.message.reply_text("⚠️ Primero debes registrarte → /start")
        return ConversationHandler.END

    my_games = db.get_user_games(user_id)
    if not my_games:
        await update.message.reply_text("📦 No tienes juegos activos. Añade uno → /add")
        return ConversationHandler.END

    keyboard = []
    for g in my_games[:20]:
        keyboard.append([InlineKeyboardButton(fmt_game(g), callback_data=f"swap_offer:{g['game_id']}")])

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
    if not g or g["user_id"] != update.effective_user.id:
        await query.edit_message_text("❌ Ese juego no existe o no es tuyo.")
        return ConversationHandler.END

    context.user_data["swap_offered_game_id"] = game_id

    await query.edit_message_text(
        "Paso 2/3 — Escribe el nombre del juego que recibiste (del otro usuario).\n\n"
        "Ejemplo: Elden Ring\n\n"
        "Puedes cancelar con /cancel"
    )
    return SWAP_SEARCH_OTHER


async def swap_search_other(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user_id = update.effective_user.id

    results = db.search_games(text)
    results = [g for g in results if g["user_id"] != user_id]  # other users only

    if not results:
        await update.message.reply_text(f"😔 No encontré «{text}» en el catálogo.\n\nPrueba con otro nombre.")
        return SWAP_SEARCH_OTHER

    results = results[:15]
    keyboard = []
    for g in results:
        owner = db.get_user(g["user_id"])
        keyboard.append(
            [InlineKeyboardButton(f"{fmt_game(g)} — {fmt_user(owner)}", callback_data=f"swap_take:{g['game_id']}")]
        )

    await update.message.reply_text("Paso 3/3 — Elige el juego que recibiste:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SWAP_SELECT_OTHER


async def swap_select_other(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, game_id_str = query.data.split(":")
    requested_game_id = int(game_id_str)

    offered_game_id = context.user_data.get("swap_offered_game_id")
    if not offered_game_id:
        await query.edit_message_text("❌ Sesión caducada. Empieza de nuevo: /swap")
        return ConversationHandler.END

    offered = db.get_game(offered_game_id)
    requested = db.get_game(requested_game_id)

    if not offered or not requested:
        await query.edit_message_text("❌ Juego no encontrado.")
        return ConversationHandler.END

    if offered["user_id"] != update.effective_user.id:
        await query.edit_message_text("❌ El juego ofrecido no es tuyo.")
        return ConversationHandler.END

    if requested["user_id"] == update.effective_user.id:
        await query.edit_message_text("❌ No puedes intercambiar contigo mismo.")
        return ConversationHandler.END

    context.user_data["swap_requested_game_id"] = requested_game_id

    owner = db.get_user(requested["user_id"])

    confirm_text = (
        "🔄 CONFIRMAR INTERCAMBIO\n\n"
        f"Tú das:  🎮 {fmt_game(offered)}\n"
        f"Tú recibes: 🎮 {fmt_game(requested)}\n\n"
        f"Con: {fmt_user(owner)}\n\n"
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

    offered = db.get_game(offered_game_id)
    requested = db.get_game(requested_game_id)
    if not offered or not requested:
        await query.edit_message_text("❌ Juego no encontrado.")
        return ConversationHandler.END

    initiator_id = update.effective_user.id
    recipient_id = requested["user_id"]

    created = db.create_swap_request(
        user1_id=initiator_id,
        user2_id=recipient_id,
        game1_id=offered_game_id,
        game2_id=requested_game_id,
    )
    if not created:
        await query.edit_message_text("❌ No se pudo crear la solicitud. (¿Juegos cambiaron o no están activos?)")
        return ConversationHandler.END

    swap_id, code = created
    initiator = db.get_user(initiator_id)

    await query.edit_message_text(
        "✅ Solicitud enviada.\n\n"
        f"📌 Código: {code}\n"
        "El otro usuario debe confirmarlo en el bot."
    )

    msg = (
        "🔔 SOLICITUD DE INTERCAMBIO\n\n"
        f"{fmt_user(initiator)} propone:\n\n"
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

    user_id = update.effective_user.id
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
            await context.bot.send_message(chat_id=int(swap["user1_id"]), text="❌ Tu solicitud de intercambio fue rechazada.")
        except Exception:
            logger.exception("Failed to notify initiator about rejection")
        return

    ok, err = db.complete_swap(swap_id, confirmer_user_id=user_id)
    if not ok:
        await query.edit_message_text(f"❌ No se pudo completar: {err}")
        return

    await query.edit_message_text("✅ Intercambio confirmado. ¡Listo!")

    # Notify initiator
    try:
        await context.bot.send_message(
            chat_id=int(swap["user1_id"]),
            text="✅ Tu intercambio fue confirmado. Los juegos cambiaron de dueño.",
        )
    except Exception:
        logger.exception("Failed to notify initiator after swap completion")

    # Optional: publish to channel/group
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
                    f"{fmt_user(u1)} ↔ {fmt_user(u2)}\n"
                    f"🎮 {g1['title']} ⇄ 🎮 {g2['title']}"
                ),
            )
    except Exception:
        logger.exception("Failed to publish swap completion")

    # Start feedback flow for BOTH users (in DM)
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
    context: ContextTypes.DEFAULT_TYPE, rater_user_id: int, ratee_user_id: int, swap_id: int
):
    """Send DM to rater with inline stars."""
    ratee = db.get_user(ratee_user_id) or {"username": "SinUsuario"}
    text = (
        "⭐ VALORACIÓN DEL INTERCAMBIO\n\n"
        f"Valora a {fmt_user(ratee)}.\n"
        "Elige estrellas:"
    )
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(stars_label(i), callback_data=f"fb_stars:{swap_id}:{ratee_user_id}:{i}")] for i in range(5, 0, -1)]
        + [[InlineKeyboardButton("Omitir", callback_data=f"fb_skip:{swap_id}:{ratee_user_id}")]]
    )
    await context.bot.send_message(chat_id=rater_user_id, text=text, reply_markup=kb)


async def fb_stars_or_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback when user selects stars or skip.
    We store a per-chat pending feedback session in context.chat_data.
    """
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    action = parts[0]  # fb_stars or fb_skip
    swap_id = int(parts[1])
    ratee_user_id = int(parts[2])
    rater_user_id = update.effective_user.id

    if action == "fb_skip":
        await query.edit_message_text("👍 Ok, sin valoración.")
        return ConversationHandler.END

    stars = int(parts[3])
    key = _fb_key(swap_id, ratee_user_id)

    # Store session in chat_data (safe for parallel users)
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

    # cleanup only this session
    context.chat_data.pop(key, None)
    context.chat_data.pop("fb_active_key", None)

    return ConversationHandler.END


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
        "❓ ¿CÓMO FUNCIONA?\n\n"
        "1. Añade el juego que quieres intercambiar (/add)\n"
        "2. Busca el juego que necesitas (/search)\n"
        "3. Escribe al dueño por privado\n"
        "4. Quedáis en un lugar público\n"
        "5. Intercambio 1×1\n"
        "6. Confirmad el intercambio con /swap\n"
        "7. Dejad valoración (⭐) después del swap\n\n"
        "🛡️ SEGURIDAD:\n"
        "• Quedar siempre en sitios concurridos\n"
        "• Comprobar el disco antes de entregar\n"
        "• Fijarse en la valoración del usuario\n"
        "• Cualquier problema → escribir al admin\n\n"
        "💬 ¿Dudas? Escribe a @tu_usuario_admin"
    )
    await update.message.reply_text(help_text)


# ============================
# STATS (ADMIN)
# ============================
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = int(env("ADMIN_ID") or "0")
    if update.effective_user.id != admin_id:
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
            SWAP_SEARCH_OTHER: [MessageHandler(filters.TEXT & ~filters.COMMAND, swap_search_other)],
            SWAP_SELECT_OTHER: [CallbackQueryHandler(swap_select_other, pattern="^swap_take:")],
            SWAP_CONFIRM: [CallbackQueryHandler(swap_confirm, pattern="^(swap_send|swap_cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Feedback handler: entry via callback fb_stars / fb_skip, then messages
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

    # accept/reject callbacks (swap)
    application.add_handler(CallbackQueryHandler(swap_accept_or_reject, pattern="^(swap_accept|swap_reject):"))

    # feedback conversation
    application.add_handler(feedback_handler)

    logger.info("🤖 Bot iniciado (polling)")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
