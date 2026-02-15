#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GameSwap Spain Bot
Bot para intercambio de juegos entre gamers
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

# ----------------------------
# DB
# ----------------------------
db = Database()


# ----------------------------
# Helpers
# ----------------------------
def env(name: str) -> str | None:
    """Read env var and normalize: strip spaces and wrapping quotes."""
    v = os.getenv(name)
    if not v:
        return None
    return v.strip().strip('"').strip("'")


def publish_target_chat_id() -> str | int | None:
    """
    Where to publish announcements.
    Priority:
      1) CHANNEL_CHAT_ID (e.g. @GameSwapSpain or -100...)
      2) GROUP_CHAT_ID   (fallback)
    Returns int if numeric, else str (for @username).
    """
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
        logger.info("Published text to %r", chat_id)
    except Exception:
        logger.exception("Failed to publish text to %r", chat_id)


async def safe_publish_photo(context: ContextTypes.DEFAULT_TYPE, photo_file_id: str, caption: str) -> None:
    chat_id = publish_target_chat_id()
    if not chat_id:
        logger.warning("Publish skipped: CHANNEL_CHAT_ID/GROUP_CHAT_ID not set")
        return
    try:
        await context.bot.send_photo(chat_id=chat_id, photo=photo_file_id, caption=caption)
        logger.info("Published photo to %r", chat_id)
    except Exception:
        logger.exception("Failed to publish photo to %r", chat_id)


# ============================
# MAIN COMMANDS
# ============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start - bienvenida y registro"""
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
        status_emoji = "✅" if game.get("status") == "active" else "🔄"
        message += (
            f"{status_emoji} {i}. {game['title']}\n"
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

    message = (
        "👤 TU PERFIL\n\n"
        f"Nombre: {user['display_name']}\n"
        f"Usuario: @{user['username']}\n"
        f"📍 Ciudad: {user['city']}\n"
        f"⭐ Valoración: {user['rating']:.1f}/5.0\n"
        f"🔄 Intercambios completados: {user['total_swaps']}\n"
        f"🎮 Juegos activos: {games_count}\n"
        f"📅 En GameSwap desde: {user['registered_date'][:10]}\n\n"
        "Comandos útiles:\n"
        "/mygames — ver mis juegos\n"
        "/add — añadir juego\n"
        "/search — buscar juego"
    )

    await update.message.reply_text(message)


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
        "/help      — esta ayuda\n\n"
        "❓ ¿CÓMO FUNCIONA?\n\n"
        "1. Añade el juego que quieres intercambiar (/add)\n"
        "2. Busca el juego que necesitas (/search)\n"
        "3. Escribe al dueño por privado\n"
        "4. Quedáis en un lugar público\n"
        "5. Intercambio 1×1\n"
        "6. Valorad el intercambio mutuamente\n\n"
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

    application.add_handler(registration_handler)
    application.add_handler(add_game_handler)
    application.add_handler(search_handler)

    application.add_handler(CommandHandler("mygames", my_games))
    application.add_handler(CommandHandler("catalog", catalog))
    application.add_handler(CommandHandler("profile", profile))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", stats))

    logger.info("🤖 Bot iniciado (polling)")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
