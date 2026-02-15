#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de base de datos para GameSwap Bot
Gestión de base de datos SQLite

Versión: swaps (pending->completed con cambio de dueño) + feedback (rating/comment/photos)
Compatible con Railway (ruta por defecto /data/gameswap.db)

✅ Fix clave (por tu bug actual):
- Username en DB ya NO usa "SinUsuario" como valor real.
- Si no hay username en Telegram -> guardamos "" (string vacío).
- Migración: convierte "SinUsuario" existentes a "".
- Búsqueda por username sigue siendo case-insensitive (COLLATE NOCASE)

✅ Otros fixes:
- conexiones con busy_timeout + WAL (si se puede) + FK ON
- create_user(): si ya existe el user_id, actualiza datos en vez de fallar
- índices útiles + UNIQUE feedback por swap/from_user
"""

import os
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import logging
import random
import string

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_file: Optional[str] = None):
        self.db_file = (db_file or os.getenv("DB_FILE") or "/data/gameswap.db").strip()
        self.init_database()

    # ----------------------------
    # Low-level helpers
    # ----------------------------
    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=30000;")
        except Exception:
            pass
        return conn

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")

    def _gen_swap_code(self) -> str:
        return "SWAP-" + "".join(random.choice(string.digits) for _ in range(6))

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return {r["name"] for r in cur.fetchall()}

    # ----------------------------
    # Username helpers
    # ----------------------------
    def _normalize_username(self, username: str) -> str:
        u = (username or "").strip()
        if u.startswith("@"):
            u = u[1:]
        return u.strip()

    # ----------------------------
    # Schema init + lightweight migrations
    # ----------------------------
    def init_database(self) -> None:
        conn = self.get_connection()
        cur = conn.cursor()

        # WAL (если доступен)
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

        # ---- users
        # IMPORTANT: username can be empty string, so we don't enforce NOT NULL.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                display_name TEXT NOT NULL,
                city TEXT NOT NULL,
                district TEXT,
                rating REAL DEFAULT 0.0,
                total_swaps INTEGER DEFAULT 0,
                registered_date TEXT NOT NULL,
                is_banned INTEGER DEFAULT 0
            )
            """
        )

        # ---- games
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                platform TEXT NOT NULL,
                condition TEXT NOT NULL,
                photo_url TEXT,
                looking_for TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                created_date TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            """
        )

        # ---- swaps
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS swaps (
                swap_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user1_id INTEGER NOT NULL,
                user2_id INTEGER NOT NULL,
                game1_id INTEGER NOT NULL,
                game2_id INTEGER NOT NULL,
                confirmed_by_user1 INTEGER DEFAULT 0,
                confirmed_by_user2 INTEGER DEFAULT 0,
                completed_date TEXT,
                rating_user1 INTEGER,
                rating_user2 INTEGER,
                FOREIGN KEY (user1_id) REFERENCES users (user_id),
                FOREIGN KEY (user2_id) REFERENCES users (user_id),
                FOREIGN KEY (game1_id) REFERENCES games (game_id),
                FOREIGN KEY (game2_id) REFERENCES games (game_id)
            )
            """
        )

        # ---- feedback
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS swap_feedback (
                feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
                swap_id INTEGER NOT NULL,
                from_user_id INTEGER NOT NULL,
                to_user_id INTEGER NOT NULL,
                stars INTEGER NOT NULL CHECK(stars >= 1 AND stars <= 5),
                comment TEXT,
                created_date TEXT NOT NULL,
                FOREIGN KEY (swap_id) REFERENCES swaps (swap_id),
                FOREIGN KEY (from_user_id) REFERENCES users (user_id),
                FOREIGN KEY (to_user_id) REFERENCES users (user_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS swap_feedback_photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feedback_id INTEGER NOT NULL,
                photo_file_id TEXT NOT NULL,
                created_date TEXT NOT NULL,
                FOREIGN KEY (feedback_id) REFERENCES swap_feedback (feedback_id)
            )
            """
        )

        # ---- MIGRATIONS: swaps columns
        cols_swaps = self._table_columns(conn, "swaps")
        if "code" not in cols_swaps:
            cur.execute("ALTER TABLE swaps ADD COLUMN code TEXT")
        if "status" not in cols_swaps:
            cur.execute("ALTER TABLE swaps ADD COLUMN status TEXT DEFAULT 'pending'")
        if "created_date" not in cols_swaps:
            cur.execute("ALTER TABLE swaps ADD COLUMN created_date TEXT")
        if "updated_date" not in cols_swaps:
            cur.execute("ALTER TABLE swaps ADD COLUMN updated_date TEXT")

        # ---- MIGRATIONS: users rating_sum / rating_count
        cols_users = self._table_columns(conn, "users")
        if "rating_sum" not in cols_users:
            cur.execute("ALTER TABLE users ADD COLUMN rating_sum INTEGER DEFAULT 0")
        if "rating_count" not in cols_users:
            cur.execute("ALTER TABLE users ADD COLUMN rating_count INTEGER DEFAULT 0")

        # ---- DATA MIGRATION: old placeholder "SinUsuario" -> ""
        try:
            cur.execute(
                "UPDATE users SET username = '' WHERE username IS NOT NULL AND LOWER(username) = 'sinusuario'"
            )
        except Exception:
            pass

        # ---- Indexes
        # username case-insensitive (still ok if empty)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username_nocase ON users(username COLLATE NOCASE)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_status ON games(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_user_status ON games(user_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_title ON games(title)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_swaps_status ON swaps(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_swaps_user2_status ON swaps(user2_id, status)")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_feedback_once_per_swap "
            "ON swap_feedback(swap_id, from_user_id)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_feedback_to_user ON swap_feedback(to_user_id, created_date)")

        conn.commit()
        conn.close()
        logger.info("✅ Base de datos inicializada (y migrada si hacía falta)")

    # ============================
    # USERS
    # ============================
    def create_user(self, user_id: int, username: str, display_name: str, city: str) -> bool:
        """
        Создаёт пользователя. Если уже существует user_id — обновляет данные.
        Username:
          - если есть @username -> сохраняем (без @)
          - если нет -> сохраняем "" (НЕ "SinUsuario")
        """
        u = self._normalize_username(username)  # may be ""
        dn = (display_name or "SinNombre").strip()
        ct = (city or "SinCiudad").strip()

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")

            cur.execute("SELECT user_id FROM users WHERE user_id = ?", (int(user_id),))
            exists = cur.fetchone() is not None

            if not exists:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, display_name, city, registered_date)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (int(user_id), u, dn, ct, self._now()),
                )
            else:
                cur.execute(
                    """
                    UPDATE users
                    SET username = ?, display_name = ?, city = ?
                    WHERE user_id = ?
                    """,
                    (u, dn, ct, int(user_id)),
                )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ Error al crear/actualizar usuario: %s", e)
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return False

    def get_user(self, user_id: int) -> Optional[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE user_id = ?", (int(user_id),))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ Error al obtener usuario: %s", e)
            return None

    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """
        Buscar usuario por username de Telegram (con o sin @) CASE-INSENSITIVE.
        Nota: si username vacío -> None.
        """
        u = self._normalize_username(username)
        if not u:
            return None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE username = ? COLLATE NOCASE LIMIT 1", (u,))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ Error get_user_by_username: %s", e)
            return None

    def search_users_by_username(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Подсказки по username (LIKE) CASE-INSENSITIVE.
        Пустые usernames не будут в результатах.
        """
        q = self._normalize_username(query)
        if not q:
            return []
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM users
                WHERE username != '' AND username LIKE ? COLLATE NOCASE
                ORDER BY total_swaps DESC, rating DESC
                LIMIT ?
                """,
                (f"%{q}%", int(limit)),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error search_users_by_username: %s", e)
            return []

    def get_total_users(self) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            n = cur.fetchone()[0]
            conn.close()
            return int(n)
        except Exception:
            return 0

    # Legacy method (kept)
    def update_user_rating(self, user_id: int, new_rating: float) -> bool:
        """
        Старый метод: выставляет rating напрямую и увеличивает total_swaps.
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE users
                SET rating = ?, total_swaps = total_swaps + 1
                WHERE user_id = ?
                """,
                (float(new_rating), int(user_id)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ Error al actualizar valoración: %s", e)
            return False

    def apply_user_rating(self, to_user_id: int, stars: int) -> bool:
        """rating_sum += stars; rating_count += 1; rating = rating_sum / rating_count"""
        if stars < 1 or stars > 5:
            return False

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")

            cur.execute("SELECT rating_sum, rating_count FROM users WHERE user_id = ?", (int(to_user_id),))
            row = cur.fetchone()
            if not row:
                cur.execute("ROLLBACK")
                conn.close()
                return False

            rs = int(row["rating_sum"] or 0) + int(stars)
            rc = int(row["rating_count"] or 0) + 1
            rating = rs / rc if rc else 0.0

            cur.execute(
                """
                UPDATE users
                SET rating_sum = ?, rating_count = ?, rating = ?
                WHERE user_id = ?
                """,
                (rs, rc, float(rating), int(to_user_id)),
            )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ Error apply_user_rating: %s", e)
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return False

    # ============================
    # GAMES
    # ============================
    def add_game(
        self,
        user_id: int,
        title: str,
        platform: str,
        condition: str,
        photo_url: Optional[str],
        looking_for: str,
    ) -> Optional[int]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO games (user_id, title, platform, condition, photo_url, looking_for, created_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), str(title).strip(), str(platform).strip(), str(condition).strip(), photo_url, str(looking_for).strip(), self._now()),
            )
            game_id = cur.lastrowid
            conn.commit()
            conn.close()
            return int(game_id)
        except Exception as e:
            logger.error("❌ Error al añadir juego: %s", e)
            return None

    def get_game(self, game_id: int) -> Optional[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM games WHERE game_id = ?", (int(game_id),))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ Error al obtener juego: %s", e)
            return None

    def get_user_games(self, user_id: int) -> List[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM games
                WHERE user_id = ? AND status = 'active'
                ORDER BY created_date DESC
                """,
                (int(user_id),),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error al obtener juegos del usuario: %s", e)
            return []

    def get_user_active_games(self, user_id: int, limit: int = 50) -> List[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM games
                WHERE user_id = ? AND status = 'active'
                ORDER BY created_date DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error get_user_active_games: %s", e)
            return []

    def get_all_active_games(self) -> List[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM games
                WHERE status = 'active'
                ORDER BY created_date DESC
                """
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error al obtener todos los juegos activos: %s", e)
            return []

    def search_games(self, query: str) -> List[Dict]:
        try:
            q = (query or "").strip()
            if not q:
                return []

            conn = self.get_connection()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT g.*
                FROM games g
                JOIN users u ON g.user_id = u.user_id
                WHERE g.status = 'active'
                  AND g.title LIKE ? COLLATE NOCASE
                ORDER BY u.total_swaps DESC, u.rating DESC, g.created_date DESC
                """,
                (f"%{q}%",),
            )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]

        except Exception as e:
            logger.error("❌ Error search_games: %s", e)
            return []


    def remove_game(self, game_id: int, user_id: int) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE games
                SET status = 'removed'
                WHERE game_id = ? AND user_id = ?
                """,
                (int(game_id), int(user_id)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ Error al eliminar juego: %s", e)
            return False

    def get_total_games(self) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM games WHERE status = 'active'")
            n = cur.fetchone()[0]
            conn.close()
            return int(n)
        except Exception:
            return 0

    # ============================
    # SWAPS
    # ============================
    def create_swap_request(
        self, user1_id: int, user2_id: int, game1_id: int, game2_id: int
    ) -> Optional[Tuple[int, str]]:

        if int(user1_id) == int(user2_id):
            return None

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")

            # Проверка существования игр
            cur.execute(
                "SELECT game_id, user_id, status FROM games WHERE game_id IN (?, ?)",
                (int(game1_id), int(game2_id)),
            )
            rows = cur.fetchall()
            if len(rows) != 2:
                conn.rollback()
                conn.close()
                return None

            info = {int(r["game_id"]): dict(r) for r in rows}

            if info[game1_id]["user_id"] != int(user1_id):
                conn.rollback()
                conn.close()
                return None

            if info[game2_id]["user_id"] != int(user2_id):
                conn.rollback()
                conn.close()
                return None

            if info[game1_id]["status"] != "active" or info[game2_id]["status"] != "active":
                conn.rollback()
                conn.close()
                return None

            # Проверяем, нет ли уже pending swap
            cur.execute(
                """
                SELECT swap_id FROM swaps
                WHERE status = 'pending'
                  AND (
                        (game1_id = ? AND game2_id = ?)
                     OR (game1_id = ? AND game2_id = ?)
                  )
                """,
                (game1_id, game2_id, game2_id, game1_id),
            )
            if cur.fetchone():
                conn.rollback()
                conn.close()
                return None

            code = self._gen_swap_code()
            now = self._now()

            cur.execute(
                """
                INSERT INTO swaps (
                    user1_id, user2_id, game1_id, game2_id,
                    confirmed_by_user1, confirmed_by_user2,
                    status, code, created_date, updated_date
                )
                VALUES (?, ?, ?, ?, 1, 0, 'pending', ?, ?, ?)
                """,
                (user1_id, user2_id, game1_id, game2_id, code, now, now),
            )

            swap_id = cur.lastrowid
            conn.commit()
            conn.close()

            return int(swap_id), code

        except Exception as e:
            if conn:
                conn.rollback()
                conn.close()
            logger.error("❌ Error create_swap_request: %s", e)
            return None


    def get_swap(self, swap_id: int) -> Optional[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM swaps WHERE swap_id = ?", (int(swap_id),))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ Error al obtener intercambio: %s", e)
            return None

    def set_swap_status(self, swap_id: int, status: str) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE swaps SET status = ?, updated_date = ? WHERE swap_id = ?",
                (str(status), self._now(), int(swap_id)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ Error al actualizar estado del intercambio: %s", e)
            return False

    def complete_swap(self, swap_id: int, confirmer_user_id: int) -> Tuple[bool, str]:
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")

            cur.execute("SELECT * FROM swaps WHERE swap_id = ?", (int(swap_id),))
            row = cur.fetchone()

            if not row:
                conn.rollback()
                return False, "swap not found"

            swap = dict(row)

            if swap["status"] != "pending":
                conn.rollback()
                return False, "swap not pending"

            if int(confirmer_user_id) != int(swap["user2_id"]):
                conn.rollback()
                return False, "only recipient can confirm"

            g1_id = int(swap["game1_id"])
            g2_id = int(swap["game2_id"])

            cur.execute(
                "SELECT game_id, user_id, status FROM games WHERE game_id IN (?, ?)",
                (g1_id, g2_id),
            )
            games = cur.fetchall()

            if len(games) != 2:
                conn.rollback()
                return False, "games missing"

            g = {int(r["game_id"]): dict(r) for r in games}

            if g[g1_id]["status"] != "active" or g[g2_id]["status"] != "active":
                conn.rollback()
                return False, "game not active"

            if g[g1_id]["user_id"] != swap["user1_id"] or g[g2_id]["user_id"] != swap["user2_id"]:
                conn.rollback()
                return False, "ownership changed"

            # Меняем владельцев
            cur.execute("UPDATE games SET user_id = ? WHERE game_id = ?", (swap["user2_id"], g1_id))
            cur.execute("UPDATE games SET user_id = ? WHERE game_id = ?", (swap["user1_id"], g2_id))

            now = self._now()

            cur.execute(
                """
                UPDATE swaps
                SET confirmed_by_user2 = 1,
                    status = 'completed',
                    completed_date = ?,
                    updated_date = ?
                WHERE swap_id = ?
                """,
                (now, now, swap_id),
            )

            cur.execute(
                "UPDATE users SET total_swaps = total_swaps + 1 WHERE user_id IN (?, ?)",
                (swap["user1_id"], swap["user2_id"]),
            )

            conn.commit()
            conn.close()
            return True, ""

        except Exception as e:
            if conn:
                conn.rollback()
                conn.close()
            return False, str(e)


    def get_total_swaps(self) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM swaps WHERE status = 'completed'")
            n = cur.fetchone()[0]
            conn.close()
            return int(n)
        except Exception:
            return 0

    # ============================
    # FEEDBACK
    # ============================
    def add_feedback(
        self,
        swap_id: int,
        from_user_id: int,
        to_user_id: int,
        stars: int,
        comment: Optional[str],
    ) -> Optional[int]:
        if stars < 1 or stars > 5:
            return None

        comment_norm = None
        if comment is not None:
            comment_norm = str(comment).strip()[:800] or None

        try:
            conn = self.get_connection()
            cur = conn.cursor()

            cur.execute(
                "SELECT feedback_id FROM swap_feedback WHERE swap_id = ? AND from_user_id = ?",
                (int(swap_id), int(from_user_id)),
            )
            if cur.fetchone():
                conn.close()
                return None

            cur.execute(
                """
                INSERT INTO swap_feedback (swap_id, from_user_id, to_user_id, stars, comment, created_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(swap_id), int(from_user_id), int(to_user_id), int(stars), comment_norm, self._now()),
            )
            feedback_id = cur.lastrowid
            conn.commit()
            conn.close()

            self.apply_user_rating(to_user_id=int(to_user_id), stars=int(stars))
            return int(feedback_id)
        except Exception as e:
            logger.error("❌ Error add_feedback: %s", e)
            return None

    def add_feedback_photo(self, feedback_id: int, photo_file_id: str) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO swap_feedback_photos (feedback_id, photo_file_id, created_date)
                VALUES (?, ?, ?)
                """,
                (int(feedback_id), str(photo_file_id), self._now()),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ Error add_feedback_photo: %s", e)
            return False

    def get_feedback_photos(self, feedback_id: int) -> List[str]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT photo_file_id FROM swap_feedback_photos
                WHERE feedback_id = ?
                ORDER BY id ASC
                """,
                (int(feedback_id),),
            )
            rows = cur.fetchall()
            conn.close()
            return [r["photo_file_id"] for r in rows]
        except Exception as e:
            logger.error("❌ Error get_feedback_photos: %s", e)
            return []

    def get_user_feedback_summary(self, user_id: int) -> Dict:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT rating, rating_count FROM users WHERE user_id = ?", (int(user_id),))
            row = cur.fetchone()
            conn.close()
            if not row:
                return {"rating": 0.0, "rating_count": 0}
            return {"rating": float(row["rating"] or 0.0), "rating_count": int(row["rating_count"] or 0)}
        except Exception:
            return {"rating": 0.0, "rating_count": 0}

    def get_user_feedback(self, user_id: int, limit: int = 20) -> List[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT *
                FROM swap_feedback
                WHERE to_user_id = ?
                ORDER BY created_date DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error get_user_feedback: %s", e)
            return []

    # ============================
    # ADMIN HELPERS (minimal set)
    # ============================
    def admin_list_users(
        self,
        limit: int = 10,
        offset: int = 0,
        only_banned: bool = False,
        query: Optional[str] = None,
    ) -> List[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            q = (query or "").strip()
            if q.startswith("@"):
                q = q[1:]
            params = []
            where = []

            if only_banned:
                where.append("is_banned = 1")

            if q:
                where.append(
                    "("
                    "username LIKE ? COLLATE NOCASE OR "
                    "display_name LIKE ? COLLATE NOCASE OR "
                    "city LIKE ? COLLATE NOCASE"
                    ")"
                )
                like = f"%{q}%"
                params.extend([like, like, like])

            where_sql = ("WHERE " + " AND ".join(where)) if where else ""

            cur.execute(
                f"""
                SELECT user_id, username, display_name, city, rating, rating_count, total_swaps, is_banned, registered_date
                FROM users
                {where_sql}
                ORDER BY registered_date DESC
                LIMIT ? OFFSET ?
                """,
                (*params, int(limit), int(offset)),
            )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error admin_list_users: %s", e)
            return []

    def admin_count_users(self, only_banned: bool = False, query: Optional[str] = None) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            q = (query or "").strip()
            if q.startswith("@"):
                q = q[1:]

            params = []
            where = []

            if only_banned:
                where.append("is_banned = 1")

            if q:
                where.append(
                    "("
                    "username LIKE ? COLLATE NOCASE OR "
                    "display_name LIKE ? COLLATE NOCASE OR "
                    "city LIKE ? COLLATE NOCASE"
                    ")"
                )
                like = f"%{q}%"
                params.extend([like, like, like])

            where_sql = ("WHERE " + " AND ".join(where)) if where else ""

            cur.execute(
                f"SELECT COUNT(*) FROM users {where_sql}",
                tuple(params),
            )
            n = cur.fetchone()[0]
            conn.close()
            return int(n)
        except Exception as e:
            logger.error("❌ Error admin_count_users: %s", e)
            return 0

    def admin_get_user(self, user_ref: str) -> Optional[Dict]:
        if user_ref is None:
            return None

        s = str(user_ref).strip()
        if not s:
            return None

        if s.isdigit():
            return self.get_user(int(s))

        return self.get_user_by_username(s)

    def admin_ban_user(self, user_ref: str, reason: Optional[str] = None) -> bool:
        u = self.admin_get_user(user_ref)
        if not u:
            return False
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (int(u["user_id"]),))
            conn.commit()
            conn.close()
            logger.warning("🚫 ADMIN BAN user_id=%s username=%s reason=%s", u["user_id"], u.get("username"), reason)
            return True
        except Exception as e:
            logger.error("❌ Error admin_ban_user: %s", e)
            return False

    def admin_unban_user(self, user_ref: str) -> bool:
        u = self.admin_get_user(user_ref)
        if not u:
            return False
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (int(u["user_id"]),))
            conn.commit()
            conn.close()
            logger.warning("✅ ADMIN UNBAN user_id=%s username=%s", u["user_id"], u.get("username"))
            return True
        except Exception as e:
            logger.error("❌ Error admin_unban_user: %s", e)
            return False

    def admin_list_user_games(self, user_ref: str, include_removed: bool = True, limit: int = 50) -> List[Dict]:
        u = self.admin_get_user(user_ref)
        if not u:
            return []
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            if include_removed:
                cur.execute(
                    """
                    SELECT * FROM games
                    WHERE user_id = ?
                    ORDER BY created_date DESC
                    LIMIT ?
                    """,
                    (int(u["user_id"]), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM games
                    WHERE user_id = ? AND status = 'active'
                    ORDER BY created_date DESC
                    LIMIT ?
                    """,
                    (int(u["user_id"]), int(limit)),
                )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error admin_list_user_games: %s", e)
            return []

    def admin_remove_game(self, game_id: int) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE games SET status = 'removed' WHERE game_id = ?",
                (int(game_id),),
            )
            conn.commit()
            changed = cur.rowcount
            conn.close()
            return changed > 0
        except Exception as e:
            logger.error("❌ Error admin_remove_game: %s", e)
            return False

    def admin_list_swaps(self, status: Optional[str] = None, limit: int = 20, offset: int = 0) -> List[Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            if status:
                cur.execute(
                    """
                    SELECT * FROM swaps
                    WHERE status = ?
                    ORDER BY COALESCE(updated_date, created_date) DESC
                    LIMIT ? OFFSET ?
                    """,
                    (str(status), int(limit), int(offset)),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM swaps
                    ORDER BY COALESCE(updated_date, created_date) DESC
                    LIMIT ? OFFSET ?
                    """,
                    (int(limit), int(offset)),
                )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ Error admin_list_swaps: %s", e)
            return []

    def admin_get_stats(self) -> Dict:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM users")
            users_total = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
            users_banned = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM games WHERE status = 'active'")
            games_active = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM swaps WHERE status = 'pending'")
            swaps_pending = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM swaps WHERE status = 'completed'")
            swaps_completed = int(cur.fetchone()[0])

            conn.close()
            return {
                "users_total": users_total,
                "users_banned": users_banned,
                "games_active": games_active,
                "swaps_pending": swaps_pending,
                "swaps_completed": swaps_completed,
            }
        except Exception as e:
            logger.error("❌ Error admin_get_stats: %s", e)
            return {
                "users_total": 0,
                "users_banned": 0,
                "games_active": 0,
                "swaps_pending": 0,
                "swaps_completed": 0,
            }
