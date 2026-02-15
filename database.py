#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de base de datos para GameSwap Bot
Gestión de base de datos SQLite
"""

import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import logging
import random
import string

logger = logging.getLogger(__name__)


class Database:
    """Clase para gestionar la base de datos"""

    def __init__(self, db_file: str = "/data/gameswap.db"):
        """Inicialización de la base de datos"""
        self.db_file = db_file
        self.init_database()

    def get_connection(self):
        """Obtener conexión a la base de datos"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")

    def _gen_swap_code(self) -> str:
        # SWAP- + 6 digits
        return "SWAP-" + "".join(random.choice(string.digits) for _ in range(6))

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall()
        return {r["name"] for r in rows}

    def init_database(self):
        """Crear tablas si no existen + migraciones simples"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Tabla de usuarios
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
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

        # Tabla de juegos
        cursor.execute(
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

        # Tabla de intercambios (ya existe en tu proyecto; la dejamos y la ampliamos)
        cursor.execute(
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

        # ---- MIGRACIONES: añadimos columnas nuevas si faltan ----
        cols = self._table_columns(conn, "swaps")

        # code, status, created_date, updated_date
        if "code" not in cols:
            cursor.execute("ALTER TABLE swaps ADD COLUMN code TEXT")
        if "status" not in cols:
            cursor.execute("ALTER TABLE swaps ADD COLUMN status TEXT DEFAULT 'pending'")
        if "created_date" not in cols:
            cursor.execute("ALTER TABLE swaps ADD COLUMN created_date TEXT")
        if "updated_date" not in cols:
            cursor.execute("ALTER TABLE swaps ADD COLUMN updated_date TEXT")

        conn.commit()
        conn.close()
        logger.info("✅ Base de datos inicializada (y migrada si hacía falta)")

    # ============= USUARIOS =============

    def create_user(self, user_id: int, username: str, display_name: str, city: str) -> bool:
        """Crear un nuevo usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO users (user_id, username, display_name, city, registered_date)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, username, display_name, city, self._now()),
            )

            conn.commit()
            conn.close()
            logger.info(f"✅ Usuario {display_name} creado")
            return True
        except Exception as e:
            logger.error(f"❌ Error al crear usuario: {e}")
            return False

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Obtener datos de un usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            conn.close()

            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Error al obtener usuario: {e}")
            return None

    def get_total_users(self) -> int:
        """Obtener el número total de usuarios"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            count = cursor.fetchone()[0]
            conn.close()
            return int(count)
        except:
            return 0

    def update_user_rating(self, user_id: int, new_rating: float):
        """Actualizar la valoración del usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE users
                SET rating = ?, total_swaps = total_swaps + 1
                WHERE user_id = ?
                """,
                (new_rating, user_id),
            )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error al actualizar valoración: {e}")
            return False

    # ============= JUEGOS =============

    def add_game(
        self,
        user_id: int,
        title: str,
        platform: str,
        condition: str,
        photo_url: Optional[str],
        looking_for: str,
    ) -> Optional[int]:
        """Añadir un juego"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO games (user_id, title, platform, condition, photo_url, looking_for, created_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, title, platform, condition, photo_url, looking_for, self._now()),
            )

            game_id = cursor.lastrowid
            conn.commit()
            conn.close()

            logger.info(f"✅ Juego {title} añadido (ID: {game_id})")
            return int(game_id)
        except Exception as e:
            logger.error(f"❌ Error al añadir juego: {e}")
            return None

    def get_game(self, game_id: int) -> Optional[Dict]:
        """Obtener un juego por ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM games WHERE game_id = ?", (game_id,))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Error al obtener juego: {e}")
            return None

    def get_user_games(self, user_id: int) -> List[Dict]:
        """Obtener todos los juegos activos de un usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT * FROM games
                WHERE user_id = ? AND status = 'active'
                ORDER BY created_date DESC
                """,
                (user_id,),
            )

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error al obtener juegos del usuario: {e}")
            return []

    def get_all_active_games(self) -> List[Dict]:
        """Obtener todos los juegos activos"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT * FROM games
                WHERE status = 'active'
                ORDER BY created_date DESC
                """
            )

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error al obtener todos los juegos activos: {e}")
            return []

    def search_games(self, query: str) -> List[Dict]:
        """Buscar juegos por título (solo active)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT * FROM games
                WHERE status = 'active'
                  AND title LIKE ?
                ORDER BY created_date DESC
                """,
                (f"%{query}%",),
            )

            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error en la búsqueda de juegos: {e}")
            return []

    def remove_game(self, game_id: int, user_id: int) -> bool:
        """Eliminar un juego (cambiar estado)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                UPDATE games
                SET status = 'removed'
                WHERE game_id = ? AND user_id = ?
                """,
                (game_id, user_id),
            )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error al eliminar juego: {e}")
            return False

    def get_total_games(self) -> int:
        """Obtener cantidad de juegos activos"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM games WHERE status = 'active'")
            count = cursor.fetchone()[0]
            conn.close()
            return int(count)
        except:
            return 0

    # ============= INTERCAMBIOS (nuevo flujo) =============

    def create_swap_request(self, user1_id: int, user2_id: int, game1_id: int, game2_id: int) -> Optional[Tuple[int, str]]:
        """
        Crea un swap pending y devuelve (swap_id, code).
        user1 = iniciador, user2 = receptor.
        game1 pertenece a user1, game2 pertenece a user2.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # sanity: validate ownership + active
            cursor.execute("SELECT game_id, user_id, status FROM games WHERE game_id IN (?, ?)", (game1_id, game2_id))
            rows = cursor.fetchall()
            if len(rows) != 2:
                conn.close()
                return None

            info = {r["game_id"]: dict(r) for r in rows}
            if info[game1_id]["user_id"] != user1_id or info[game2_id]["user_id"] != user2_id:
                conn.close()
                return None
            if info[game1_id]["status"] != "active" or info[game2_id]["status"] != "active":
                conn.close()
                return None

            code = self._gen_swap_code()
            now = self._now()

            # keep old flags for compatibility, but use status/code
            cursor.execute(
                """
                INSERT INTO swaps (user1_id, user2_id, game1_id, game2_id,
                                   confirmed_by_user1, confirmed_by_user2,
                                   status, code, created_date, updated_date)
                VALUES (?, ?, ?, ?, 1, 0, 'pending', ?, ?, ?)
                """,
                (user1_id, user2_id, game1_id, game2_id, code, now, now),
            )

            swap_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return int(swap_id), code
        except Exception as e:
            logger.error(f"❌ Error al crear intercambio: {e}")
            return None

    def get_swap(self, swap_id: int) -> Optional[Dict]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM swaps WHERE swap_id = ?", (swap_id,))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Error al obtener intercambio: {e}")
            return None

    def set_swap_status(self, swap_id: int, status: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE swaps SET status = ?, updated_date = ? WHERE swap_id = ?",
                (status, self._now(), swap_id),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error al actualizar estado del intercambio: {e}")
            return False

    def complete_swap(self, swap_id: int, confirmer_user_id: int) -> Tuple[bool, str]:
        """
        Confirmación por user2. Hace swap de owners en games y marca completed.
        Devuelve (ok, message_error)
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("BEGIN")

            cursor.execute("SELECT * FROM swaps WHERE swap_id = ?", (swap_id,))
            swap = cursor.fetchone()
            if not swap:
                cursor.execute("ROLLBACK")
                return False, "swap not found"

            swap = dict(swap)

            if swap.get("status") != "pending":
                cursor.execute("ROLLBACK")
                return False, f"swap status is {swap.get('status')}"

            if confirmer_user_id != swap["user2_id"]:
                cursor.execute("ROLLBACK")
                return False, "only recipient can confirm"

            user1_id = swap["user1_id"]
            user2_id = swap["user2_id"]
            game1_id = swap["game1_id"]
            game2_id = swap["game2_id"]

            # verify current owners and status
            cursor.execute("SELECT game_id, user_id, status FROM games WHERE game_id IN (?, ?)", (game1_id, game2_id))
            rows = cursor.fetchall()
            if len(rows) != 2:
                cursor.execute("ROLLBACK")
                return False, "game not found"

            g = {r["game_id"]: dict(r) for r in rows}
            if g[game1_id]["user_id"] != user1_id or g[game2_id]["user_id"] != user2_id:
                cursor.execute("ROLLBACK")
                return False, "owners changed; cannot complete"
            if g[game1_id]["status"] != "active" or g[game2_id]["status"] != "active":
                cursor.execute("ROLLBACK")
                return False, "one of games not active"

            # swap owners
            cursor.execute("UPDATE games SET user_id = ? WHERE game_id = ?", (user2_id, game1_id))
            cursor.execute("UPDATE games SET user_id = ? WHERE game_id = ?", (user1_id, game2_id))

            # mark swap completed
            now = self._now()
            cursor.execute(
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

            # increment counters
            cursor.execute("UPDATE users SET total_swaps = total_swaps + 1 WHERE user_id IN (?, ?)", (user1_id, user2_id))

            conn.commit()
            conn.close()
            return True, ""
        except Exception as e:
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return False, str(e)

    def get_total_swaps(self) -> int:
        """Obtener cantidad de intercambios completados"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM swaps WHERE status = 'completed'")
            count = cursor.fetchone()[0]
            conn.close()
            return int(count)
        except:
            return 0
