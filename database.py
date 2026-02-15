#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GameSwap Spain Bot — SQLite Database layer

Goals:
- Production-ready (Railway): default DB path /data/gameswap.db (ENV: DB_FILE)
- Robust SQLite connection: FK ON, busy_timeout, WAL (if possible)
- Backward compatible: lightweight migrations via ALTER TABLE
- Swap is atomic: BEGIN IMMEDIATE + owner/status checks
- Username: NEVER "SinUsuario". If missing -> "" (empty string). Migrates old "SinUsuario" -> "".
- Adds catalog helpers: platform counts, filtered catalog (platform/city), pagination, owner info in rows.

Python: 3.10+
"""

from __future__ import annotations

import os
import sqlite3
import logging
import random
import string
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_file: Optional[str] = None):
        self.db_file = (db_file or os.getenv("DB_FILE") or "/data/gameswap.db").strip()
        self.init_database()

    # ----------------------------
    # Low-level helpers
    # ----------------------------
    def get_connection(self) -> sqlite3.Connection:
        # isolation_level=None => manual transactions (BEGIN/COMMIT/ROLLBACK)
        conn = sqlite3.connect(self.db_file, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row

        # PRAGMA settings (best-effort)
        try:
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=30000;")
        except Exception:
            pass

        return conn

    def _now(self) -> str:
        return datetime.now().isoformat(timespec="seconds")

    def _normalize_username(self, username: Optional[str]) -> str:
        u = (username or "").strip()
        if u.startswith("@"):
            u = u[1:]
        return u.strip()

    def _gen_swap_code(self) -> str:
        return "SWAP-" + "".join(random.choice(string.digits) for _ in range(6))

    def _table_exists(self, conn: sqlite3.Connection, table: str) -> bool:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        )
        return cur.fetchone() is not None

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        cur = conn.cursor()
        try:
            cur.execute(f"PRAGMA table_info({table})")
            return {r["name"] for r in cur.fetchall()}
        except Exception:
            return set()

    def _add_column_if_missing(
        self,
        conn: sqlite3.Connection,
        table: str,
        col_name: str,
        col_def_sql: str,
    ) -> None:
        cols = self._table_columns(conn, table)
        if col_name in cols:
            return
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def_sql}")

    # ----------------------------
    # Schema init + migrations
    # ----------------------------
    def init_database(self) -> None:
        os.makedirs(os.path.dirname(self.db_file), exist_ok=True) if "/" in self.db_file else None

        conn = self.get_connection()
        cur = conn.cursor()

        # WAL (best-effort)
        try:
            cur.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

        # ---- users
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                display_name TEXT NOT NULL,
                city TEXT NOT NULL,
                rating REAL DEFAULT 0.0,
                rating_sum INTEGER DEFAULT 0,
                rating_count INTEGER DEFAULT 0,
                total_swaps INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                registered_date TEXT NOT NULL
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
        # IMPORTANT: keep compatibility with older versions; add missing columns via ALTER.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS swaps (
                swap_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user1_id INTEGER NOT NULL,
                user2_id INTEGER NOT NULL,
                game1_id INTEGER NOT NULL,
                game2_id INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                code TEXT,
                confirmed_by_user1 INTEGER DEFAULT 0,
                confirmed_by_user2 INTEGER DEFAULT 0,
                created_date TEXT,
                updated_date TEXT,
                completed_date TEXT,
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

        # ----------------------------
        # Light migrations
        # ----------------------------
        # users
        self._add_column_if_missing(conn, "users", "rating_sum", "INTEGER DEFAULT 0")
        self._add_column_if_missing(conn, "users", "rating_count", "INTEGER DEFAULT 0")
        self._add_column_if_missing(conn, "users", "total_swaps", "INTEGER DEFAULT 0")
        self._add_column_if_missing(conn, "users", "is_banned", "INTEGER DEFAULT 0")
        self._add_column_if_missing(conn, "users", "rating", "REAL DEFAULT 0.0")
        self._add_column_if_missing(conn, "users", "registered_date", "TEXT")

        # swaps: in case some old schema missing these columns
        self._add_column_if_missing(conn, "swaps", "status", "TEXT DEFAULT 'pending'")
        self._add_column_if_missing(conn, "swaps", "code", "TEXT")
        self._add_column_if_missing(conn, "swaps", "confirmed_by_user1", "INTEGER DEFAULT 0")
        self._add_column_if_missing(conn, "swaps", "confirmed_by_user2", "INTEGER DEFAULT 0")
        self._add_column_if_missing(conn, "swaps", "created_date", "TEXT")
        self._add_column_if_missing(conn, "swaps", "updated_date", "TEXT")
        self._add_column_if_missing(conn, "swaps", "completed_date", "TEXT")

        # games: status / created_date might be missing in very old DBs
        self._add_column_if_missing(conn, "games", "status", "TEXT DEFAULT 'active'")
        self._add_column_if_missing(conn, "games", "created_date", "TEXT")
        self._add_column_if_missing(conn, "games", "photo_url", "TEXT")

        # ----------------------------
        # Data migration: "SinUsuario" -> ""
        # ----------------------------
        try:
            cur.execute(
                "UPDATE users SET username='' WHERE username IS NOT NULL AND LOWER(username)='sinusuario'"
            )
        except Exception:
            pass

        # Also normalize NULL usernames to empty string (optional, safe)
        try:
            cur.execute("UPDATE users SET username='' WHERE username IS NULL")
        except Exception:
            pass

        # Fill missing registered_date for old users
        try:
            cur.execute("UPDATE users SET registered_date=? WHERE registered_date IS NULL OR registered_date=''", (self._now(),))
        except Exception:
            pass

        # ----------------------------
        # Indexes (performance)
        # ----------------------------
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username_nocase ON users(username COLLATE NOCASE)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_city_nocase ON users(city COLLATE NOCASE)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_status_platform_created ON games(status, platform, created_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_user_status ON games(user_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_title_nocase ON games(title COLLATE NOCASE)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_swaps_status ON swaps(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_swaps_user2_status ON swaps(user2_id, status)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_feedback_once_per_swap ON swap_feedback(swap_id, from_user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_feedback_to_user ON swap_feedback(to_user_id, created_date)")

        conn.commit()
        conn.close()
        logger.info("✅ Database initialized & migrated: %s", self.db_file)

    # ============================
    # USERS
    # ============================
    def create_user(self, user_id: int, username: Optional[str], display_name: str, city: str) -> bool:
        """
        Upsert user:
          - username: normalized, may be "" (empty string)
          - if user exists: update username/display_name/city
        """
        u = self._normalize_username(username)
        dn = (display_name or "SinNombre").strip()
        ct = (city or "SinCiudad").strip()

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")

            cur.execute("SELECT user_id FROM users WHERE user_id=?", (int(user_id),))
            exists = cur.fetchone() is not None

            if not exists:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, display_name, city, rating, rating_sum, rating_count, total_swaps, is_banned, registered_date)
                    VALUES (?, ?, ?, ?, 0.0, 0, 0, 0, 0, ?)
                    """,
                    (int(user_id), u, dn, ct, self._now()),
                )
            else:
                cur.execute(
                    """
                    UPDATE users
                    SET username=?, display_name=?, city=?
                    WHERE user_id=?
                    """,
                    (u, dn, ct, int(user_id)),
                )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ create_user error: %s", e)
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return False

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE user_id=?", (int(user_id),))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ get_user error: %s", e)
            return None

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        u = self._normalize_username(username)
        if not u:
            return None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE username=? COLLATE NOCASE LIMIT 1", (u,))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ get_user_by_username error: %s", e)
            return None

    def search_users_by_username(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
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
            logger.error("❌ search_users_by_username error: %s", e)
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

    def is_banned(self, user_id: int) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT is_banned FROM users WHERE user_id=?", (int(user_id),))
            row = cur.fetchone()
            conn.close()
            return bool(row and int(row["is_banned"] or 0) == 1)
        except Exception:
            return False

    # Legacy method (kept for compatibility)
    def update_user_rating(self, user_id: int, new_rating: float) -> bool:
        """
        Legacy: set rating directly and increment total_swaps.
        Prefer apply_user_rating() via feedback.
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute(
                "UPDATE users SET rating=?, total_swaps=total_swaps+1 WHERE user_id=?",
                (float(new_rating), int(user_id)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ update_user_rating error: %s", e)
            return False

    def apply_user_rating(self, to_user_id: int, stars: int) -> bool:
        """rating_sum += stars; rating_count += 1; rating = rating_sum / rating_count"""
        if stars < 1 or stars > 5:
            return False

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")

            cur.execute("SELECT rating_sum, rating_count FROM users WHERE user_id=?", (int(to_user_id),))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                conn.close()
                return False

            rs = int(row["rating_sum"] or 0) + int(stars)
            rc = int(row["rating_count"] or 0) + 1
            rating = rs / rc if rc else 0.0

            cur.execute(
                "UPDATE users SET rating_sum=?, rating_count=?, rating=? WHERE user_id=?",
                (rs, rc, float(rating), int(to_user_id)),
            )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ apply_user_rating error: %s", e)
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
            cur.execute("BEGIN")

            cur.execute(
                """
                INSERT INTO games (user_id, title, platform, condition, photo_url, looking_for, status, created_date)
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
                """,
                (
                    int(user_id),
                    str(title).strip(),
                    str(platform).strip(),
                    str(condition).strip(),
                    photo_url,
                    str(looking_for).strip(),
                    self._now(),
                ),
            )
            game_id = cur.lastrowid

            conn.commit()
            conn.close()
            return int(game_id)
        except Exception as e:
            logger.error("❌ add_game error: %s", e)
            return None

    def get_game(self, game_id: int) -> Optional[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM games WHERE game_id=?", (int(game_id),))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ get_game error: %s", e)
            return None

    def get_user_games(self, user_id: int) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM games
                WHERE user_id=? AND status='active'
                ORDER BY created_date DESC
                """,
                (int(user_id),),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ get_user_games error: %s", e)
            return []

    def get_user_active_games(self, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM games
                WHERE user_id=? AND status='active'
                ORDER BY created_date DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ get_user_active_games error: %s", e)
            return []

    def get_all_active_games(self) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM games
                WHERE status='active'
                ORDER BY created_date DESC
                """
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ get_all_active_games error: %s", e)
            return []

    def remove_game(self, game_id: int, user_id: int) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute(
                "UPDATE games SET status='removed' WHERE game_id=? AND user_id=?",
                (int(game_id), int(user_id)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ remove_game error: %s", e)
            return False

    def get_total_games(self) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM games WHERE status='active'")
            n = cur.fetchone()[0]
            conn.close()
            return int(n)
        except Exception:
            return 0

    def search_games(self, query: str) -> List[Dict[str, Any]]:
        """
        Search active games by title, ordering by owner trust (total_swaps, rating) then recency.
        """
        q = (query or "").strip()
        if not q:
            return []

        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT g.*
                FROM games g
                JOIN users u ON u.user_id = g.user_id
                WHERE g.status='active'
                  AND g.title LIKE ? COLLATE NOCASE
                ORDER BY u.total_swaps DESC, u.rating DESC, g.created_date DESC
                """,
                (f"%{q}%",),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ search_games error: %s", e)
            return []

    # ============================
    # CATALOG (NEW) — platform/city + pagination + owner info
    # ============================
    def get_platform_counts(self, *, city: Optional[str] = None, exclude_user_id: Optional[int] = None) -> Dict[str, int]:
        """
        Returns counts of active games per platform.
        Optional filters:
          - city: owner's city (case-insensitive exact match)
          - exclude_user_id: exclude own games
        """
        ct = (city or "").strip()
        city_filter = ct if ct else None

        try:
            conn = self.get_connection()
            cur = conn.cursor()

            params: List[Any] = []
            where = ["g.status='active'"]

            if exclude_user_id is not None:
                where.append("g.user_id != ?")
                params.append(int(exclude_user_id))

            if city_filter:
                where.append("LOWER(u.city) = LOWER(?)")
                params.append(city_filter)

            where_sql = " AND ".join(where)

            cur.execute(
                f"""
                SELECT g.platform, COUNT(*) as cnt
                FROM games g
                JOIN users u ON u.user_id = g.user_id
                WHERE {where_sql}
                GROUP BY g.platform
                ORDER BY cnt DESC
                """,
                tuple(params),
            )

            rows = cur.fetchall()
            conn.close()

            out = {str(r["platform"]): int(r["cnt"]) for r in rows}
            return out
        except Exception as e:
            logger.error("❌ get_platform_counts error: %s", e)
            return {}

    def count_catalog_games(
        self,
        *,
        platform: Optional[str] = None,
        city: Optional[str] = None,
        exclude_user_id: Optional[int] = None,
    ) -> int:
        """
        Counts active games with filters.
        """
        pf = (platform or "").strip()
        ct = (city or "").strip()

        try:
            conn = self.get_connection()
            cur = conn.cursor()

            params: List[Any] = []
            where = ["g.status='active'"]

            if exclude_user_id is not None:
                where.append("g.user_id != ?")
                params.append(int(exclude_user_id))

            if pf:
                where.append("g.platform = ?")
                params.append(pf)

            if ct:
                where.append("LOWER(u.city) = LOWER(?)")
                params.append(ct)

            where_sql = " AND ".join(where)

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM games g
                JOIN users u ON u.user_id = g.user_id
                WHERE {where_sql}
                """,
                tuple(params),
            )

            n = int(cur.fetchone()[0])
            conn.close()
            return n
        except Exception as e:
            logger.error("❌ count_catalog_games error: %s", e)
            return 0

    def list_catalog_games(
        self,
        *,
        platform: Optional[str] = None,
        city: Optional[str] = None,
        exclude_user_id: Optional[int] = None,
        offset: int = 0,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Returns active games + owner info for catalog cards.
        Each row includes:
          - game fields: game_id, title, platform, condition, photo_url, looking_for, created_date
          - owner fields: owner_id, display_name, username, city, rating, total_swaps, rating_count
        """
        pf = (platform or "").strip()
        ct = (city or "").strip()

        try:
            conn = self.get_connection()
            cur = conn.cursor()

            params: List[Any] = []
            where = ["g.status='active'"]

            if exclude_user_id is not None:
                where.append("g.user_id != ?")
                params.append(int(exclude_user_id))

            if pf:
                where.append("g.platform = ?")
                params.append(pf)

            if ct:
                where.append("LOWER(u.city) = LOWER(?)")
                params.append(ct)

            where_sql = " AND ".join(where)

            cur.execute(
                f"""
                SELECT
                  g.game_id, g.title, g.platform, g.condition, g.photo_url, g.looking_for, g.created_date,
                  u.user_id AS owner_id, u.display_name, u.username, u.city, u.rating, u.rating_count, u.total_swaps
                FROM games g
                JOIN users u ON u.user_id = g.user_id
                WHERE {where_sql}
                ORDER BY u.total_swaps DESC, u.rating DESC, g.created_date DESC
                LIMIT ? OFFSET ?
                """,
                tuple(params + [int(limit), int(offset)]),
            )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ list_catalog_games error: %s", e)
            return []

    def list_distinct_cities(self, *, exclude_empty: bool = True, limit: int = 200) -> List[str]:
        """
        Optional helper: list cities present in users.
        Useful for autocomplete or 'top cities'.
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            if exclude_empty:
                cur.execute(
                    """
                    SELECT DISTINCT city
                    FROM users
                    WHERE city IS NOT NULL AND TRIM(city) != ''
                    ORDER BY city COLLATE NOCASE
                    LIMIT ?
                    """,
                    (int(limit),),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT COALESCE(city,'') AS city
                    FROM users
                    ORDER BY city COLLATE NOCASE
                    LIMIT ?
                    """,
                    (int(limit),),
                )

            rows = cur.fetchall()
            conn.close()
            return [str(r["city"]) for r in rows if str(r["city"]).strip()]
        except Exception as e:
            logger.error("❌ list_distinct_cities error: %s", e)
            return []

    # ============================
    # SWAPS
    # ============================
    def create_swap_request(self, user1_id: int, user2_id: int, game1_id: int, game2_id: int) -> Optional[Tuple[int, str]]:
        """
        Creates pending swap atomically.
        Rules:
          - no self-swap
          - both games exist, active
          - game1 belongs to user1, game2 belongs to user2
          - no duplicate pending swap for same pair of games (either direction)
        """
        if int(user1_id) == int(user2_id):
            return None

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")

            # Check games exist
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
            g1 = info.get(int(game1_id))
            g2 = info.get(int(game2_id))
            if not g1 or not g2:
                conn.rollback()
                conn.close()
                return None

            if int(g1["user_id"]) != int(user1_id):
                conn.rollback()
                conn.close()
                return None

            if int(g2["user_id"]) != int(user2_id):
                conn.rollback()
                conn.close()
                return None

            if g1["status"] != "active" or g2["status"] != "active":
                conn.rollback()
                conn.close()
                return None

            # Duplicate pending swap guard
            cur.execute(
                """
                SELECT swap_id FROM swaps
                WHERE status='pending'
                  AND ((game1_id=? AND game2_id=?) OR (game1_id=? AND game2_id=?))
                LIMIT 1
                """,
                (int(game1_id), int(game2_id), int(game2_id), int(game1_id)),
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
                (int(user1_id), int(user2_id), int(game1_id), int(game2_id), code, now, now),
            )

            swap_id = cur.lastrowid
            conn.commit()
            conn.close()

            return int(swap_id), code

        except Exception as e:
            logger.error("❌ create_swap_request error: %s", e)
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return None

    def get_swap(self, swap_id: int) -> Optional[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM swaps WHERE swap_id=?", (int(swap_id),))
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as e:
            logger.error("❌ get_swap error: %s", e)
            return None

    def set_swap_status(self, swap_id: int, status: str) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute(
                "UPDATE swaps SET status=?, updated_date=? WHERE swap_id=?",
                (str(status), self._now(), int(swap_id)),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error("❌ set_swap_status error: %s", e)
            return False

    def complete_swap(self, swap_id: int, confirmer_user_id: int) -> Tuple[bool, str]:
        """
        Completes swap atomically:
          - only user2 can confirm
          - swap must be pending
          - games must still be active and owned by expected users
          - swap ownership changes (games.user_id swapped)
          - swap.status -> completed
          - users.total_swaps += 1 for both
        """
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")

            cur.execute("SELECT * FROM swaps WHERE swap_id=?", (int(swap_id),))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                conn.close()
                return False, "swap not found"

            swap = dict(row)
            if swap.get("status") != "pending":
                conn.rollback()
                conn.close()
                return False, "swap not pending"

            if int(confirmer_user_id) != int(swap["user2_id"]):
                conn.rollback()
                conn.close()
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
                conn.close()
                return False, "games missing"

            g = {int(r["game_id"]): dict(r) for r in games}
            if g[g1_id]["status"] != "active" or g[g2_id]["status"] != "active":
                conn.rollback()
                conn.close()
                return False, "game not active"

            if int(g[g1_id]["user_id"]) != int(swap["user1_id"]) or int(g[g2_id]["user_id"]) != int(swap["user2_id"]):
                conn.rollback()
                conn.close()
                return False, "ownership changed"

            # Swap owners
            cur.execute("UPDATE games SET user_id=? WHERE game_id=?", (int(swap["user2_id"]), g1_id))
            cur.execute("UPDATE games SET user_id=? WHERE game_id=?", (int(swap["user1_id"]), g2_id))

            now = self._now()
            cur.execute(
                """
                UPDATE swaps
                SET confirmed_by_user2=1,
                    status='completed',
                    completed_date=?,
                    updated_date=?
                WHERE swap_id=?
                """,
                (now, now, int(swap_id)),
            )

            cur.execute(
                "UPDATE users SET total_swaps = total_swaps + 1 WHERE user_id IN (?, ?)",
                (int(swap["user1_id"]), int(swap["user2_id"])),
            )

            conn.commit()
            conn.close()
            return True, ""

        except Exception as e:
            logger.error("❌ complete_swap error: %s", e)
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return False, str(e)

    def get_total_swaps(self) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM swaps WHERE status='completed'")
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

        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")

            # prevent duplicates
            cur.execute(
                "SELECT feedback_id FROM swap_feedback WHERE swap_id=? AND from_user_id=?",
                (int(swap_id), int(from_user_id)),
            )
            if cur.fetchone():
                conn.rollback()
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

            # update rating
            self.apply_user_rating(to_user_id=int(to_user_id), stars=int(stars))

            conn.commit()
            conn.close()
            return int(feedback_id)

        except Exception as e:
            logger.error("❌ add_feedback error: %s", e)
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            return None

    def add_feedback_photo(self, feedback_id: int, photo_file_id: str) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")
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
            logger.error("❌ add_feedback_photo error: %s", e)
            return False

    def get_feedback_photos(self, feedback_id: int) -> List[str]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT photo_file_id
                FROM swap_feedback_photos
                WHERE feedback_id=?
                ORDER BY id ASC
                """,
                (int(feedback_id),),
            )
            rows = cur.fetchall()
            conn.close()
            return [str(r["photo_file_id"]) for r in rows]
        except Exception as e:
            logger.error("❌ get_feedback_photos error: %s", e)
            return []

    def get_user_feedback_summary(self, user_id: int) -> Dict[str, Any]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT rating, rating_count FROM users WHERE user_id=?", (int(user_id),))
            row = cur.fetchone()
            conn.close()
            if not row:
                return {"rating": 0.0, "rating_count": 0}
            return {"rating": float(row["rating"] or 0.0), "rating_count": int(row["rating_count"] or 0)}
        except Exception:
            return {"rating": 0.0, "rating_count": 0}

    def get_user_feedback(self, user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT *
                FROM swap_feedback
                WHERE to_user_id=?
                ORDER BY created_date DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ get_user_feedback error: %s", e)
            return []

    # ============================
    # ADMIN HELPERS
    # ============================
    def admin_list_users(
        self,
        limit: int = 10,
        offset: int = 0,
        only_banned: bool = False,
        query: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            q = (query or "").strip()
            if q.startswith("@"):
                q = q[1:]

            params: List[Any] = []
            where: List[str] = []

            if only_banned:
                where.append("is_banned=1")

            if q:
                where.append("(username LIKE ? COLLATE NOCASE OR display_name LIKE ? COLLATE NOCASE OR city LIKE ? COLLATE NOCASE)")
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
                tuple(params + [int(limit), int(offset)]),
            )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ admin_list_users error: %s", e)
            return []

    def admin_count_users(self, only_banned: bool = False, query: Optional[str] = None) -> int:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            q = (query or "").strip()
            if q.startswith("@"):
                q = q[1:]

            params: List[Any] = []
            where: List[str] = []

            if only_banned:
                where.append("is_banned=1")

            if q:
                where.append("(username LIKE ? COLLATE NOCASE OR display_name LIKE ? COLLATE NOCASE OR city LIKE ? COLLATE NOCASE)")
                like = f"%{q}%"
                params.extend([like, like, like])

            where_sql = ("WHERE " + " AND ".join(where)) if where else ""

            cur.execute(f"SELECT COUNT(*) FROM users {where_sql}", tuple(params))
            n = int(cur.fetchone()[0])
            conn.close()
            return n
        except Exception as e:
            logger.error("❌ admin_count_users error: %s", e)
            return 0

    def admin_get_user(self, user_ref: str) -> Optional[Dict[str, Any]]:
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
            cur.execute("BEGIN")
            cur.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (int(u["user_id"]),))
            conn.commit()
            conn.close()
            logger.warning("🚫 ADMIN BAN user_id=%s username=%s reason=%s", u["user_id"], u.get("username"), reason)
            return True
        except Exception as e:
            logger.error("❌ admin_ban_user error: %s", e)
            return False

    def admin_unban_user(self, user_ref: str) -> bool:
        u = self.admin_get_user(user_ref)
        if not u:
            return False
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (int(u["user_id"]),))
            conn.commit()
            conn.close()
            logger.warning("✅ ADMIN UNBAN user_id=%s username=%s", u["user_id"], u.get("username"))
            return True
        except Exception as e:
            logger.error("❌ admin_unban_user error: %s", e)
            return False

    def admin_list_user_games(self, user_ref: str, include_removed: bool = True, limit: int = 50) -> List[Dict[str, Any]]:
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
                    WHERE user_id=?
                    ORDER BY created_date DESC
                    LIMIT ?
                    """,
                    (int(u["user_id"]), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM games
                    WHERE user_id=? AND status='active'
                    ORDER BY created_date DESC
                    LIMIT ?
                    """,
                    (int(u["user_id"]), int(limit)),
                )

            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("❌ admin_list_user_games error: %s", e)
            return []

    def admin_remove_game(self, game_id: int) -> bool:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute("UPDATE games SET status='removed' WHERE game_id=?", (int(game_id),))
            changed = cur.rowcount
            conn.commit()
            conn.close()
            return changed > 0
        except Exception as e:
            logger.error("❌ admin_remove_game error: %s", e)
            return False

    def admin_list_swaps(self, status: Optional[str] = None, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            if status:
                cur.execute(
                    """
                    SELECT * FROM swaps
                    WHERE status=?
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
            logger.error("❌ admin_list_swaps error: %s", e)
            return []

    def admin_get_stats(self) -> Dict[str, int]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM users")
            users_total = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM users WHERE is_banned=1")
            users_banned = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM games WHERE status='active'")
            games_active = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM swaps WHERE status='pending'")
            swaps_pending = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM swaps WHERE status='completed'")
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
            logger.error("❌ admin_get_stats error: %s", e)
            return {
                "users_total": 0,
                "users_banned": 0,
                "games_active": 0,
                "swaps_pending": 0,
                "swaps_completed": 0,
            }
