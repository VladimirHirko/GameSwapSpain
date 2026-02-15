#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de base de datos para GameSwap Bot
Gestión de base de datos SQLite
"""
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)

class Database:
    """Clase para gestionar la base de datos"""
   
    def __init__(self, db_file: str = 'gameswap.db'):
        """Inicialización de la base de datos"""
        self.db_file = db_file
        self.init_database()
   
    def get_connection(self):
        """Obtener conexión a la base de datos"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn
   
    def init_database(self):
        """Crear tablas si no existen"""
        conn = self.get_connection()
        cursor = conn.cursor()
       
        # Tabla de usuarios
        cursor.execute('''
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
        ''')
       
        # Tabla de juegos
        cursor.execute('''
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
        ''')
       
        # Tabla de intercambios
        cursor.execute('''
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
        ''')
       
        conn.commit()
        conn.close()
        logger.info("✅ Base de datos inicializada")
   
    # ============= USUARIOS =============
   
    def create_user(self, user_id: int, username: str, display_name: str, city: str) -> bool:
        """Crear un nuevo usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            cursor.execute('''
                INSERT INTO users (user_id, username, display_name, city, registered_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, username, display_name, city, datetime.now().isoformat()))
           
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
           
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            conn.close()
           
            if row:
                return dict(row)
            return None
        except Exception as e:
            logger.error(f"❌ Error al obtener usuario: {e}")
            return None
   
    def get_total_users(self) -> int:
        """Obtener el número total de usuarios"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM users')
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except:
            return 0
   
    def update_user_rating(self, user_id: int, new_rating: float):
        """Actualizar la valoración del usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            cursor.execute('''
                UPDATE users
                SET rating = ?, total_swaps = total_swaps + 1
                WHERE user_id = ?
            ''', (new_rating, user_id))
           
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error al actualizar valoración: {e}")
            return False
   
    # ============= JUEGOS =============
   
    def add_game(self, user_id: int, title: str, platform: str,
                 condition: str, photo_url: Optional[str], looking_for: str) -> Optional[int]:
        """Añadir un juego"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            cursor.execute('''
                INSERT INTO games (user_id, title, platform, condition, photo_url, looking_for, created_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, title, platform, condition, photo_url, looking_for, datetime.now().isoformat()))
           
            game_id = cursor.lastrowid
            conn.commit()
            conn.close()
           
            logger.info(f"✅ Juego {title} añadido (ID: {game_id})")
            return game_id
        except Exception as e:
            logger.error(f"❌ Error al añadir juego: {e}")
            return None
   
    def get_user_games(self, user_id: int) -> List[Dict]:
        """Obtener todos los juegos de un usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            cursor.execute('''
                SELECT * FROM games
                WHERE user_id = ? AND status = 'active'
                ORDER BY created_date DESC
            ''', (user_id,))
           
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
           
            cursor.execute('''
                SELECT * FROM games
                WHERE status = 'active'
                ORDER BY created_date DESC
            ''')
           
            rows = cursor.fetchall()
            conn.close()
           
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error al obtener todos los juegos activos: {e}")
            return []
   
    def search_games(self, query: str) -> List[Dict]:
        """Buscar juegos por título"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            cursor.execute('''
                SELECT * FROM games
                WHERE status = 'active'
                AND title LIKE ?
                ORDER BY created_date DESC
            ''', (f'%{query}%',))
           
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
           
            cursor.execute('''
                UPDATE games
                SET status = 'removed'
                WHERE game_id = ? AND user_id = ?
            ''', (game_id, user_id))
           
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
            return count
        except:
            return 0
   
    # ============= INTERCAMBIOS =============
   
    def create_swap(self, user1_id: int, user2_id: int, game1_id: int, game2_id: int) -> Optional[int]:
        """Crear registro de intercambio"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            cursor.execute('''
                INSERT INTO swaps (user1_id, user2_id, game1_id, game2_id)
                VALUES (?, ?, ?, ?)
            ''', (user1_id, user2_id, game1_id, game2_id))
           
            swap_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return swap_id
        except Exception as e:
            logger.error(f"❌ Error al crear intercambio: {e}")
            return None
   
    def confirm_swap(self, swap_id: int, user_id: int) -> bool:
        """Confirmar intercambio por parte de un usuario"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            # Obtener información del intercambio
            cursor.execute('SELECT * FROM swaps WHERE swap_id = ?', (swap_id,))
            swap = cursor.fetchone()
           
            if not swap:
                return False
           
            # Determinar qué usuario está confirmando
            if swap['user1_id'] == user_id:
                cursor.execute('''
                    UPDATE swaps SET confirmed_by_user1 = 1 WHERE swap_id = ?
                ''', (swap_id,))
            elif swap['user2_id'] == user_id:
                cursor.execute('''
                    UPDATE swaps SET confirmed_by_user2 = 1 WHERE swap_id = ?
                ''', (swap_id,))
            else:
                return False
           
            # Verificar si ambos confirmaron
            cursor.execute('SELECT * FROM swaps WHERE swap_id = ?', (swap_id,))
            swap = cursor.fetchone()
           
            if swap['confirmed_by_user1'] and swap['confirmed_by_user2']:
                # Ambos confirmaron → finalizar intercambio
                cursor.execute('''
                    UPDATE swaps
                    SET completed_date = ?
                    WHERE swap_id = ?
                ''', (datetime.now().isoformat(), swap_id))
               
                # Actualizar estado de los juegos
                cursor.execute('UPDATE games SET status = ? WHERE game_id IN (?, ?)',
                             ('swapped', swap['game1_id'], swap['game2_id']))
           
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error al confirmar intercambio: {e}")
            return False
   
    def get_total_swaps(self) -> int:
        """Obtener cantidad de intercambios completados"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM swaps WHERE completed_date IS NOT NULL')
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except:
            return 0