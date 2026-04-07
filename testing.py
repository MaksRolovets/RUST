# -*- coding: utf-8 -*-
"""
restore.py — Разовое восстановление пользователей из users_report.txt
Запусти этот файл один раз, чтобы восстановить БД после дропа.
"""

import re
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).with_name("bot_data.sqlite3")

def now_iso() -> str:
    return datetime.utcnow().isoformat()


def restore_users_from_report():
    report_path = Path(__file__).with_name("users_report.txt")
    
    if not report_path.exists():
        print("❌ Файл users_report.txt не найден!")
        return

    print("🔄 Начинаем восстановление пользователей из users_report.txt...")

    restored = 0
    skipped = 0
    errors = 0

    try:
        with report_path.open("r", encoding="utf-8") as f:
            content = f.read()

        # Находим все user_id
        matches = re.findall(r"user_id=(\d+)", content)
        unique_user_ids = set(int(uid) for uid in matches if uid.isdigit())

        print(f"Найдено уникальных user_id: {len(unique_user_ids)}")

        now_ts = now_iso()

        with sqlite3.connect(DB_PATH) as conn:
            for user_id in sorted(unique_user_ids):
                try:
                    # Проверяем, есть ли уже такой пользователь
                    exists = conn.execute(
                        "SELECT 1 FROM users WHERE user_id = ?", 
                        (user_id,)
                    ).fetchone()

                    if exists:
                        skipped += 1
                        continue

                    # Добавляем пользователя
                    conn.execute(
                        """
                        INSERT INTO users (user_id, first_seen, last_seen)
                        VALUES (?, ?, ?)
                        """,
                        (user_id, now_ts, now_ts)
                    )

                    # Добавляем доступ
                    conn.execute(
                        "INSERT OR IGNORE INTO user_access (user_id, total_queries) VALUES (?, 0)",
                        (user_id,)
                    )

                    restored += 1

                except Exception as e:
                    errors += 1
                    print(f"Ошибка при восстановлении user_id={user_id}: {e}")

            conn.commit()

        print("\n✅ Восстановление завершено!")
        print(f"   Восстановлено новых пользователей: {restored}")
        print(f"   Уже существовало:                 {skipped}")
        print(f"   Ошибок:                            {errors}")
        print(f"   Всего уникальных user_id в файле:  {len(unique_user_ids)}")

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    restore_users_from_report()
    input("\nНажмите Enter для выхода...")