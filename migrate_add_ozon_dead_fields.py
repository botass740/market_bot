import sqlite3
from pathlib import Path

DB_PATH = Path("parser.db")

def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]  # row[1] = name
    return column in cols

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH.resolve()}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # products.dead_check_fail_count
    if not _column_exists(cur, "products", "dead_check_fail_count"):
        cur.execute("ALTER TABLE products ADD COLUMN dead_check_fail_count INTEGER NOT NULL DEFAULT 0;")
        print("Added column: dead_check_fail_count")
    else:
        print("Column already exists: dead_check_fail_count")

    # products.last_dead_reason
    if not _column_exists(cur, "products", "last_dead_reason"):
        cur.execute("ALTER TABLE products ADD COLUMN last_dead_reason VARCHAR(32);")
        print("Added column: last_dead_reason")
    else:
        print("Column already exists: last_dead_reason")

    conn.commit()
    conn.close()
    print("Migration completed OK")

if __name__ == "__main__":
    main()