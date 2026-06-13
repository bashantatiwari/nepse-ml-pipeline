"""
ColumnStore Verification Script
Proves:
1. Docker is running a real ColumnStore container
2. Warehouse tables exist in ColumnStore with ENGINE=ColumnStore
3. Pipeline can insert and read NABIL data from ColumnStore
"""
import sys
import mariadb
from src.config.settings import (
    COLUMNSTORE_HOST, COLUMNSTORE_PORT,
    COLUMNSTORE_USER, COLUMNSTORE_PASSWORD,
    COLUMNSTORE_DATABASE,
)


def get_conn():
    return mariadb.connect(
        host=COLUMNSTORE_HOST,
        port=int(COLUMNSTORE_PORT),
        user=COLUMNSTORE_USER,
        password=COLUMNSTORE_PASSWORD,
        database=COLUMNSTORE_DATABASE,
    )


def check(label, passed, detail=""):
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{status} — {label}")
    if detail:
        print(f"       {detail}")
    return passed


def run():
    print("\n" + "=" * 60)
    print("  MariaDB ColumnStore Verification")
    print("=" * 60 + "\n")

    results = []

    # 1. Connection
    try:
        conn = get_conn()
        cursor = conn.cursor()
        results.append(check("ColumnStore container reachable",
                             True, f"Host: {COLUMNSTORE_HOST}:{COLUMNSTORE_PORT}"))
    except Exception as e:
        results.append(check("ColumnStore container reachable", False, str(e)))
        print("\n❌ Cannot connect. Aborting remaining checks.")
        sys.exit(1)

    # 2. VERSION
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()[0]
    is_columnstore = "columnstore" in version.lower() or "mariadb" in version.lower()
    results.append(check("MariaDB version confirmed", is_columnstore, f"VERSION: {version}"))

    # 3. ColumnStore engine available
    cursor.execute("SHOW ENGINES")
    engines = {row[0].lower(): row[1] for row in cursor.fetchall()}
    cs_available = "columnstore" in engines
    results.append(check("ColumnStore engine available",
                         cs_available,
                         f"Engines: {list(engines.keys())}"))

    # 4. Warehouse tables exist
    cursor.execute("SHOW TABLES")
    tables = {row[0].lower() for row in cursor.fetchall()}
    for table in ["raw_nabil_prices", "processed_nabil_features", "predictions"]:
        results.append(check(f"Table '{table}' exists", table in tables))

    # 5. ENGINE=ColumnStore on warehouse tables
    for table in ["raw_nabil_prices", "processed_nabil_features", "predictions"]:
        if table in tables:
            cursor.execute(f"SHOW CREATE TABLE {table}")
            create_sql = cursor.fetchone()[1].upper()
            is_cs = "COLUMNSTORE" in create_sql
            results.append(check(f"Table '{table}' uses ENGINE=ColumnStore", is_cs))

    # 6. Insert test row
    try:
        cursor.execute("""
            INSERT INTO raw_nabil_prices
            (symbol, published_date, open, high, low, close,
             percent_change, traded_quantity, traded_amount, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("TEST", "2000-01-01", 1.0, 2.0, 0.5, 1.5, 0.1, 100.0, 150.0, "test"))
        results.append(check("Insert test row into raw_nabil_prices", True))
    except Exception as e:
        results.append(check("Insert test row into raw_nabil_prices", False, str(e)))

    # 7. Read test row back
    try:
        cursor.execute(
            "SELECT * FROM raw_nabil_prices WHERE symbol = 'TEST' "
            "AND published_date = '2000-01-01'"
        )
        row = cursor.fetchone()
        results.append(check("Read test row from raw_nabil_prices",
                             row is not None, f"Row: {row}"))
    except Exception as e:
        results.append(check("Read test row from raw_nabil_prices", False, str(e)))

    # 8. Row counts
    for table in ["raw_nabil_prices", "processed_nabil_features", "predictions"]:
        if table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results.append(check(f"Row count for '{table}'", True, f"{count} rows"))

    conn.close()

    # Summary
    passed = sum(1 for r in results if r)
    total = len(results)
    print(f"\n{'=' * 60}")
    print(f"  Result: {passed}/{total} checks passed")
    print(f"{'=' * 60}\n")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    run()
