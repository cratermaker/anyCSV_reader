import os
import glob
import hashlib
import sqlite3
import pandas as pd
import csv
import re
from collections import Counter

DB_FILE = "csv_data.db"
TABLE_PREFIX = "csv_table_"

common_column_count = []

def init_db():
    db_path = os.path.abspath(DB_FILE)
    print(f"Using DB path: {db_path}")
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS imported_files (
            filename TEXT PRIMARY KEY,
            filehash TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_file_hash(file_path):
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def generate_table_name(file_path):
    name = os.path.splitext(os.path.basename(file_path))[0]
    name = re.sub(r'[^\w]', '_', name)  # Replace non-alphanumeric characters with underscore
    return TABLE_PREFIX + name.lower()

def infer_columns_structure():
    if common_column_count:
        return common_column_count[0]
    return 5  # Fallback default if nothing valid was processed yet

def import_csvs_to_db(folder_path):
    init_db()  # Ensure DB is ready
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

    for f in csv_files:
        filehash = get_file_hash(f)
        cur.execute("SELECT 1 FROM imported_files WHERE filename = ? AND filehash = ?", (f, filehash))
        if cur.fetchone():
            print(f"Skipping already imported file: {f}")
            continue

        try:
            with open(f, 'r', encoding='utf-8', errors='ignore') as file:
                sample = file.read(2048)
                file.seek(0)
                sniffer = csv.Sniffer()
                try:
                    dialect = sniffer.sniff(sample)
                except csv.Error:
                    dialect = csv.excel
                try:
                    has_header = sniffer.has_header(sample)
                except csv.Error:
                    has_header = False

            try:
                df = pd.read_csv(f, dtype=str, delimiter=dialect.delimiter, header=0 if has_header else None, on_bad_lines='skip', engine='python')
            except Exception as e:
                df = pd.read_csv(f, dtype=str, delimiter=dialect.delimiter, header=None, on_bad_lines='skip', engine='python')

            if df.empty or df.shape[1] < 1:
                raise ValueError("Parsed DataFrame is empty or invalid")

            if not has_header:
                df.columns = [f"column_{i+1}" for i in range(df.shape[1])]
            else:
                df.columns = [str(col) if str(col).strip() else f"column_{i+1}" for i, col in enumerate(df.columns)]

            common_column_count.append(df.shape[1])

            df['_hash'] = df.astype(str).apply(lambda row: hashlib.sha256("".join(row).encode()).hexdigest(), axis=1)
            table_name = generate_table_name(f)
            cols_def = ", ".join([f'"{col}" TEXT' for col in df.columns])

            cur.execute(f"CREATE TABLE IF NOT EXISTS '{table_name}' ({cols_def})")
            cur.execute(f"SELECT _hash FROM '{table_name}'")
            existing_hashes = set(row[0] for row in cur.fetchall())

            new_rows = df[~df['_hash'].isin(existing_hashes)]

            if not new_rows.empty:
                new_rows.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"Imported {len(new_rows)} new rows into table: {table_name}")
            else:
                print(f"No new data to import for: {f}")

            cur.execute("INSERT OR REPLACE INTO imported_files (filename, filehash) VALUES (?, ?)", (f, filehash))
            conn.commit()

        except Exception as e:
            print(f"Trying fallback for unreadable file: {f}")
            try:
                fallback_cols = infer_columns_structure()
                df = pd.read_csv(f, dtype=str, header=None, on_bad_lines='skip', engine='python')
                df = df.iloc[:, :fallback_cols]
                df.columns = [f"column_{i+1}" for i in range(df.shape[1])]
                df['_hash'] = df.astype(str).apply(lambda row: hashlib.sha256("".join(row).encode()).hexdigest(), axis=1)
                table_name = generate_table_name(f)
                cols_def = ", ".join([f'"{col}" TEXT' for col in df.columns])
                cur.execute(f"CREATE TABLE IF NOT EXISTS '{table_name}' ({cols_def})")
                cur.execute(f"SELECT _hash FROM '{table_name}'")
                existing_hashes = set(row[0] for row in cur.fetchall())
                new_rows = df[~df['_hash'].isin(existing_hashes)]

                if not new_rows.empty:
                    new_rows.to_sql(table_name, conn, if_exists='append', index=False)
                    print(f"(Fallback) Imported {len(new_rows)} rows into table: {table_name}")
                else:
                    print(f"(Fallback) No new data for: {f}")

                cur.execute("INSERT OR REPLACE INTO imported_files (filename, filehash) VALUES (?, ?)", (f, filehash))
                conn.commit()
            except Exception as inner_e:
                print(f"Failed to import even with fallback: {f} | Error: {inner_e}")

    conn.close()

def search_db(keyword):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall() if row[0].startswith(TABLE_PREFIX)]

    found_any = False
    for table in tables:
        try:
            cur.execute(f"PRAGMA table_info('{table}')")
            all_columns_info = cur.fetchall()
            if not all_columns_info:
                continue

            all_columns = [row[1] for row in all_columns_info]
            columns = [col for col in all_columns if col != '_hash']

            terms = keyword.split()
            sql = f"SELECT * FROM '{table}' WHERE " + " AND ".join([
                "(" + " OR ".join([f"{col} LIKE ?" for col in columns]) + ")" for _ in terms
            ])
            args = []
            for term in terms:
                args.extend([f"%{term}%"] * len(columns))

            cur.execute(sql, args)
            rows = cur.fetchall()

            if rows:
                found_any = True
                print(f"\nMatches in {table}:")
                print(" | ".join(columns))
                for row in rows:
                    row_dict = dict(zip(all_columns, row))
                    display_row = [str(row_dict.get(col, "")) for col in columns]
                    print(" | ".join(display_row))
        except Exception as e:
            print(f"Error searching in table {table}: {e}")

    if not found_any:
        print("No matches found.")
    conn.close()

def main():
    while True:
        print("\n=== CSV to SQLite Tool ===")
        print("1. Import CSV files from folder")
        print("2. Search data")
        print("3. Exit")
        choice = input("Enter choice (1-3): ").strip()

        if choice == '1':
            folder = input("Enter folder path containing CSV files: ").strip()
            if os.path.isdir(folder):
                import_csvs_to_db(folder)
            else:
                print("Invalid folder path.")
        elif choice == '2':
            keyword = input("Enter keyword to search: ").strip()
            if keyword:
                search_db(keyword)
            else:
                print("Keyword cannot be empty.")
        elif choice == '3':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Try again.")

if __name__ == "__main__":
    main()
