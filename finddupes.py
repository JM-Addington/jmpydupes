import os
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys
import datetime
import traceback
import threading

DB_NAME = 'file_data.db'

def create_db_and_table():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT,
            path TEXT UNIQUE,
            size INTEGER,
            last_modified DATETIME,
            last_checked DATETIME
        )
        ''')
        conn.commit()

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    return conn

def close_db_connection(conn):
    if conn:
        conn.close()

def process_file(file_path):
    file_path = Path(file_path).resolve()  # Get the full path
    print(f"PyDupes: Processing {file_path}")
    
    try:
        # Get file size and last modified time
        stat = file_path.stat()
        size = stat.st_size
        last_modified = datetime.datetime.fromtimestamp(stat.st_mtime)
        
        # Calculate MD5 hash
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)
        
        return file_hash.hexdigest(), str(file_path), size, last_modified
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
    
    return None  # Return None if there was an error

def insert_data(data):
    now = datetime.datetime.now()
    conn = get_db_connection()
    try:
        # If this file already exists in the database, update the last_checked time
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id FROM files WHERE path = ?
        ''', (data[1],))
        existing_file = cursor.fetchone()
        
        if existing_file:
            cursor.execute('''
            UPDATE files
            SET hash = ?, size = ?, last_modified = ?, last_checked = ?
            WHERE id = ?
            ''', (data[0], data[2], data[3], now, existing_file[0]))
            conn.commit()
            
            print (f"PyDupes: Updated {data[1]}")
            return
        
        with conn:  # This automatically handles commit/rollback
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO files (hash, path, size, last_modified, last_checked)
            VALUES (?, ?, ?, ?, ?)
            ''', (*data, now))
    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error inserting data: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
    finally:
        close_db_connection(conn)

def walk_directory(directory):
    for root, dirs, files in os.walk(directory, topdown=True, onerror=None, followlinks=False):
        for name in files:
            try:
                yield os.path.join(root, name)
            except Exception as e:
                print(f"Error accessing file {name} in {root}: {str(e)}", file=sys.stderr)
        
        # Handle permission errors for directories
        dirs[:] = [d for d in dirs if os.access(os.path.join(root, d), os.R_OK)]

def main(directory):
    # Create database and table if they don't exist
    create_db_and_table()
    
    # Get all files in the specified directory and subdirectories
    files = walk_directory(directory)
    
    for file in files:
        data = process_file(file)
        if data is not None:
            insert_data(data)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <directory_to_process>")
        sys.exit(1)
    
    directory_to_process = sys.argv[1]
    if not os.path.isdir(directory_to_process):
        print(f"Error: {directory_to_process} is not a valid directory", file=sys.stderr)
        sys.exit(1)
    
    main(directory_to_process)