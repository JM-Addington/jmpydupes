import os
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys
import datetime
import traceback
import argparse

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
    
    # Check to see if the file still exists, if not, print an info message and remove it from the database
    if not os.path.exists(file_path):
        print(f"PyDupes: {file_path} no longer exists, removing from database")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
        DELETE FROM files WHERE path = ?
        ''', (file_path,))
        conn.commit()
        close_db_connection(conn)
        return None
    
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
        
# Rescan duplicates
def rescan_duplicates():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    SELECT hash, path FROM files
    WHERE hash IN (
        SELECT hash FROM files
        GROUP BY hash
        HAVING COUNT(*) > 1
    )
    ORDER BY hash
    ''')
    duplicates = cursor.fetchall()
    close_db_connection(conn)
    
    # Rescan each duplicate file, no thread pool
    for duplicate in duplicates:
        data = process_file(duplicate[1])
        if data is not None:
            insert_data(data)
    
    return duplicates

def list_duplicates_excluding_original(output_file=None):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all hashes where there are duplicates
    cursor.execute('''
    SELECT hash FROM files
    GROUP BY hash
    HAVING COUNT(*) > 1
    ''')
    hashes = [row[0] for row in cursor.fetchall()]

    duplicates_excl_original = []

    for file_hash in hashes:
        cursor.execute('''
        SELECT path FROM files WHERE hash = ?
        ''', (file_hash,))
        paths = [row[0] for row in cursor.fetchall()]

        # Find the original file (with the shortest path)
        original = min(paths, key=lambda x: len(x))
        print(f"Original file for hash {file_hash}: {original}")

        # Exclude the original from duplicates
        duplicates = [p for p in paths if p != original]

        duplicates_excl_original.extend(duplicates)

    close_db_connection(conn)

    # Output the list of duplicates excluding originals
    if output_file:
        try:
            with open(output_file, 'w') as f:
                for dup_file in duplicates_excl_original:
                    f.write(f"{dup_file}\n")
            print(f"\nList of duplicate files excluding originals has been written to {output_file}")
        except Exception as e:
            print(f"Error writing to file {output_file}: {e}", file=sys.stderr)
    else:
        print("\nList of duplicate files excluding originals:")
        for dup_file in duplicates_excl_original:
            print(dup_file)

    return duplicates_excl_original

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
    parser = argparse.ArgumentParser(description='Process files and find duplicates.')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Subparser for the 'process' command
    parser_process = subparsers.add_parser('process', help='Process a directory to find duplicates')
    parser_process.add_argument('directory', help='Directory to process')
    
    # Subparser for the 'rescan-duplicates' command
    parser_rescan = subparsers.add_parser('rescan-duplicates', help='Rescan duplicate files')
    
    # Subparser for the 'list-duplicates' command
    parser_list = subparsers.add_parser('list-duplicates', help='List duplicates excluding originals')
    parser_list.add_argument('-o', '--output', help='Output file to write the list to')
    
    args = parser.parse_args()
    
    if args.command == 'process':
        directory_to_process = args.directory
        if not os.path.isdir(directory_to_process):
            print(f"Error: {directory_to_process} is not a valid directory", file=sys.stderr)
            sys.exit(1)
        main(directory_to_process)
    elif args.command == 'rescan-duplicates':
        rescan_duplicates()
    elif args.command == 'list-duplicates':
        list_duplicates_excluding_original(output_file=args.output)
    else:
        parser.print_help()