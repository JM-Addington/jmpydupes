import os
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys
import datetime
import traceback
import argparse
from pathlib import PurePath

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
        # Create index on hash
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON files (hash)')
        conn.commit()

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Create index on hash if it doesn't exist
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON files (hash);')
    conn.commit()
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
    from pathlib import PurePath  # Import PurePath for OS-agnostic path handling

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

        # Prepare a list to hold file info
        file_info = []
        for file_path in paths:
            # Create a PurePath object
            path_obj = PurePath(file_path)
            # Get the parts of the path
            path_parts = path_obj.parts
            # Number of folders is total parts minus 1 (for the file name)
            num_folders = len(path_parts) - 1
            # Length of the entire path string
            path_length = len(str(path_obj))
            file_info.append({
                'path': file_path,
                'num_folders': num_folders,
                'path_length': path_length
            })

        # Find the minimum number of folders
        min_num_folders = min(info['num_folders'] for info in file_info)
        # Filter files that have the minimum number of folders
        candidates = [info for info in file_info if info['num_folders'] == min_num_folders]
        # Among candidates, find the minimum path length
        min_path_length = min(info['path_length'] for info in candidates)
        # Filter candidates that have the minimum path length
        original_candidates = [info for info in candidates if info['path_length'] == min_path_length]
        # Select the original file (here we pick the first one)
        original_file = original_candidates[0]['path']
        print(f"Original file for hash {file_hash}: {original_file}")

        # Exclude the original file from duplicates
        duplicates = [info['path'] for info in file_info if info['path'] != original_file]
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

def list_duplicates_csv(output_file):
    import csv
    from pathlib import PurePath  # For OS-agnostic path handling

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all hashes where there are duplicates
    cursor.execute('''
    SELECT hash FROM files
    GROUP BY hash
    HAVING COUNT(*) > 1
    ''')
    hashes = [row[0] for row in cursor.fetchall()]

    duplicates_info = []

    for file_hash in hashes:
        cursor.execute('''
        SELECT path FROM files WHERE hash = ?
        ''', (file_hash,))
        paths = [row[0] for row in cursor.fetchall()]

        # Prepare a list to hold file info
        file_info = []
        for file_path in paths:
            # Create a PurePath object
            path_obj = PurePath(file_path)
            # Get the parts of the path
            path_parts = path_obj.parts
            # Number of folders is total parts minus 1 (for the file name)
            num_folders = len(path_parts) - 1
            # Length of the entire path string
            path_length = len(str(path_obj))
            file_info.append({
                'path': file_path,
                'num_folders': num_folders,
                'path_length': path_length,
                'hash': file_hash
            })

        # Find the minimum number of folders
        min_num_folders = min(info['num_folders'] for info in file_info)
        # Filter files that have the minimum number of folders
        candidates = [info for info in file_info if info['num_folders'] == min_num_folders]
        # Among candidates, find the minimum path length
        min_path_length = min(info['path_length'] for info in candidates)
        # Filter candidates that have the minimum path length
        original_candidates = [info for info in candidates if info['path_length'] == min_path_length]
        # Select the original file (pick the first one)
        original_file_info = original_candidates[0]
        original_file = original_file_info['path']
        print(f"Original file for hash {file_hash}: {original_file}")

        # Tag each file as 'original' or 'duplicate' and collect info
        for info in file_info:
            if info['path'] == original_file:
                duplicates_info.append({
                    'status': 'original',
                    'path': info['path'],
                    'hash': info['hash']
                })
            else:
                duplicates_info.append({
                    'status': 'duplicate',
                    'path': info['path'],
                    'hash': info['hash']
                })

    close_db_connection(conn)

    # Output the data to a CSV file
    if output_file:
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['status', 'path', 'hash']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for info in duplicates_info:
                    writer.writerow(info)
            print(f"\nList of duplicates and originals has been written to {output_file}")
        except Exception as e:
            print(f"Error writing to file {output_file}: {e}", file=sys.stderr)
    else:
        # If no output file specified, print to console
        print("\nList of duplicates and originals:")
        for info in duplicates_info:
            print(f"{info['status']}, {info['path']}, {info['hash']}")

    return duplicates_info

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

    # Subparser for the 'list-duplicates-csv' command
    parser_csv = subparsers.add_parser('list-duplicates-csv', help='List duplicates and originals in CSV format')
    parser_csv.add_argument('-o', '--output', required=True, help='Output CSV file to write the list to')

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
    elif args.command == 'list-duplicates-csv':
        list_duplicates_csv(output_file=args.output)
    else:
        parser.print_help()