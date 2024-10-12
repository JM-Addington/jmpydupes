import os
import sys
import argparse
import datetime
import sqlite3
import traceback
import csv
import xxhash
import concurrent.futures
from pathlib import Path, PurePath

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
    # Ensure file_path is a Path object and get the absolute path
    if not isinstance(file_path, Path):
        file_path = Path(file_path)
    file_path = file_path.absolute()

    # Check if the file exists using os.lstat
    try:
        os.lstat(file_path)
    except FileNotFoundError:
        # Handle missing file
        return None

    print(f"PyDupes: Processing {file_path}")
    try:
        # Get file size and last modified time
        stat = file_path.stat()
        size = stat.st_size
        last_modified = datetime.datetime.fromtimestamp(stat.st_mtime)

        # Calculate xxHash
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)

        file_hash = hasher.hexdigest()
        return file_hash, str(file_path), size, last_modified
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        traceback.print_exc()
        return None  # Return None if there was an error

def process_file_for_thread(file_path):
    try:
        file_path = Path(file_path).resolve()
        if not file_path.exists():
            print(f"File does not exist: {file_path}")
            return None

        # Get file size and last modified time
        stat = file_path.stat()
        size = stat.st_size
        last_modified = datetime.datetime.fromtimestamp(stat.st_mtime)

        # Calculate xxHash
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                hasher.update(chunk)
        file_hash = hasher.hexdigest()

        return (file_hash, str(file_path), size, last_modified)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        traceback.print_exc()
        return None

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

def insert_data_batch(data_list):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        sql = '''
        INSERT OR REPLACE INTO files (hash, path, size, last_modified, last_checked)
        VALUES (?, ?, ?, ?, ?)
        '''
        now = datetime.datetime.now()
        data_with_timestamp = [(*data, now) for data in data_list]
        cursor.executemany(sql, data_with_timestamp)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error during batch insert: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error during batch insert: {e}", file=sys.stderr)
        traceback.print_exc()
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

def load_existing_paths():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT path FROM files')
    rows = cursor.fetchall()
    close_db_connection(conn)
    existing_paths = set(row[0] for row in rows)
    return existing_paths

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

def get_duplicates(preferred_source_directories=None, within_directory=None):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all files (or files within the specified directory)
    if within_directory:
        within_directory = os.path.abspath(within_directory)
        # Select files within the specified directory
        cursor.execute('''
        SELECT hash, path FROM files WHERE path LIKE ?
        ''', (f'{within_directory}%',))
    else:
        # Get all files
        cursor.execute('''
        SELECT hash, path FROM files
        ''')
    all_files = cursor.fetchall()

    # Organize files by hash
    files_by_hash = {}
    for file_hash, file_path in all_files:
        files_by_hash.setdefault(file_hash, []).append(file_path)

    duplicates_list = []

    for file_hash, paths in files_by_hash.items():
        if len(paths) < 2:
            continue  # Not a duplicate group

        # Prepare a list to hold file info
        file_info = []
        for file_path in paths:
            # Create a PurePath object
            path_obj = PurePath(file_path)
            # Number of folders is total parts minus 1 (for the file name)
            num_folders = len(path_obj.parts) - 1
            # Length of the entire path string
            path_length = len(str(path_obj))
            # Determine the preference level based on preferred directories
            preference_level = None
            if preferred_source_directories:
                for index, preferred_dir in enumerate(preferred_source_directories):
                    preferred_path = PurePath(preferred_dir)
                    if preferred_path in path_obj.parents or preferred_path == path_obj.parent:
                        preference_level = index  # Lower index means higher preference
                        break  # Stop at the first match
            file_info.append({
                'path': file_path,
                'num_folders': num_folders,
                'path_length': path_length,
                'hash': file_hash,
                'preference_level': preference_level  # None if not in preferred directories
            })

        # If within_directory is specified, filter out files outside of it
        if within_directory:
            file_info = [info for info in file_info if info['path'].startswith(within_directory)]

            # If less than 2 files remain after filtering, no duplicates to process
            if len(file_info) < 2:
                continue

        original_file_info = None
        no_matching_original = False

        if preferred_source_directories:
            # Same selection logic as before
            preferred_files = [info for info in file_info if info['preference_level'] is not None]
            if preferred_files:
                min_preference = min(info['preference_level'] for info in preferred_files)
                highest_pref_files = [info for info in preferred_files if info['preference_level'] == min_preference]
                min_num_folders = min(info['num_folders'] for info in highest_pref_files)
                candidates = [info for info in highest_pref_files if info['num_folders'] == min_num_folders]
                min_path_length = min(info['path_length'] for info in candidates)
                original_candidates = [info for info in candidates if info['path_length'] == min_path_length]
                original_file_info = original_candidates[0]
            else:
                no_matching_original = True
                original_file_info = select_default_original(file_info)
        else:
            original_file_info = select_default_original(file_info)

        # Collect the duplicates excluding the original
        duplicates = [info for info in file_info if info['path'] != original_file_info['path']]

        duplicates_list.append({
            'hash': file_hash,
            'original': original_file_info,
            'duplicates': duplicates,
            'no_matching_original': no_matching_original
        })

    close_db_connection(conn)
    return duplicates_list

def select_default_original(file_info):
    # Default selection: least number of folders, then shortest path length
    min_num_folders = min(info['num_folders'] for info in file_info)
    candidates = [info for info in file_info if info['num_folders'] == min_num_folders]
    min_path_length = min(info['path_length'] for info in candidates)
    original_candidates = [info for info in candidates if info['path_length'] == min_path_length]
    return original_candidates[0]

def list_duplicates_excluding_original(output_file=None, preferred_source_directories=None):
    duplicates_list = get_duplicates(preferred_source_directories=preferred_source_directories, within_directory=within_directory)
    duplicates_excl_original = []

    for group in duplicates_list:
        original_file = group['original']['path']
        duplicates = [info['path'] for info in group['duplicates']]

        if group['no_matching_original']:
            print(f"Duplicate group with hash {group['hash']} has no matching original in specified directories.")
        else:
            print(f"Original file for hash {group['hash']}: {original_file}")

        duplicates_excl_original.extend(duplicates)

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

def list_duplicates_csv(output_file, preferred_source_directories=None):
    duplicates_list = get_duplicates(preferred_source_directories=preferred_source_directories, within_directory=within_directory)
    duplicates_info = []

    for group in duplicates_list:
        original_file_info = group['original']
        duplicates = group['duplicates']

        if group['no_matching_original']:
            status_flag = 'duplicate - no matching original path'
            print(f"Duplicate group with hash {group['hash']} has no matching original in specified directories.")
        else:
            status_flag = 'original'
            print(f"Original file for hash {group['hash']}: {original_file_info['path']}")

        # Add original file info
        duplicates_info.append({
            'status': status_flag,
            'path': original_file_info['path'],
            'hash': group['hash']
        })

        # Add duplicates info
        for info in duplicates:
            duplicates_info.append({
                'status': 'duplicate',
                'path': info['path'],
                'hash': group['hash']
            })

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

def delete_duplicates(preferred_source_directories=None, output_file=None,
                      overwrite=False, append=False, simulate_delete=False, within_directory=None):

    duplicates_list = get_duplicates(preferred_source_directories=preferred_source_directories, within_directory=within_directory)
    total_deleted = 0

    writer = None
    csvfile = None

    # Handle output file options
    if output_file:
        file_exists = os.path.isfile(output_file)
        file_mode = 'w'

        if file_exists:
            if overwrite:
                file_mode = 'w'
            elif append:
                file_mode = 'a'
            else:
                print(f"Error: Output file '{output_file}' already exists. Use --overwrite or --append to specify the desired behavior.", file=sys.stderr)
                return
        else:
            file_mode = 'w'

        try:
            csvfile = open(output_file, file_mode, newline='', encoding='utf-8')
            fieldnames = ['status', 'path', 'hash']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if file_mode == 'w' or (file_mode == 'a' and os.stat(output_file).st_size == 0):
                writer.writeheader()
                csvfile.flush()
        except Exception as e:
            print(f"Error opening file {output_file}: {e}", file=sys.stderr)
            writer = None
            csvfile = None

    try:
        for group in duplicates_list:
            original_file_info = group['original']
            original_path = original_file_info['path']
            original_path_normalized = os.path.normpath(original_path)

            if group['no_matching_original']:
                status_flag = 'kept - no matching original'
                print(f"Duplicate group with hash {group['hash']} has no matching original in specified directories.")
            else:
                status_flag = 'kept'
                print(f"Original file for hash {group['hash']}: {original_path}")

            # Log the original file
            log_entry = {
                'status': status_flag,
                'path': original_path,
                'hash': group['hash']
            }
            if writer:
                writer.writerow(log_entry)
                csvfile.flush()

            for dup_info in group['duplicates']:
                dup_file = dup_info['path']
                dup_file_normalized = os.path.normpath(dup_file)

                # When within_directory is specified, only delete duplicates within that directory
                if within_directory:
                    within_directory_normalized = os.path.normpath(os.path.abspath(within_directory))
                    if not dup_file_normalized.startswith(within_directory_normalized):
                        # Skip duplicates not within the specified directory
                        continue

                try:
                    if not simulate_delete:
                        os.remove(dup_file)
                        print(f"Deleted duplicate file: {dup_file}")
                        status = 'deleted'
                        total_deleted += 1
                    else:
                        print(f"Simulated deletion of duplicate file: {dup_file}")
                        status = 'deleted (simulated)'
                except Exception as e:
                    print(f"Error deleting file {dup_file}: {e}", file=sys.stderr)
                    status = f'error - {e}'

                # Log the duplicate file
                log_entry = {
                    'status': status,
                    'path': dup_file,
                    'hash': group['hash']
                }
                if writer:
                    writer.writerow(log_entry)
                    csvfile.flush()
    finally:
        # Ensure the CSV file is properly closed
        if csvfile:
            csvfile.close()

    print(f"\nTotal duplicates deleted: {total_deleted}")

    if simulate_delete:
        print("Note: Deletion was simulated. No files were actually deleted.")
    return

def remove_missing_files():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT path FROM files')
    rows = cursor.fetchall()
    total_removed = 0

    paths_to_remove = []

    for row in rows:
        file_path = row[0]
        if not os.path.exists(file_path):
            print(f"Marking {file_path} for removal from database as it no longer exists on disk.")
            paths_to_remove.append((file_path,))

    if paths_to_remove:
        cursor.executemany('DELETE FROM files WHERE path = ?', paths_to_remove)
        conn.commit()
        total_removed = len(paths_to_remove)
    else:
        print("No missing files found in the database.")

    close_db_connection(conn)
    print(f"Total entries removed from database: {total_removed}")
    

def main(directory, skip_existing=False, num_threads=4):
    print("Initializing database and tables...")
    create_db_and_table()

    print(f"Scanning directory: {directory}")
    # Get all files in the specified directory and subdirectories
    files = list(walk_directory(directory))
    print(f"Total files found: {len(files)}")

    # Exclude existing files if skip_existing is True
    if skip_existing:
        print("Loading existing file paths from database to skip already processed files...")
        existing_paths = load_existing_paths()
        files_to_process = [file for file in files if str(Path(file).resolve()) not in existing_paths]
        print(f"Files to process after skipping existing: {len(files_to_process)}")
    else:
        files_to_process = files

    if not files_to_process:
        print("No new files to process.")
        return

    print(f"Processing {len(files_to_process)} files with {num_threads} threads per batch...")

    # Process files in batches, batch size equals the number of threads
    total_batches = (len(files_to_process) + num_threads - 1) // num_threads
    for batch_num, i in enumerate(range(0, len(files_to_process), num_threads), start=1):
        batch = files_to_process[i:i + num_threads]
        print(f"\nProcessing batch {batch_num}/{total_batches}: {len(batch)} files")
        
        # Use ThreadPoolExecutor for multithreading
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            results = list(executor.map(process_file_for_thread, batch))
        
        # Filter out None results and prepare data for bulk insertion
        data_to_insert = [data for data in results if data is not None]
        
        # Bulk insert into the database
        if data_to_insert:
            print(f"Inserting {len(data_to_insert)} records into the database...")
            insert_data_batch(data_to_insert)
            print("Database insertion complete.")
        else:
            print("No new files to insert in this batch.")

    print("\nProcessing complete.")
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process files and find duplicates.')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Subparser for the 'process' command
    parser_process = subparsers.add_parser('process', help='Process a directory to find duplicates')
    parser_process.add_argument('directory', help='Directory to process')
    parser_process.add_argument('--skip-existing', action='store_true', help='Skip processing files that are already in the database')
    parser_process.add_argument('--threads', type=int, default=4, help='Number of threads per batch for hashing')


    # Subparser for the 'rescan-duplicates' command
    parser_rescan = subparsers.add_parser('rescan-duplicates', help='Rescan duplicate files')

    parser_clean_db = subparsers.add_parser('clean-db', help='Remove entries from the database for files that no longer exist on disk')

    # Subparser for the 'list-duplicates' command
    parser_list = subparsers.add_parser('list-duplicates', help='List duplicates excluding originals')
    parser_list.add_argument('-o', '--output', help='Output file to write the list to')
    parser_list.add_argument('--prefer-directory', help='Preferred source directories for selecting original files (comma-separated if multiple)')
    parser_list.add_argument('--within-directory', help='Only examine duplicates within this directory')

    # Subparser for the 'list-duplicates-csv' command
    parser_csv = subparsers.add_parser('list-duplicates-csv', help='List duplicates and originals in CSV format')
    parser_csv.add_argument('-o', '--output', required=True, help='Output CSV file to write the list to')
    parser_csv.add_argument('--prefer-directory', help='Preferred source directories for selecting original files (comma-separated if multiple)')
    parser_csv.add_argument('--within-directory', help='Only examine duplicates within this directory')

    # Subparser for the 'delete-duplicates' command
    parser_delete = subparsers.add_parser('delete-duplicates', help='Delete duplicate files')
    parser_delete.add_argument('--prefer-directory', help='Preferred source directories for selecting original files (comma-separated if multiple)')
    parser_delete.add_argument('-o', '--output', help='Output CSV file to log the deleted files')
    group = parser_delete.add_mutually_exclusive_group()
    group.add_argument('--overwrite', action='store_true', help='Overwrite the output file if it exists')
    group.add_argument('--append', action='store_true', help='Append to the output file if it exists')
    parser_delete.add_argument('--simulate-delete', action='store_true', help='Simulate deletion without actually deleting files')
    parser_delete.add_argument('--within-directory', help='Only examine duplicates within this directory')

    args = parser.parse_args()

    if args.command == 'process':
        directory_to_process = args.directory
        if not os.path.isdir(directory_to_process):
            print(f"Error: {directory_to_process} is not a valid directory", file=sys.stderr)
            sys.exit(1)
        skip_existing = args.skip_existing
        num_threads = args.threads
        main(directory_to_process, skip_existing=skip_existing, num_threads=num_threads)


    elif args.command == 'rescan-duplicates':
        rescan_duplicates()

    elif args.command == 'clean-db':
        remove_missing_files()

    elif args.command == 'list-duplicates':
        # Handle arguments specific to this command
        if args.prefer_directory:
            preferred_directories = [d.strip() for d in args.prefer_directory.split(',')]
        else:
            preferred_directories = None

        within_directory = args.within_directory

        list_duplicates_excluding_original(output_file=args.output, preferred_source_directories=preferred_directories, within_directory=within_directory)

    elif args.command == 'list-duplicates-csv':
        # Handle arguments specific to this command
        if args.prefer_directory:
            preferred_directories = [d.strip() for d in args.prefer_directory.split(',')]
        else:
            preferred_directories = None

        within_directory = args.within_directory

        list_duplicates_csv(output_file=args.output, preferred_source_directories=preferred_directories, within_directory=within_directory)

    elif args.command == 'delete-duplicates':
        # Handle arguments specific to this command
        if args.prefer_directory:
            preferred_directories = [d.strip() for d in args.prefer_directory.split(',')]
        else:
            preferred_directories = None

        within_directory = args.within_directory

        delete_duplicates(
            preferred_source_directories=preferred_directories,
            output_file=args.output,
            overwrite=args.overwrite,
            append=args.append,
            simulate_delete=args.simulate_delete,
            within_directory=within_directory
        )

    else:
        parser.print_help()