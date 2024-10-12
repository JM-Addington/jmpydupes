import os
import shutil
import sqlite3
from pathlib import Path
from finddupes import (
    create_db_and_table,
    get_db_connection,
    close_db_connection,
    process_file,
    insert_data_batch,
    walk_directory,
    load_existing_paths,
    rescan_duplicates,
    get_duplicates,
    select_default_original,
    list_duplicates_excluding_original,
    list_duplicates_csv,
    delete_duplicates,
    remove_missing_files,
    main,
)

# Set the environment variable for test database
os.environ['TEST_DB_NAME'] = 'test_file_data.db'

def setup_test_data():
    """
    Set up test directories and files for testing.
    Creates files with known content to identify duplicates.
    """
    base_dir = './test'
    
    # Remove existing test directory if it exists
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    
    # Create directories
    dirs = ['dir1', 'dir2', 'dir3', 'dir4']  # Added 'dir4'
    subdirs = {
        'dir1': ['subdir1'],
        'dir2': ['subdir2'],
        'dir4': ['subdir3']  # Added 'subdir3' under 'dir4'
    }
    
    for d in dirs:
        os.makedirs(os.path.join(base_dir, d))
    
    # Create subdirectories
    for d, subdir_list in subdirs.items():
        for sub in subdir_list:
            os.makedirs(os.path.join(base_dir, d, sub))
    
    # Create files with known content
    files = [
        # (relative path, content)
        ('dir1/file1.txt', '11111'),  # Duplicate in dir2 and dir4
        ('dir2/file1.txt', '11111'),  # Duplicate of dir1/file1.txt
        ('dir4/file1.txt', '11111'),  # Duplicate of dir1/file1.txt
        
        ('dir1/file2.txt', '22222'),  # Unique
        ('dir2/file4.txt', '33333'),  # Unique
        
        ('dir1/subdir1/file3.txt', '44444'),  # Duplicate in dir2/subdir2 and dir4/subdir3
        ('dir2/subdir2/file3.txt', '44444'),  # Duplicate of dir1/subdir1/file3.txt
        ('dir4/subdir3/file3.txt', '44444'),  # Duplicate of dir1/subdir1/file3.txt
        
        ('dir3/file5.txt', '55555'),  # Unique
    ]
    
    for filepath, content in files:
        full_path = os.path.join(base_dir, filepath)
        # Ensure the directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as f:
            f.write(content)

def setup_test_environment():
    """
    Set up the test environment by creating test data and ensuring a clean database.
    """
    # Setup test data
    setup_test_data()
    
    # Ensure the test database is clean
    if os.path.exists('test_file_data.db'):
        os.remove('test_file_data.db')
    
    # Remove any existing log files or output files
    if os.path.exists('duplicates.csv'):
        os.remove('duplicates.csv')
    if os.path.exists('finddupes.log'):
        os.remove('finddupes.log')

def test_process():
    """
    Test the processing of files in the test directories.
    """
    # Process dir1
    main('./test/dir1', skip_existing=False, num_threads=2)
    
    # Verify that files from dir1 are in the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT path FROM files')
    paths = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    expected_files = [
        str(Path('./test/dir1/file1.txt').resolve()),
        str(Path('./test/dir1/file2.txt').resolve()),
        str(Path('./test/dir1/subdir1/file3.txt').resolve()),
    ]
    
    assert all(file in paths for file in expected_files), "Not all files from dir1 are in the database."

def test_list_duplicates_within_directory():
    """
    Test listing duplicates within dir1.
    Since dir1 has no duplicates within itself, we expect no duplicates.
    """
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=None,
        within_directory='./test/dir1'
    )
    # Since dir1 has no duplicates within itself, expect an empty list
    assert len(duplicates) == 0, "There should be no duplicates within dir1."

def test_processing_additional_directory():
    """
    Test processing dir2 and identifying duplicates across directories.
    """
    # Process dir2 with skip_existing to avoid reprocessing files
    main('./test/dir2', skip_existing=True, num_threads=2)
    
    # List duplicates across dir1 and dir2, preferring dir1
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=[str(Path('./test/dir1').resolve())]
    )
    
    expected_duplicates = [
        str(Path('./test/dir2/file1.txt').resolve()),
        str(Path('./test/dir2/subdir2/file3.txt').resolve()),
    ]
    
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."
    assert len(duplicates) == len(expected_duplicates), "Unexpected duplicates found."

def test_multiple_preferred_directories():
    """
    Test with multiple preferred directories to ensure preference order is respected.
    """
    # Process dir4 with skip_existing to avoid reprocessing files
    main('./test/dir4', skip_existing=True, num_threads=2)
    
    # Get absolute paths for preferred directories, in order of preference
    # dir1 has highest preference, dir4 is next
    preferred_dirs = [
        str(Path('./test/dir1').resolve()),
        str(Path('./test/dir4').resolve()),
    ]
    
    # List duplicates across all directories, preferring dir1 and then dir4
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=preferred_dirs
    )
    
    # Expected duplicates should be in dir2
    expected_duplicates = [
        str(Path('./test/dir2/file1.txt').resolve()),
        str(Path('./test/dir2/subdir2/file3.txt').resolve()),
    ]
    
    # Duplicates in dir4 should not appear since it's a preferred directory after dir1
    duplicates_in_dir4 = [
        str(Path('./test/dir4/file1.txt').resolve()),
        str(Path('./test/dir4/subdir3/file3.txt').resolve()),
    ]
    
    # Verify that duplicates in dir2 are identified
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates in dir2 not found."
    
    # Verify that duplicates in dir4 are not included since dir4 is a preferred directory
    assert all(file not in duplicates for file in duplicates_in_dir4), "Duplicates in dir4 should not be considered for deletion."
    
    # Check that the total number of duplicates matches the expected number
    assert len(duplicates) == len(expected_duplicates), "Unexpected number of duplicates found."

    # Optionally, perform deletion to see if the script deletes the correct files
    delete_duplicates(
        preferred_source_directories=preferred_dirs,
        simulate_delete=True
    )
    # Ensure files in dir4 are not deleted
    for file in duplicates_in_dir4:
        assert os.path.exists(file), f"File {file} in preferred directory should not be deleted."

def test_simulated_deletion():
    """
    Test simulated deletion of duplicates.
    """
    delete_duplicates(
        preferred_source_directories=[str(Path('./test/dir1').resolve())],
        simulate_delete=True
    )
    # In simulated deletion, files are not actually deleted
    assert os.path.exists('./test/dir2/file1.txt'), "File should not be deleted in simulation."

def test_actual_deletion():
    """
    Test actual deletion of duplicates.
    """
    delete_duplicates(
        preferred_source_directories=[str(Path('./test/dir1').resolve())],
        simulate_delete=False
    )
    # Verify that duplicates have been deleted
    assert not os.path.exists('./test/dir2/file1.txt'), "Duplicate file was not deleted."
    assert not os.path.exists('./test/dir2/subdir2/file3.txt'), "Duplicate file was not deleted."

def test_csv_output():
    """
    Test generating CSV output of duplicates.
    """
    list_duplicates_csv(
        output_file='duplicates.csv',
        preferred_source_directories=[str(Path('./test/dir1').resolve())]
    )
    # Check that the CSV file exists and has content
    assert os.path.exists('duplicates.csv'), "CSV output file was not created."
    with open('duplicates.csv', 'r') as csvfile:
        content = csvfile.read()
    assert 'file1.txt' in content, "CSV output does not contain expected data."
    assert 'file3.txt' in content, "CSV output does not contain expected data."

def test_rescan_duplicates():
    """
    Test rescanning duplicates to update the database.
    """
    rescan_duplicates()
    # We can check that the last_checked time has been updated
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT last_checked FROM files')
    last_checked_times = [row[0] for row in cursor.fetchall()]
    conn.close()
    assert all(last_checked_times), "Not all files have a last_checked timestamp."

def test_clean_db():
    """
    Test cleaning the database by removing entries for missing files.
    """
    # Delete a file manually
    if os.path.exists('./test/dir1/file2.txt'):
        os.remove('./test/dir1/file2.txt')
    remove_missing_files()
    # Verify that the file is no longer in the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT path FROM files WHERE path = ?', (str(Path('./test/dir1/file2.txt').resolve()),))
    result = cursor.fetchone()
    conn.close()
    assert result is None, "Missing file was not removed from the database."

def teardown_test_environment():
    """
    Clean up the test environment by removing test data and the test database.
    """
    shutil.rmtree('./test')
    if os.path.exists('test_file_data.db'):
        os.remove('test_file_data.db')
    if os.path.exists('duplicates.csv'):
        os.remove('duplicates.csv')
    if os.path.exists('finddupes.log'):
        os.remove('finddupes.log')

if __name__ == '__main__':
    setup_test_environment()
    try:
        test_process()
        test_list_duplicates_within_directory()
        test_processing_additional_directory()
        test_multiple_preferred_directories()
        test_simulated_deletion()
        test_actual_deletion()
        test_csv_output()
        test_rescan_duplicates()
        test_clean_db()
        print("All tests passed successfully.")
    finally:
        teardown_test_environment()