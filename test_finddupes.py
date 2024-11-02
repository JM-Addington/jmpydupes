import os
import sqlite3
import shutil
from faker import Faker
import xxhash
from pathlib import Path
import pytest
import random
import string

# Create a custom provider
class CustomProvider:
    @staticmethod
    def hex_string(length=16):
        return ''.join(random.choices(string.hexdigits.lower(), k=length))

# Set the environment variable for test database before importing finddupes
ORIGINAL_DB_NAME = os.environ.get('DB_NAME', None)
TEST_DB_NAME = 'test_file_data.db'
base_dir = './test'
os.environ['DB_NAME'] = TEST_DB_NAME

fake = Faker()
fake.add_provider(CustomProvider())

from finddupes import (
    processed_data,
    create_db_and_table,
    get_db_connection,
    close_db_connection,
    check_db_connection,
    insert_data_batch,
    walk_directory,
    load_existing_paths,
    rescan_duplicates,
    get_duplicates,
    select_original,
    list_duplicates_excluding_original,
    list_duplicates_csv,
    delete_duplicates,
    remove_missing_files,
    main,
)


def setup_test_data(files):
    """
    Set up test directories and files for testing.
    Creates files with known content to identify duplicates.
    """
    # Remove existing test directory if it exists
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    for filepath, content in files:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            f.write(content)


@pytest.fixture
def setup_environment():
    """
    Fixture to set up the test environment before each test.
    """
    os.environ['DB_NAME'] = TEST_DB_NAME

    # Ensure the test database is clean
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)

    # Remove any existing log files or output files
    if os.path.exists('duplicates.csv'):
        os.remove('duplicates.csv')
    if os.path.exists('finddupes.log'):
        os.remove('finddupes.log')

    processed_data.clear()

    yield

    # Teardown code
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    if os.path.exists('duplicates.csv'):
        os.remove('duplicates.csv')
    if os.path.exists('finddupes.log'):
        os.remove('finddupes.log')

    # Restore the original DB_NAME environment variable
    if ORIGINAL_DB_NAME:
        os.environ['DB_NAME'] = ORIGINAL_DB_NAME
    else:
        os.environ.pop('DB_NAME', None)


def test_process(setup_environment):
    """
    Test the processing of files in the test directories.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1-no_dupes/file1.txt', '11111'),
        (base_dir + '/dir2/file1.txt', '11111'),
        (base_dir + '/dir4/file1.txt', '11111'),
        (base_dir + '/dir1-no_dupes/file2.txt', '22222'),
        (base_dir + '/dir2/file4.txt', '33333'),
        (base_dir + '/dir1-no_dupes/subdir1-no_dupes/file3.txt', '44444'),
        (base_dir + '/dir2/subdir2/file3.txt', '44444'),
        (base_dir + '/dir4/subdir3/file3.txt', '44444'),
        (base_dir + '/dir3/file5.txt', '55555'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    # Verify that files from dir1-no_dupes are in the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT path, hash FROM files')
    results = cursor.fetchall()
    conn.close()

    # Create a mapping from absolute paths to expected hashes
    expected_hashes = {}
    for file in files_to_create:
        file_path_abs = str(Path(file[0]).resolve())
        file_hash = xxhash.xxh64(file[1]).hexdigest()
        expected_hashes[file_path_abs] = file_hash

    # Check that each file in the database matches the expected hash
    for result in results:
        path_in_db = str(result[0])
        hash_in_db = result[1]

        # Assert that the path from the database is in the expected hashes
        assert expected_hashes.get(path_in_db, None) is not None, f"File {path_in_db} not in expected files."

        # Assert that the hash matches the expected hash
        expected_hash = expected_hashes.get(path_in_db, None)
        assert hash_in_db == expected_hash, f"Hash does not match for file {path_in_db}."


def test_list_duplicates_within_directory(setup_environment):
    """
    Test listing duplicates within dir1-no_dupes.
    Since dir1-no_dupes has no duplicates within itself, we expect no duplicates.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1-no_dupes/file1.txt', '11111'),
        (base_dir + '/dir1-no_dupes/file2.txt', '22222'),
        (base_dir + '/dir1-no_dupes/subdir1-no_dupes/file1-dupe4.txt', '44444'),

        (base_dir + '/dir2_dupes_within/subdir2/file3-dupe4.txt', '44444'),
        (base_dir + '/dir2_dupes_within/subdir3/file3-dupe5.txt', '44444'),

        (base_dir + '/dir3_dupes_with_dir2/file5.txt-dupe4', '44444'),
        (base_dir + '/dir3_dupes_with_dir2/file6.txt', '55555'),
    ]

    setup_test_data(files_to_create)

    # Scan for duplicates
    main(base_dir, skip_existing=False, num_threads=2)

    print("Testing duplicates within /dir1-no_dupes [expect none]")
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=None,
        within_directory=base_dir + '/dir1-no_dupes'
    )

    # Since dir1-no_dupes has no duplicates within itself, expect an empty list
    assert len(duplicates) == 0, "There should be no duplicates within dir1-no_dupes."

    print("Testing duplicates within /dir2-dupes_within [expect 1]")
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=None,
        within_directory=base_dir + '/dir2_dupes_within'
    )
    # Since dir2_dupes_within has duplicates, expect duplicates
    assert len(duplicates) == 1, "There should be duplicates within dir2_dupes_within."

    print("Testing duplicates within entire test directory [expect 3]")
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=None,
        within_directory=base_dir
    )

    assert len(duplicates) == 3, "There should be 3 duplicates within base_dir."


def test_processing_additional_directory(setup_environment):
    """
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1/original.txt', '44444'),

        (base_dir + '/dir2_dupes_within/subdir2/file3-dupe4.txt', '44444'),
        (base_dir + '/dir2_dupes_within/subdir3/file5-dupe4.txt', '44444'),

        (base_dir + '/dir3_dupes_with_dir2/file5.txt-dupe4', '123456'),
        (base_dir + '/dir3_dupes_with_dir2/file6.txt', '55555'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, num_threads=2)

    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=[str(Path(base_dir + '/dir1-no_dupes').resolve())]
    )

    expected_duplicates = [
        str(Path(base_dir + '/dir2_dupes_within/subdir2/file3-dupe4.txt').resolve()),
        str(Path(base_dir + '/dir2_dupes_within/subdir3/file5-dupe4.txt').resolve()),
    ]

    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."
    assert len(duplicates) == len(expected_duplicates), "Unexpected duplicates found."


def test_multiple_preferred_directories(setup_environment):
    """
    Test with multiple preferred directories to ensure preference order is respected.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1/file1.txt', '00000'),
        (base_dir + '/dir2/file1.txt', '11111'),
        (base_dir + '/dir3/file1.txt', '11111'),
        (base_dir + '/dir4/file1.txt', '11111'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, num_threads=4)

    # Get absolute paths for preferred directories, in order of preference
    # dir1-no_dupes has the highest preference, dir4 is next
    preferred_dirs = [
        str(Path(base_dir + '/dir1').resolve()),
        str(Path(base_dir + '/dir2').resolve()),
    ]

    # List duplicates across all directories, preferring dir1-no_dupes and then dir4
    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=preferred_dirs
    )

    # Expected duplicates, dir1 has no dupes and dir2 is preferred so dir3 and dir4 should have the duplicate
    expected_duplicates = [
        str(Path(base_dir + '/dir3/file1.txt').resolve()),
        str(Path(base_dir + '/dir4/file1.txt').resolve()),
    ]

    # Verify that duplicates in dir2 are identified
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."

    # Switch it around so dir4 and dir3 are preferred. Dupe should be dir3 and dir2
    preferred_dirs = [
        str(Path(base_dir + '/dir4').resolve()),
        str(Path(base_dir + '/dir3').resolve()),
    ]

    duplicates = list_duplicates_excluding_original(
        preferred_source_directories=preferred_dirs
    )

    expected_duplicates = [
        str(Path(base_dir + '/dir3/file1.txt').resolve()),
        str(Path(base_dir + '/dir2/file1.txt').resolve()),
    ]

    # Verify that duplicates in dir2 are identified
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."


def test_by_depth(setup_environment):
    """
    Test that duplicates are identified by depth.

    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1/file1.txt', '11111'),
        (base_dir + '/dir2/subdir2/file1.txt', '11111'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    duplicates = list_duplicates_excluding_original()

    expected_duplicates = [
        str(Path(base_dir + '/dir2/subdir2/file1.txt').resolve())
    ]

    # Check that the second file was returned as one to be deleted:
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."

    # Check that the other files was NOT returned
    assert(Path(base_dir + '/dir1/file1.txt').resolve() not in duplicates), "Unexpected duplicate found."

def test_by_alphabetic(setup_environment):
    """
    Test that duplicates are identified by depth.

    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/zxy/file1.txt', '11111'),
        (base_dir + '/abc/file1.txt', '11111'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    duplicates = list_duplicates_excluding_original()

    expected_duplicates = [
        str(Path(base_dir + '/zxy/file1.txt').resolve())
    ]

    # Check that the second file was returned as one to be deleted:
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."

    # Check that the other files was NOT returned
    assert (Path(base_dir + '/abc/file1.txt').resolve() not in duplicates), "Unexpected duplicate found."


def test_simulated_deletion(setup_environment):
    """
    Test simulated deletion of duplicates.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1/file1.txt', '11111'),
        (base_dir + '/dir2/file1.txt', '11111'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    duplicates = delete_duplicates(
        preferred_source_directories=[str(Path(base_dir + '/dir1').resolve())],
        simulate_delete=True
    )

    expected_duplicates = [
        str(Path(base_dir + '/dir2/file1.txt').resolve())
    ]

    # Check that the second file was returned as one to be deleted:
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."

    # In simulated deletion, files are not actually deleted
    assert os.path.exists(base_dir + '/dir1/file1.txt'), "File should not be deleted in simulation."
    assert os.path.exists(base_dir + '/dir2/file1.txt'), "File should not be deleted in simulation."


def test_actual_deletion(setup_environment):
    """
    Test actual deletion of duplicates.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1/file1.txt', '11111'),
        (base_dir + '/dir2/file1.txt', '11111'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    duplicates = delete_duplicates(
        preferred_source_directories=[str(Path(base_dir + '/dir1').resolve())],
        simulate_delete=False
    )

    expected_duplicates = [
        str(Path(base_dir + '/dir2/file1.txt').resolve())
    ]

    # Check that the second file was returned as one to be deleted:
    assert all(file in duplicates for file in expected_duplicates), "Expected duplicates not found."

    # Verify that duplicates have been deleted
    assert not os.path.exists(base_dir + '/dir2/file1.txt'), "Duplicate file was not deleted."

    # Verify that the original file is still present!
    assert os.path.exists(base_dir + '/dir1/file1.txt'), "Original file was deleted."


def test_csv_output(setup_environment):
    """
    Test generating CSV output of duplicates.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1/file1.txt', '11111'),
        (base_dir + '/dir1/file2.txt', '22222'),
        (base_dir + '/dir2/file1.txt', '11111'),
        (base_dir + '/dir2/file2.txt', '22222'),
        (base_dir + '/dir3/file1.txt', '11111'),
        (base_dir + '/dir3/file2.txt', '22222'),
        (base_dir + '/dir4/file1.txt', '11111'),
        (base_dir + '/dir4/file2.txt', '22222'),
    ]

    expected_output = {'status': ['original', 'duplicate', 'duplicate', 'duplicate', 'original', 'duplicate', 'duplicate', 'duplicate'],
                       'path': [str(Path(base_dir + '/dir1/file1.txt').resolve()), str(Path(base_dir + '/dir2/file1.txt').resolve()), str(Path(base_dir + '/dir4/file1.txt').resolve()), str(Path(base_dir + '/dir3/file1.txt').resolve()), str(Path(base_dir + '/dir1/file2.txt').resolve()), str(Path(base_dir + '/dir2/file2.txt').resolve()), str(Path(base_dir + '/dir4/file2.txt').resolve()), str(Path(base_dir + '/dir3/file2.txt').resolve())],
                       'hash': ['3baf032d46de01d6', '3baf032d46de01d6', '3baf032d46de01d6', '3baf032d46de01d6', '63718f6861b7ee6f', '63718f6861b7ee6f', '63718f6861b7ee6f', '63718f6861b7ee6f']}

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    list_duplicates_csv(
        output_file='duplicates.csv',
        preferred_source_directories=[str(Path(base_dir + '/dir1').resolve())]
    )
    # Check that the CSV file exists and has content
    assert os.path.exists('duplicates.csv'), "CSV output file was not created."
    with open('duplicates.csv', 'r') as csvfile:
        content = csvfile.read().split("\n")
        header = content[0].split(',')

        # Initialize the dictionary with empty lists
        result_dict = {key: [] for key in header}

        # Populate the dictionary
        for row in content[1:]:
            if row:
                values = row.split(',')
                for key, value in zip(header, values):
                    result_dict[key].append(value)
        print(result_dict)

    assert set(result_dict['hash']) == set(expected_output['hash'])
    assert set(result_dict['path']) == set(expected_output['path'])
    assert set(result_dict['status']) == set(expected_output['status'])



def test_clean_db(setup_environment):
    """
    Test cleaning the database by removing entries for missing files.
    """
    # Create files with known content
    files_to_create = [
        (base_dir + '/dir1-no_dupes/file2.txt', '22222'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    # Delete a file manually
    if os.path.exists(base_dir + '/dir1-no_dupes/file2.txt'):
        os.remove(base_dir + '/dir1-no_dupes/file2.txt')
    remove_missing_files()
    # Verify that the file is no longer in the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT path FROM files WHERE path = ?',
                   (str(Path(base_dir + '/dir1-no_dupes/file2.txt').resolve()),))
    result = cursor.fetchone()
    conn.close()
    assert result is None, "Missing file was not removed from the database."


def test_keep_originals_in_preferred_directory(setup_environment):
    """
    This test ensures that duplicate files are deleted from all other directories but kept in
    preferred directory.
    """
    # Create files to test scenario
    files_to_create = [
        (base_dir + '/file1.txt', '11111'),
        (base_dir + '/file2.txt', '22222'),
        (base_dir + '/file3.txt', '33333'),

        # Creating duplicates in base directory
        (base_dir + '/file4.txt', '22222'),
        (base_dir + '/file5.txt', '33333'),

        # Creating files in preferred directory
        (base_dir + '/preferred_directory/file1.txt', '11111'),
        (base_dir + '/preferred_directory/file2.txt', '22222'),
        (base_dir + '/preferred_directory/file3.txt', '33333'),

        # Creating duplicates in another directory
        (base_dir + '/another_directory/file1.txt', '11111'),
        (base_dir + '/another_directory/file2.txt', '22222'),
        (base_dir + '/another_directory/file3.txt', '33333'),
        (base_dir + '/another_directory/file4.txt', '11111'),
        (base_dir + '/another_directory/file5.txt', '22222'),
        (base_dir + '/another_directory/file6.txt', '33333'),
    ]

    setup_test_data(files_to_create)

    # Process files
    main(base_dir, skip_existing=False, num_threads=2)
    preferred_directories = [base_dir + '/preferred_directory']
    delete_duplicates(preferred_source_directories=preferred_directories)

    assert len(os.listdir(base_dir + '/preferred_directory')) == 3


def test_select_original_in_preferred_directory(setup_environment):
    """
    This test ensures that the original file is selected from the preferred directory when duplicates
    exist in multiple directories.
    """
    # Create files to test scenario
    files_to_create = [
        (base_dir + '/file1.txt', '11111'),
        (base_dir + '/file2.txt', '22222'),
        (base_dir + '/file3.txt', '33333'),

        # Creating duplicates in base directory
        (base_dir + '/file4.txt', '22222'),
        (base_dir + '/file5.txt', '33333'),

        # Creating files in preferred directory
        (base_dir + '/preferred_directory/file1.txt', '11111'),
        (base_dir + '/preferred_directory/file2.txt', '22222'),
        (base_dir + '/preferred_directory/file3.txt', '33333'),

        # Creating duplicates in another directory
        (base_dir + '/another_directory/file1.txt', '11111'),
        (base_dir + '/another_directory/file2.txt', '22222'),
        (base_dir + '/another_directory/file3.txt', '33333'),
        (base_dir + '/another_directory/file4.txt', '11111'),
        (base_dir + '/another_directory/file5.txt', '22222'),
        (base_dir + '/another_directory/file6.txt', '33333'),
    ]

    setup_test_data(files_to_create)

    # Select the original files
    preferred_directories = [base_dir + '/preferred_directory']
    files = [file_path for file_path, _ in files_to_create]
    original, duplicates = select_original(files, preferred_source_directories=preferred_directories)

    # Check if the original file is correctly selected from the preferred directory
    assert original.startswith(base_dir + '/preferred_directory/')
    assert len(duplicates) == len(files)
    assert original not in duplicates


def test_create_db_and_table(setup_environment, tmpdir):
    db_path = tmpdir.join("test_file_data.db")
    os.environ['DB_NAME'] = str(db_path)
    create_db_and_table()

    # Check if the database file was created
    assert os.path.isfile(db_path)

    # Connect to the database and check if the table and index exist
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Check if 'files' table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
        table_exists = cursor.fetchone()
        assert table_exists is not None, "The 'files' table was not created."

        # Check if 'idx_hash' index exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_hash'")
        index_exists = cursor.fetchone()
        assert index_exists is not None, "The index on 'hash' was not created."


def test_get_duplicates(setup_environment, tmpdir):
    """
    This test ensures that the get_duplicates function correctly identifies duplicate files
    and organizes them by hash.
    """
    # Set up a temporary environment for the test
    db_path = tmpdir.join("test_file_data.db")
    os.environ['DB_NAME'] = str(db_path)

    #create required tables
    create_db_and_table()

    files_to_create = [

        # Creating files in preferred directory
        (base_dir + '/preferred_directory/file1.txt', '11111'),
        (base_dir + '/preferred_directory/file2.txt', '22222'),

        # Creating duplicates in another directory
        (base_dir + '/another_directory/file3.txt', '11111'),
        (base_dir + '/another_directory/file4.txt', '22222'),
        (base_dir + '/another_directory/file5.txt', '11111'),
        (base_dir + '/another_directory/file6.txt', '22222'),
    ]

    setup_test_data(files_to_create)

    # Process files
    main(base_dir, skip_existing=False, num_threads=2)

    # Call the get_duplicates function
    duplicates = get_duplicates(['another_directory/'])

    # Check the output
    assert len(duplicates) == 2, "The function should return two groups of duplicates."

    # Check the first duplicate group
    assert len(duplicates[0]['duplicates']) == 2
    assert f'{base_dir}/another_directory/file_3' not in duplicates[0]['duplicates']

    # Check the second duplicate group
    assert len(duplicates[0]['duplicates']) == 2
    assert f'{base_dir}/another_directory/file_4' not in duplicates[0]['duplicates']


def test_close_db_connection(tmpdir):

    """
        This test ensures that DB connection is closed and close-db-connection() is working fine
    """
    # Set up a temporary environment for the test
    db_path = tmpdir.join("test_file_data.db")
    os.environ['DB_NAME'] = str(db_path)

    # create required tables
    create_db_and_table()

    conn = sqlite3.connect(db_path)
    close_db_connection(conn)
    assert check_db_connection(conn) == False, "DB connection is not closed properly"


def test_insert_batch_data(setup_environment):

    """
    This test ensures that insert_data_batch() function adds up data correctly in the database
    """

    data_to_insert = [
        (fake.hex_string(), fake.file_path(), fake.random_int(min=1, max=1000), fake.date_time()) for _ in range(5)
    ]

    create_db_and_table()  # Ensure the database and table are set up

    insert_data_batch(data_to_insert)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch all rows from the files table
    cursor.execute('SELECT * FROM files')
    rows = cursor.fetchall()

    fetched_data = [(row[1], row[2], row[3], row[4]) for row in rows]

    # Check that we inserted 5 records
    assert len(rows) == 5
    fetched_data.sort()
    data_to_insert_sorted = [(data[0], data[1], data[2], str(data[3])) for data in data_to_insert]
    data_to_insert_sorted.sort()
    assert set(fetched_data) == set(data_to_insert_sorted)


def test_walk_directory(tmp_path):

    """
    This test ensures that walk_directory function walks the directory and yields the file paths
    properly.
    """

    # Create a temporary directory structure
    dir_structure = {
        'dir1': ['file1.txt', 'file2.txt'],
        'dir2': ['file3.txt'],
        'dir3': ['file4.txt', 'file5.txt'],
    }

    # Create the directory structure
    for dir_name, files in dir_structure.items():
        dir_path = tmp_path / dir_name
        dir_path.mkdir()
        for file_name in files:
            (dir_path / file_name).write_text('Sample content')

    # Collect the file paths yielded by the generator
    yielded_files = list(walk_directory(str(tmp_path)))

    # Create a list of expected file paths
    expected_files = [
        str(tmp_path / 'dir1' / 'file1.txt'),
        str(tmp_path / 'dir1' / 'file2.txt'),
        str(tmp_path / 'dir2' / 'file3.txt'),
        str(tmp_path / 'dir3' / 'file4.txt'),
        str(tmp_path / 'dir3' / 'file5.txt'),
    ]

    # Sort both lists to ensure order doesn't affect the comparison
    yielded_files.sort()
    expected_files.sort()

    # Verify that the yielded file paths match the expected paths
    assert yielded_files == expected_files


def test_load_existing_path(setup_environment):

    """
    This test ensures that load_existing_path() function loads data correctly from the database and
    returns for a quick lookup
    """

    data_to_insert = [
        (fake.hex_string(), fake.file_path(), fake.random_int(min=1, max=1000), fake.date_time()) for _ in range(5)
    ]

    create_db_and_table()

    insert_data_batch(data_to_insert)

    existing_paths = load_existing_paths()

    expected_paths = [p[1] for p in data_to_insert]

    assert set(expected_paths) == set(existing_paths)
