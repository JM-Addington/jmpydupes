import os
import shutil
import xxhash
import time
from pathlib import Path
import pytest

# Set the environment variable for test database before importing finddupes
ORIGINAL_DB_NAME = os.environ.get('DB_NAME', None)
TEST_DB_NAME = 'test_file_data.db'
base_dir = './test'
os.environ['DB_NAME'] = TEST_DB_NAME

from finddupes import (
    processed_data,
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

    main(base_dir)

    # Get absolute paths for preferred directories, in order of preference
    # dir1-no_dupes has highest preference, dir4 is next
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
        (base_dir + '/dir1/file1.txt', '00000'),
        (base_dir + '/dir2/file1.txt', '11111'),
        (base_dir + '/dir3/file1.txt', '11111'),
        (base_dir + '/dir4/file1.txt', '11111'),
    ]

    setup_test_data(files_to_create)

    main(base_dir, skip_existing=False, num_threads=2)

    list_duplicates_csv(
        output_file='duplicates.csv',
        preferred_source_directories=[str(Path(base_dir + '/dir1-no_dupes').resolve())]
    )
    # Check that the CSV file exists and has content
    assert os.path.exists('duplicates.csv'), "CSV output file was not created."
    with open('duplicates.csv', 'r') as csvfile:
        content = csvfile.read()
    print (content)
    assert 'file1.txt' in content, "CSV output does not contain expected data."



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
