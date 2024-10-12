# Duplicate File Finder Script

This script is designed to help you find and manage duplicate files across multiple directories. It allows you to process directories, identify duplicates, and remove unwanted copies while preserving preferred versions of files based on your criteria.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Available Commands](#available-commands)
  - [Typical Workflow](#typical-workflow)
    - [1. Clean the Database](#1-clean-the-database)
    - [2. Process the Main Directory](#2-process-the-main-directory)
    - [3. Remove Duplicates Within the Main Directory](#3-remove-duplicates-within-the-main-directory)
    - [4. Add Additional Directories](#4-add-additional-directories)
    - [5. Remove Duplicates Across Directories](#5-remove-duplicates-across-directories)
    - [6. Review and Rescan as Needed](#6-review-and-rescan-as-needed)
  - [Command Examples](#command-examples)
- [Options and Arguments](#options-and-arguments)
  - [Processing Files (`process`)](#processing-files-process)
  - [Listing Duplicates (`list-duplicates`)](#listing-duplicates-list-duplicates)
  - [Deleting Duplicates (`delete-duplicates`)](#deleting-duplicates-delete-duplicates)
  - [Rescanning Duplicates (`rescan-duplicates`)](#rescanning-duplicates-rescan-duplicates)
  - [Cleaning the Database (`clean-db`)](#cleaning-the-database-clean-db)
- [Recommendations](#recommendations)
- [License](#license)

---

## Features

- Scan directories to build a database of files with their hashes and metadata.
- Identify duplicate files based on content (file hashes).
- Prefer certain directories when keeping originals.
- Remove duplicates while preserving the preferred original files.
- Process files using multiple threads for improved performance.
- Log detailed output to a file for debugging and record-keeping.
- Clean up the database by removing entries for files that no longer exist.
- Rescan duplicates to update file information.

## Requirements

- Python 3.6 or higher
- The following Python packages:
  - `xxhash`
  - `tqdm`

Install the required packages using:

```bash
pip install xxhash tqdm
```

## Installation

Clone the repository or download the script `finddupes.py` to your local machine.

---

## Usage

The script provides several commands to process directories, find duplicates, list them, and delete unwanted copies.

### Available Commands

- `process`: Process files in a directory to build or update the database.
- `list-duplicates`: List duplicates excluding the original files.
- `delete-duplicates`: Delete duplicate files based on specified criteria.
- `rescan-duplicates`: Rescan duplicate files to update their information.
- `clean-db`: Remove entries from the database for files that no longer exist on disk.
- `list-duplicates-csv`: List duplicates and originals in CSV format.

### Typical Workflow

The typical workflow involves the following steps:

#### **1. Clean the Database**

Start by cleaning the database to remove any entries for files that no longer exist on disk.

```bash
python finddupes.py clean-db
```

This ensures that the database reflects the current state of your files.

#### **2. Process the Main Directory**

Choose a main directory that you want to prefer when keeping original files. Process this directory first.

```bash
python finddupes.py process /path/to/main_directory --threads 4 --log-file finddupes.log
```

- `--threads 4` specifies using 4 threads for processing. By default, it uses the number of CPU cores.
- `--log-file finddupes.log` logs detailed output to the specified file.

#### **3. Remove Duplicates Within the Main Directory**

Remove duplicates within the main directory based on folder depth or other criteria, ensuring that the best version of files is kept.

First, list duplicates within the main directory:

```bash
python finddupes.py list-duplicates --within-directory /path/to/main_directory > duplicates.txt
```

Review `duplicates.txt` to see which files are considered duplicates.

Then, delete duplicates within the main directory:

```bash
python finddupes.py delete-duplicates --within-directory /path/to/main_directory --simulate-delete
```

- Use `--simulate-delete` to perform a dry run without actually deleting files.
- Remove `--simulate-delete` to perform the actual deletion.

#### **4. Add Additional Directories**

Process additional directories where duplicates might exist.

```bash
python finddupes.py process /path/to/other_directory --skip-existing --threads 4 --log-file finddupes.log
```

- `--skip-existing` ensures that only new files are processed, avoiding re-hashing files already in the database.

#### **5. Remove Duplicates Across Directories**

Now, remove duplicates across directories, preferring the original files in the main directory.

```bash
python finddupes.py delete-duplicates --prefer-directory /path/to/main_directory --simulate-delete
```

- `--prefer-directory /path/to/main_directory` sets the main directory as the preferred source for originals.
- Optionally, you can specify multiple preferred directories, ordered by preference:

  ```bash
  python finddupes.py delete-duplicates --prefer-directory /path/to/main_directory,/path/to/another_directory
  ```

- Review the output to ensure that the correct files will be deleted.
- Remove `--simulate-delete` to delete the duplicates.

#### **6. Review and Rescan as Needed**

After deleting duplicates, you can rescan the files to update the database.

```bash
python finddupes.py rescan-duplicates
```

Or, process the directories again with `--skip-existing` to update any new changes.

---

### Command Examples

**Process a directory:**

```bash
python finddupes.py process /path/to/directory --threads 4 --log-file finddupes.log
```

**List duplicates excluding originals:**

```bash
python finddupes.py list-duplicates --prefer-directory /path/to/main_directory --output duplicates.txt
```

**Delete duplicates, preferring the main directory:**

```bash
python finddupes.py delete-duplicates --prefer-directory /path/to/main_directory --simulate-delete
```

**Clean the database:**

```bash
python finddupes.py clean-db
```

---

## Options and Arguments

### Processing Files (`process`)

```bash
python finddupes.py process <directory> [options]
```

**Options:**

- `<directory>`: The directory to process.
- `--skip-existing`: Skip processing files that are already in the database.
- `--threads N`: Number of threads for hashing (default: number of CPU cores).
- `--log-file FILE`: Path to log file for detailed output.

### Listing Duplicates (`list-duplicates`)

```bash
python finddupes.py list-duplicates [options]
```

**Options:**

- `-o, --output FILE`: Output file to write the list to. If not specified, prints to console.
- `--prefer-directory DIRS`: Preferred source directories for selecting original files (comma-separated if multiple). The first directory has the highest preference.
- `--within-directory DIR`: Only examine duplicates within this directory.

### Deleting Duplicates (`delete-duplicates`)

```bash
python finddupes.py delete-duplicates [options]
```

**Options:**

- `--prefer-directory DIRS`: Preferred source directories for selecting original files (comma-separated if multiple).
- `-o, --output FILE`: Output CSV file to log the deleted files.
- `--overwrite`: Overwrite the output file if it exists.
- `--append`: Append to the output file if it exists.
- `--simulate-delete`: Simulate deletion without actually deleting files.
- `--within-directory DIR`: Only delete duplicates within this directory.

### Rescanning Duplicates (`rescan-duplicates`)

```bash
python finddupes.py rescan-duplicates
```

Rescans duplicates to update their hashes and metadata in the database.

### Cleaning the Database (`clean-db`)

```bash
python finddupes.py clean-db
```

Removes entries from the database for files that no longer exist on disk.

---

## Recommendations

- **Clean the Database Regularly**: Use `clean-db` to ensure the database reflects the current state of your files, especially after moving or deleting files outside of this script.
- **Process Directories in Order**: Start with your main directory, then process additional directories, preferring the main directory when removing duplicates.
- **Use `--skip-existing`**: When adding new directories, use `--skip-existing` to avoid reprocessing files already in the database.
- **Simulate Deletions First**: Always use `--simulate-delete` initially to ensure that the correct files will be deleted before performing actual deletions.
- **Review Outputs**: Check the outputs of `list-duplicates` and deletion simulations to verify that the script is identifying duplicates as expected.
- **Backup Important Data**: Before deleting files, consider backing up important data to prevent accidental loss.
- **Log Outputs**: Utilize the `--log-file` option to keep detailed logs for debugging and record-keeping.

---

## License

This script was written primarily by AI, and therefore has no license or copyright.

---

*Note: Replace `/path/to/main_directory` and other placeholder paths with the actual paths on your system.*

This readme provides instructions and examples to help you effectively use the Duplicate File Finder script. By following the typical workflow and utilizing the options available, you can manage duplicate files across directories while preserving preferred versions and ensuring data integrity.