# PyDupes

PyDupes is a Python-based tool designed to identify and manage duplicate files within a specified directory structure. PyDupes uses the MD5 hash of each file to compare files and identify duplicates.

All results are stored in a sqlite database,

This is a quick and dirty tool that we wrote to find duplicate files on client hard drives without installing a GUI. You can copy
the database file to your local machine to view the results. The included web GUI works and allows you to download the duplicated
files list as CSV.

## Features

- Recursive directory scanning
- MD5 hash-based file comparison
- Support for various file types

## Installation

To install PyDupes, clone this repository and install the required dependencies:
 - flask

That's it! You're ready to start using PyDupes.