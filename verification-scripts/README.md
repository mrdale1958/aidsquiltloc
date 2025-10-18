# Verification Scripts for Metadata and Artifacts

This project contains two verification scripts designed to ensure the integrity of metadata and artifact files related to the AIDS Memorial Quilt Records.

## Scripts Overview

### 1. `verify_metadata.py`
This script verifies that all JSON files in the directory `D:/LOCData/metadata` are complete and well-formed. It checks for the presence of required fields in each JSON file and ensures that the files can be parsed without errors.

### 2. `verify_artifacts.py`
This script checks that all artifact files indicated by the metadata in the JSON files are present in the specified directory. It reads the metadata from the JSON files and verifies the existence of each artifact file.

## Prerequisites

- Python 3.x
- Required libraries listed in `requirements.txt`

## Installation

1. Clone the repository or download the scripts.
2. Navigate to the project directory.
3. Install the required dependencies:

```
pip install -r requirements.txt
```

## Usage

### Verify Metadata

To run the metadata verification script, execute the following command:

```
python verify_metadata.py
```

### Verify Artifacts

To run the artifact verification script, execute the following command:

```
python verify_artifacts.py
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.