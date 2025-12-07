# Big Data Structure

## Description

As part of our Big Data Structure course, this project allows you to:

1. **Create Python objects** automatically from a JSON Schema
2. **Calculate sizes** of documents, collections, and databases
3. **Estimate execution costs** for operators like Filter and Join
4. **Simulate distributed (sharded) environments** to compare performance
5. **Trace and explain computations** through detailed cost and size analysis

This is only the first part of the project. This later aims to package more features, let see what will happen in the next courses...

## Project Structure

```
delivery_1/
├── src/
│   ├── main.py                          # Entry point with CLI
│   ├── services/
│   │   ├── __init__.py
│   │   ├── delivery_1_service.py        # Main service
│   │   └── delivery_2_service.py        # Main service
│   └── utils/
│       ├── __init__.py
│       ├── load_file.py                 # Factory to load files
│       ├── schema_builder.py            # Builds dataclasses from JSON Schema
│       └── size_computer.py             # Computes sizes
│   ├── operators/
│   │   ├── __init__.py
│   │   ├── filter_operator.py           # Filter operator (non-sharded)
│   │   ├── filter_sharded_operator.py   # Filter operator (sharded)
│   │   ├── join_nested_operator.py      # Nested loop join (non-sharded)
│   │   └── join_sharded_operator.py     # Nested loop join (sharded)
│   ├── queries/
│   │   ├── __init__.py
│   │   └── quick_cases.py               # Predefined scenarios (Filter, Join, etc.)
├── basic_schema.json                    # Database JSON Schema
├── basic_statistic.json                 # Database statistics
├── key_sizes.json                       # Type size references
└── README.md                            # This file
```

## Installation

### Option 1: Docker (Recommended)

**Prerequisites:** Docker installed on your system

```bash
cd delivery_1

# Build and run with the helper script
./run.sh

# Or manually build the image
docker build -t big-data-structure:latest .

# Run with docker
docker run --rm big-data-structure:latest --delivery_1

# Or use docker-compose
docker-compose up
```

### Option 2: Local Python

**Prerequisites:** Python 3.11+ installed

```bash
cd delivery_1
cd delivery_2
# No external dependencies required, uses only Python 3 stdlib
```

## Usage

### Using Docker

```bash
# Run with default files
docker run --rm big-data-structure:latest --delivery_1

# Show help
docker run --rm big-data-structure:latest --help

# Use custom files (mount current directory)
docker run --rm -v $(pwd):/app big-data-structure:latest --delivery_1 \
  --schema custom_schema.json \
  --stats custom_stats.json \
  --sizes custom_sizes.json

# Using docker-compose
docker-compose up
```

### Using Local Python

### Basic command

```bash
python3 src/main.py --delivery_1
python3 src/main.py --delivery_2
```

### Show help

```bash
python3 src/main.py --help
```

### Use custom files

```bash
python3 src/main.py --delivery_1 \
  --schema custom_schema.json \
  --stats custom_stats.json \
  --sizes custom_sizes.json
```

### Show version

```bash
python3 src/main.py --version
```

## Configuration Files

### 1. JSON Schema (`basic_schema.json`)

Defines your database structure according to JSON Schema standard.

**Example**:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "E-commerce Database",
  "type": "object",
  "properties": {
    "Product": {
      "type": "object",
      "properties": {
        "id": { "type": "integer" },
        "name": { "type": "string" },
        "price": { "type": "number" },
        "tags": {
          "type": "array",
          "items": { "type": "string" }
        }
      },
      "required": ["id", "name", "price"]
    }
  }
}
```

### 2. Statistics (`basic_statistic.json`)

Contains actual statistics of your database.

**Supported fields**:
- `document_count`: Number of documents in the collection
- `field_specifics`: Statistics per field
  - `avg_length`: Average length for strings
  - `avg_items`: Average number of elements for arrays
  - `null_percentage`: Percentage of null values (0-100)
  - `occurrence_multiplier`: Multiplier for repeated fields

**Example**:
```json
{
  "database": {
    "name": "ecommerce_db",
    "description": "E-commerce database"
  },
  "collections": {
    "Product": {
      "document_count": 100000,
      "field_specifics": {
        "name": {
          "avg_length": 60
        },
        "description": {
          "avg_length": 200,
          "null_percentage": 10
        },
        "tags": {
          "avg_items": 5
        }
      }
    }
  }
}
```

### 3. Type Sizes (`key_sizes.json`)

Defines base sizes for each data type.

**Example**:
```json
{
  "key_value_pair": 12,
  "number": 8,
  "string": 80,
  "date": 20,
  "long_string": 200
}
```

## Program Output

The program displays:

  ### Delivery 1:

1. **File loading**: Confirmation of loading the 3 files
2. **Collection building**: List of created dataclasses with field count
3. **Size computation**: Database overview
4. **Collection details**: 
   - Number of documents
   - Average document size
   - Total size
   - Percentage of total
   - Visual bar
5. **Summary**: Top 3 largest collections

**Output example**:
```
======================================================================
COMPUTING DATABASE SIZES
======================================================================

   Database: ecommerce_db
   Total Collections: 7
   Total Documents: 4,030,130,200
   Total Size: 1.12 TB
              (1148.54 GB)

======================================================================
COLLECTION SIZE BREAKDOWN
======================================================================

    OrderLine
      Documents: 4,000,000,000
      Avg Doc Size: 307 bytes
      Total Size: 1.12 TB
      Percentage: 99.58%
      [█████████████████████████████████████████████████]
```
  ### Delivery 2:
  1. **File loading**
  2. **Operator & params** (Filter/Join, mode, collection, selectivity)
  3. **Size estimation** (N_out, avg_doc_size, output_size)
  4. **Cost breakdown** (I/O, CPU, Network, Total)

**Output example**:
'''
======================================================================
REQ — Filter 'range/BETWEEN' (with sharding, aligned)
======================================================================
Params: {'operator': 'Filter (with sharding)', 'collection': 'Stock', 'output_keys': ['quantity', 'location'], 'filter_key': 'IDW', 'selectivity': 0.15, 'sharding_info': {'nb_shards': 4, 'shard_key': 'IDW', 'distribution': 'uniform'}}
N_out: 3000000
avg_doc_size: 48.00 B
output_size: 144000000 bytes
Costs: io=2343.75 cpu=20000.0 net=360.00000000000006 total=22703.75
'''
    

## Exit Codes

- `0`: Success
- `1`: Error (missing files, execution error, etc.)

## Support

For any questions or issues, consult the documentation in source files or contact us.

## Version

Current version: **1.0.0**
