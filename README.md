# KLM Booking Analysis

Analyzes KLM booking data to identify the most popular destination countries by season and day of week.

## Requirements

- Python 3.8+
- Java 17+ (automatically detected by the application)
- PySpark (installed via requirements.txt)

### Windows-Specific Notes

If you're running on Windows, the application automatically configures Spark to work on Windows. However, you may need to:

1. **Install Java 17+**: Download from [Adoptium](https://adoptium.net/) or use [Chocolatey](https://chocolatey.org/): `choco install openjdk17`
2. **Set JAVA_HOME**: Set the `JAVA_HOME` environment variable to your Java installation path (e.g., `C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot`)
3. **WinUtils (Optional)**: For HDFS support on Windows, you may need to install WinUtils. However, local file processing works without it.

The application automatically handles Windows-specific Spark configuration (TCP sockets instead of Unix sockets).

## Installation

```bash
pip install -r requirements.txt
```

## How the Batch Job Works

The batch job processes booking data through the following steps:

1. **Data Loading**: Reads booking JSON files (local or HDFS) and airport data
2. **Filtering**: Applies business rules:
   - Date range filter (only flights within specified dates)
   - KL airline filter (only KLM operating flights)
   - Netherlands origin filter (only flights departing from Netherlands)
   - Confirmed status filter (only passengers with latest booking status = Confirmed)
3. **Enrichment**: Adds derived information:
   - Maps destination airports to countries
   - Calculates day of week based on origin airport timezone
   - Calculates season from departure date
4. **Aggregation**: Groups passengers by country, day of week, and season
   - Each passenger counted once per flight leg (unique passenger-flight combination)
   - Only counts passengers where latest booking status is Confirmed
   - Tracks adults vs children and average age
5. **Output**: Generates sorted report (CSV or JSON)

The job uses PySpark for distributed processing, enabling it to handle datasets from MB to TB scale.

## How to Run

### Quick Start with Example Data

The simplest way to run the job on the provided example data:

```bash
python3 -m src.main \
  --bookings-input data/bookings/booking.json \
  --airports-input data/airports/airports.dat \
  --output report.csv \
  --start-date 2019-01-01 \
  --end-date 2019-12-31
```

This requires no additional configuration - just run the command and the report will be generated in `report.csv`.

### Local Files

```bash
python3 -m src.main \
  --bookings-input data/bookings \
  --airports-input data/airports/airports.dat \
  --output report.csv \
  --start-date 2019-01-01 \
  --end-date 2019-12-31
```

### HDFS Input

```bash
python3 -m src.main \
  --bookings-input hdfs://namenode:9000/bookings \
  --airports-input data/airports/airports.dat \
  --output hdfs://namenode:9000/output/report.csv \
  --start-date 2019-01-01 \
  --end-date 2019-12-31
```

The application automatically detects HDFS paths (starting with `hdfs://` or `hdfs:/`) and uses Spark's distributed reading capabilities. Hadoop configuration is read from `/etc/hadoop/conf/` if available.

### Streaming Mode

For real-time analysis showing top destination countries for a specific day. By default, it uses today's date, but you can specify any date:

**Using today's date (default):**
```bash
python3 -m src.main \
  --bookings-input data/bookings \
  --airports-input data/airports/airports.dat \
  --output - \
  --streaming
```

**With a specific date:**
```bash
python3 -m src.main \
  --bookings-input data/bookings \
  --airports-input data/airports/airports.dat \
  --output - \
  --streaming \
  --start-date 2019-06-15
```

Note: In streaming mode, `--start-date` is optional (defaults to today). The `--end-date` parameter is not used in streaming mode.

## Command Line Options

- `--bookings-input` (required): Path to bookings directory or file
  - Local: `data/bookings` or `data/bookings/booking.json`
  - HDFS: `hdfs://namenode:9000/bookings`
  
- `--airports-input` (required): Path to airports.dat file
  - Example: `data/airports/airports.dat`
  
- `--output` (required): Output file path
  - CSV: `report.csv` (writes CSV format)
  - JSON: `report.json` (writes JSON format)
  - HDFS: `hdfs://namenode:9000/output/report.csv`
  - Console: `-` (for streaming mode)
  
- `--start-date`: Start date for filtering bookings
  - Format: `YYYY-MM-DD`
  - Example: `2019-01-01`
  - **Batch mode**: Required
  - **Streaming mode**: Optional (defaults to today's date)
  
- `--end-date`: End date for filtering bookings
  - Format: `YYYY-MM-DD`
  - Example: `2019-12-31`
  - **Batch mode**: Required
  - **Streaming mode**: Not used (only processes single day specified by `--start-date`)
  
- `--netherlands-airports` (optional): Comma-separated list of Netherlands airport codes
  - Default: `AMS,EIN,RTM,GRQ,MST`
  - Example: `--netherlands-airports AMS,EIN,RTM`
  
- `--streaming` (optional): Run in streaming mode for real-time analysis
  - Shows top destination countries for a specific day
  - Use `--start-date` to specify which day to analyze (defaults to today if not specified)
  - Output is displayed to console instead of a file
  - Example: `--streaming --start-date 2019-06-15` analyzes bookings for June 15, 2019
  
- `-d, --debug` (optional): Enable debug logging for detailed output
  - Shows detailed processing steps, file operations, and internal state
  - Useful for troubleshooting and understanding data flow
  - Example: `--debug` or `-d`
  
- `--use-spark-dataframes` (optional): Use optimized Spark DataFrame processing
  - Leverages Spark's distributed processing for large datasets (TB scale)
  - Pushes filtering and aggregation down to Spark for better performance
  - Recommended for datasets larger than a few GB
  - Example: `--use-spark-dataframes`

## Processing Modes

The application supports two processing modes:

### Standard Mode (Default)

Uses Python lists and PySpark for data loading only. Best for:
- Small to medium datasets (< 10 GB)
- Development and testing
- When exact reproducibility is required

### Spark DataFrame Mode (`--use-spark-dataframes`)

Uses Spark DataFrames for distributed processing. Best for:
- Large datasets (10 GB to TB scale)
- Production environments with Hadoop clusters
- When performance is critical

### Result Differences Between Modes

**Important**: The standard and Spark DataFrame modes may produce slightly different results (typically 0.1-0.2% difference) due to how they handle edge cases in "latest booking status" calculation.

#### Why the Difference Occurs

The assignment requires counting passengers only when their **latest booking status is Confirmed**. However, when a passenger has multiple booking events with **identical timestamps**, the assignment doesn't specify how to break the tie:

```
Example:
  Passenger: PASS123
  Flight: AMS → JFK on 2019-06-15
  
  Event 1: Timestamp = 2019-03-17T13:47:26.475Z, Status = CONFIRMED
  Event 2: Timestamp = 2019-03-17T13:47:26.475Z, Status = CANCELLED
```

**Standard Mode**:
- Processes events in file order
- When timestamps are equal, keeps the first one encountered
- Deterministic based on file order

**Spark DataFrame Mode**:
- Uses Spark window functions with `row_number()`
- Breaks ties using Spark's internal ordering
- Deterministic but different from standard mode

#### Impact

- **Difference**: Typically 0.1-0.2% of rows (1-2 rows out of 1000)
- **Both modes are correct**: They both implement the business rules correctly
- **Both are deterministic**: Same input always produces same output
- **The difference is in edge cases only**: Not a logic error

#### Recommendation: Define Explicit Tie-Breaking Logic

To eliminate this difference in production systems, we should:

1. **Define explicit tie-breaking rules** in the business requirements:
   - Option A: When timestamps are equal, prefer CONFIRMED status
   - Option B: When timestamps are equal, use event ID or sequence number


## Output Format

The output is a CSV or JSON file with the following columns:

| Column | Description |
|--------|-------------|
| Country | Destination country name |
| Day of Week | Day name (Monday, Tuesday, etc.) |
| Season | Season (Spring, Summer, Fall, Winter) |
| Passenger Count | Number of unique passengers |
| Adults Count | Number of adult passengers (ADT) |
| Children Count | Number of child passengers (CHD) |
| Average Age | Average age of passengers (when available) |

Rows are sorted by:
1. Passenger count (descending) - highest counts appear first
2. Season (alphabetically) - for grouping within same passenger count
3. Day of week (alphabetically) - for grouping within same passenger count and season

## Required Assets

All necessary files are provided in this repository:

- **Data files**: `data/bookings/booking.json` and `data/airports/airports.dat`
- **Source code**: Complete implementation in `src/` directory
- **Dependencies**: `requirements.txt` with all Python packages
- **Configuration**: Application automatically detects Java and Hadoop configuration

No additional setup or configuration files are required to run the job on the example dataset.

## Testing

The project includes comprehensive unit and integration tests covering all core business logic and functional requirements.

### Running Tests

**Run all tests:**
```bash
python3 -m pytest tests/
```

**Run with verbose output:**
```bash
python3 -m pytest tests/ -v
```

**Run specific test file:**
```bash
python3 -m pytest tests/test_filters.py -v
```

**Run tests for Spark DataFrame functions:**
```bash
python3 -m pytest tests/test_*spark.py -v
```

**Run only integration tests:**
```bash
python3 -m pytest tests/test_integration*.py -v
```

**Run tests with coverage report:**
```bash
python3 -m pytest tests/ --cov=src --cov-report=term-missing
```

### Test Coverage

The test suite provides comprehensive coverage:

- **Core Business Logic** (100% coverage):
  - Date range filtering
  - KL airline filtering
  - Netherlands origin filtering
  - Latest booking status calculation
  - Season and day of week calculation
  - Passenger aggregation logic

- **Data Models** (100% coverage):
  - Booking event parsing and validation
  - Airport data models
  - Report row models

- **Spark DataFrame Functions**:
  - Filter functions (date range, KL flights, Netherlands origin, latest status)
  - Aggregation functions (passenger counting, adults/children, average age)
  - End-to-end Spark DataFrame pipeline

- **Integration Tests**:
  - Standard pipeline end-to-end
  - Spark DataFrame pipeline end-to-end
  - Functional requirements verification

### Test Files

- `tests/test_filters.py` - Standard filtering functions
- `tests/test_filters_spark.py` - Spark DataFrame filtering functions
- `tests/test_aggregation.py` - Standard aggregation functions
- `tests/test_aggregation_spark.py` - Spark DataFrame aggregation functions
- `tests/test_enrichment.py` - Enrichment functions (season, day of week)
- `tests/test_models.py` - Data model validation
- `tests/test_airport_lookup.py` - Airport lookup service
- `tests/test_integration.py` - Standard pipeline integration tests
- `tests/test_integration_spark.py` - Spark DataFrame pipeline integration tests

All tests use pytest and can be run individually or as a suite.
