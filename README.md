# Databricks UC Raw to Silver Streaming Pipeline

A Databricks Unity Catalog streaming pipeline that processes sensor data from raw/bronze layer to silver layer with data quality checks.

## Project Structure

```
databricks-uc-raw-to-silver-streaming/
├─ src/
│  └─ raw_to_silver_stream.py       # Main streaming pipeline code
├─ resources/
│  └─ job_raw_to_silver.yml         # Databricks job configuration
├─ databricks.yml                   # Databricks Asset Bundle config
├─ .github/
│  └─ workflows/
│     └─ deploy.yml                 # CI/CD pipeline
└─ README.md
```

## Features

- **Streaming Processing**: Real-time data processing using Spark Structured Streaming
- **Data Quality Checks**: Validates CO2, CH4, temperature, and humidity readings
- **Unity Catalog Integration**: Uses Databricks Unity Catalog for data governance
- **Delta Lake Format**: Leverages Delta Lake for ACID transactions
- **Partitioning**: Data partitioned by year, month, day for efficient queries
- **CI/CD Ready**: GitHub Actions workflow for automated deployment

## Data Schema

### Input (Bronze/Raw Layer)
- `timestamp_utc`: Timestamp of the reading
- `device_id`: Sensor device identifier
- `site_id`: Site location identifier
- `latitude`, `longitude`: Geographic coordinates
- `co2_ppm`: CO2 concentration in parts per million
- `ch4_ppm`: Methane concentration in parts per million
- `temperature_c`: Temperature in Celsius
- `humidity_pct`: Humidity percentage
- `signal_rssi_dbm`: Signal strength

### Output (Silver Layer)
All input fields plus:
- `is_valid_co2`, `is_valid_ch4`, `is_valid_temperature`, `is_valid_humidity`: Quality flags
- `is_valid_record`: Overall record validity flag
- `year`, `month`, `day`, `hour`: Partitioning columns
- `processed_timestamp`: Processing time
- `processing_layer`: Layer identifier

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Catalog and Schemas** created in Unity Catalog:
   - Catalog: `your_catalog_name`
   - Schema 1: `raw` (for bronze layer)
   - Schema 2: `silver` (for silver layer)
3. **Raw data** loaded into `{catalog}.raw.sensor_data_raw` table
4. **Databricks CLI** installed locally

## Setup Instructions

### 1. Update Configuration

Edit the following files with your specific values:

**src/raw_to_silver_stream.py:**
```python
CATALOG_NAME = "your_catalog_name"  # Your catalog name
RAW_SCHEMA = "raw"                   # Your raw schema
SILVER_SCHEMA = "silver"             # Your silver schema
```

**databricks.yml:**
```yaml
workspace:
  host: "https://your-workspace.cloud.databricks.com"
```

**resources/job_raw_to_silver.yml:**
```yaml
email_notifications:
  on_failure:
    - "your_email@example.com"
```

### 2. Load Raw Data

Upload your CSV data to the bronze/raw table:

```python
# Run this in a Databricks notebook
df = spark.read.csv("/path/to/Rawdata.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("your_catalog.raw.sensor_data_raw")
```

### 3. Deploy Using Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Validate the bundle
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev

# Run the job
databricks bundle run -t dev raw_to_silver_streaming_job
```

### 4. Deploy Using GitHub Actions

1. Add GitHub Secrets:
   - `DATABRICKS_HOST`: Your workspace URL
   - `DATABRICKS_TOKEN`: Your personal access token

2. Push to GitHub:
```bash
git add .
git commit -m "Initial commit"
git push origin develop  # Deploys to dev
git push origin main     # Deploys to prod
```

## Running the Pipeline

### Option 1: Using Databricks Bundle CLI
```bash
databricks bundle run -t dev raw_to_silver_streaming_job
```

### Option 2: Using Databricks Workspace UI
1. Navigate to **Workflows** in your Databricks workspace
2. Find the job: `[DEV] Raw to Silver Streaming Pipeline`
3. Click **Run now**

### Option 3: Direct Python Execution
```bash
# In Databricks notebook or cluster
%run ./src/raw_to_silver_stream.py
```

## Monitoring

### Check Stream Status
```python
# In Databricks notebook
spark.streams.active  # List active streams
```

### Query Silver Data
```sql
SELECT * FROM your_catalog.silver.sensor_data_silver 
WHERE is_valid_record = true
ORDER BY timestamp_utc DESC
LIMIT 100;
```

### Data Quality Metrics
```sql
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN is_valid_record THEN 1 ELSE 0 END) as valid_records,
  SUM(CASE WHEN NOT is_valid_co2 THEN 1 ELSE 0 END) as invalid_co2,
  SUM(CASE WHEN NOT is_valid_ch4 THEN 1 ELSE 0 END) as invalid_ch4,
  SUM(CASE WHEN NOT is_valid_temperature THEN 1 ELSE 0 END) as invalid_temp,
  SUM(CASE WHEN NOT is_valid_humidity THEN 1 ELSE 0 END) as invalid_humidity
FROM your_catalog.silver.sensor_data_silver;
```

## Troubleshooting

### Stream not starting
- Check checkpoint location permissions
- Verify catalog and schema names
- Ensure raw table exists and has data

### Data quality issues
- Review validation rules in `apply_data_quality_rules()`
- Check source data ranges
- Adjust thresholds as needed

### Performance optimization
- Adjust trigger interval in `write_stream()` (default: 10 seconds)
- Increase cluster size in `job_raw_to_silver.yml`
- Enable auto-scaling

## Environment Configuration

| Environment | Branch   | Catalog         | Deploy Target |
|-------------|----------|-----------------|---------------|
| Development | develop  | dev_catalog     | dev           |
| Staging     | PR→main  | staging_catalog | staging       |
| Production  | main     | prod_catalog    | prod          |

## Additional Resources

- [Databricks Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Lake](https://docs.delta.io/)

## License

MIT License
