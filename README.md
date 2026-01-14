# Databricks Streaming Pipeline: Raw to Silver

A real-time streaming pipeline that monitors IoT sensor data in a raw table and automatically processes it to a silver layer with data quality checks.

## 🎯 What This Does

```
┌─────────────────────────────────────────────────────────┐
│  1. Insert data manually into raw table                │
│     carbon_dev.raw.iot_sensor_landing_layer             │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  2. Databricks Streaming Job continuously monitors      │
│     raw_to_silver_stream.py (runs every 10 seconds)     │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  3. Applies data quality checks:                        │
│     • CO2 validation (0-5000 ppm)                       │
│     • CH4 validation (0-100 ppm)                        │
│     • Temperature validation (-50 to 100°C)             │
│     • Humidity validation (0-100%)                      │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  4. Writes to silver table (auto-created)               │
│     carbon_dev.silver.iot_sensor_silver                 │
│     Partitioned by: year, month, day                    │
└─────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
DBX-Stream/
├── src/
│   └── raw_to_silver_stream.py    ← Streaming code (runs in Databricks)
├── .github/
│   └── workflows/
│       └── deploy.yml             ← Triggers job on git push
├── databricks.yml                 ← Configuration reference
├── resources/
│   └── job_raw_to_silver.yml      ← Job settings reference
└── README.md
```

## 🔧 Configuration

### Databricks Setup
- **Workspace**: your workspace name
- **Catalog**: your catalog name
- **Raw Table**: your raw table name
- **Silver Table**: autocreated as part of logic

### GitHub Secrets (Already configured)
- `DATABRICKS_HOST` - Your workspace URL
- `DATABRICKS_TOKEN` - Personal access token
- `DATABRICKS_JOB_ID` - Your job ID

## 📊 Data Flow

### Input Schema (Raw Table)
| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | String | Timestamp of reading |
| device_id | String | Sensor device ID |
| site_id | String | Site location |
| latitude | Double | GPS latitude |
| longitude | Double | GPS longitude |
| co2_ppm | Double | CO2 concentration |
| ch4_ppm | Double | Methane concentration |
| temperature_c | Double | Temperature in Celsius |
| humidity_pct | Double | Humidity percentage |
| signal_rssi_dbm | Integer | Signal strength |

### Output Schema (Silver Table)
All raw columns **plus**:
- `is_valid_co2`, `is_valid_ch4`, `is_valid_temperature`, `is_valid_humidity` - Quality flags
- `is_valid_record` - Overall validity
- `year`, `month`, `day`, `hour` - Partition columns
- `processed_timestamp` - When record was processed
- `processing_layer` - Always "silver"

## 🚀 How to Use

### Step 1: Insert Data into Raw Table

### Step 2: Streaming Job Automatically Processes
Your Databricks job monitors the raw table and processes new records within 10 seconds.

### Step 3: Query Silver Table
```sql
-- Check processed data
SELECT * FROM carbon_dev.silver.iot_sensor_silver
WHERE is_valid_record = true
ORDER BY timestamp_utc DESC
LIMIT 10;

-- Data quality summary
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN is_valid_record THEN 1 ELSE 0 END) as valid_records,
  SUM(CASE WHEN NOT is_valid_co2 THEN 1 ELSE 0 END) as invalid_co2,
  SUM(CASE WHEN NOT is_valid_ch4 THEN 1 ELSE 0 END) as invalid_ch4
FROM carbon_dev.silver.iot_sensor_silver;
```

## 🔄 GitHub Workflow

When you push code changes:
```bash
git add .
git commit -m "Update streaming logic"
git push
```

GitHub Actions automatically:
1. Triggers your Databricks job via API
2. Job restarts with updated code
3. Continues monitoring raw table

## 📝 Files Explained

| File | Purpose | Used By |
|------|---------|---------|
| `src/raw_to_silver_stream.py` | Streaming code | ✅ Databricks job |
| `.github/workflows/deploy.yml` | CI/CD trigger | ✅ GitHub Actions |
| `databricks.yml` | Config reference | ❌ Documentation only |
| `resources/job_raw_to_silver.yml` | Job settings reference | ❌ Documentation only |

## 🧪 Testing

### Test Real-Time Streaming
```sql
-- Insert a test record
INSERT INTO carbon_dev.raw.iot_sensor_landing_layer VALUES (
  '2026-01-14T15:00:00Z', 'DEV-TEST', 'TEST_SITE',
  40.7128, -74.0060, 700.0, 4.5, 20.0, 65.0, -70
);

-- Wait ~10 seconds, then check silver table
SELECT * FROM carbon_dev.silver.iot_sensor_silver 
WHERE device_id = 'DEV-TEST';
```

### Monitor Streaming
```python
# In Databricks notebook
spark.streams.active  # Shows active streaming queries
```

## ⚙️ Data Quality Rules

- **CO2**: 0-5000 ppm (invalid if outside range)
- **CH4**: 0-100 ppm (invalid if outside range)
- **Temperature**: -50 to 100°C (invalid if outside range)
- **Humidity**: 0-100% (invalid if outside range)
- **Record**: Valid only if ALL metrics are valid AND timestamp/device_id exist

## 🛠️ Troubleshooting

**Stream not running?**
- Check your Databricks job status in Workflows
- Verify raw table exists: `SELECT COUNT(*) FROM carbon_dev.raw.iot_sensor_landing_layer`

**Silver table not created?**
- Run the job once - it auto-creates the table on first run

**GitHub Actions failing?**
- Verify all 3 secrets are set: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_JOB_ID`

## 📚 Key Features

✅ **Real-time**: Processes new records within 10 seconds  
✅ **Auto-create**: Silver table created automatically  
✅ **Data Quality**: Validates all sensor readings  
✅ **Partitioned**: Optimized for date-based queries  
✅ **Checkpointed**: Exactly-once processing guarantee  
✅ **CI/CD**: Automated deployment on git push  
