"""
Raw to Silver Layer Streaming Pipeline
Reads sensor data from bronze/raw layer and writes to silver layer with data quality transformations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp, 
    year, month, dayofmonth, hour,
    when, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RawToSilverStream:
    """Streaming pipeline from Raw to Silver layer"""
    
    def __init__(self, catalog_name: str, raw_schema: str, silver_schema: str):
        """
        Initialize the streaming pipeline
        
        Args:
            catalog_name: Unity Catalog name
            raw_schema: Schema name for raw/bronze layer
            silver_schema: Schema name for silver layer
        """
        self.catalog_name = catalog_name
        self.raw_schema = raw_schema
        self.silver_schema = silver_schema
        self.spark = SparkSession.builder.appName("RawToSilverStream").getOrCreate()
        
        # Set catalog
        self.spark.sql(f"USE CATALOG {self.catalog_name}")
        
        # Table names
        self.raw_table = f"{self.catalog_name}.{self.raw_schema}.sensor_data_raw"
        self.silver_table = f"{self.catalog_name}.{self.silver_schema}.sensor_data_silver"
        self.checkpoint_path = f"/tmp/checkpoints/{self.silver_schema}/sensor_data_silver"
        
    def define_schema(self):
        """Define the schema for the raw data"""
        return StructType([
            StructField("timestamp_utc", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("site_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("co2_ppm", DoubleType(), True),
            StructField("ch4_ppm", DoubleType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("humidity_pct", DoubleType(), True),
            StructField("signal_rssi_dbm", IntegerType(), True)
        ])
    
    def apply_data_quality_rules(self, df):
        """
        Apply data quality transformations and business rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Applying data quality rules...")
        
        # Convert timestamp string to timestamp type
        df = df.withColumn("timestamp_utc", to_timestamp(col("timestamp_utc")))
        
        # Add data quality flags
        df = df.withColumn(
            "is_valid_co2",
            when((col("co2_ppm") >= 0) & (col("co2_ppm") <= 5000), True).otherwise(False)
        )
        
        df = df.withColumn(
            "is_valid_ch4",
            when((col("ch4_ppm") >= 0) & (col("ch4_ppm") <= 100), True).otherwise(False)
        )
        
        df = df.withColumn(
            "is_valid_temperature",
            when((col("temperature_c") >= -50) & (col("temperature_c") <= 100), True).otherwise(False)
        )
        
        df = df.withColumn(
            "is_valid_humidity",
            when((col("humidity_pct") >= 0) & (col("humidity_pct") <= 100), True).otherwise(False)
        )
        
        # Add overall data quality flag
        df = df.withColumn(
            "is_valid_record",
            when(
                col("is_valid_co2") & 
                col("is_valid_ch4") & 
                col("is_valid_temperature") & 
                col("is_valid_humidity") &
                col("timestamp_utc").isNotNull() &
                col("device_id").isNotNull(),
                True
            ).otherwise(False)
        )
        
        # Add partitioning columns
        df = df.withColumn("year", year(col("timestamp_utc")))
        df = df.withColumn("month", month(col("timestamp_utc")))
        df = df.withColumn("day", dayofmonth(col("timestamp_utc")))
        df = df.withColumn("hour", hour(col("timestamp_utc")))
        
        # Add processing metadata
        df = df.withColumn("processed_timestamp", current_timestamp())
        df = df.withColumn("processing_layer", lit("silver"))
        
        return df
    
    def read_stream(self):
        """
        Read streaming data from raw/bronze table
        
        Returns:
            Streaming DataFrame
        """
        logger.info(f"Reading stream from {self.raw_table}...")
        
        # Read from Delta table as a stream
        stream_df = (
            self.spark.readStream
            .format("delta")
            .table(self.raw_table)
        )
        
        return stream_df
    
    def write_stream(self, df):
        """
        Write streaming data to silver table
        
        Args:
            df: Streaming DataFrame to write
        """
        logger.info(f"Writing stream to {self.silver_table}...")
        
        # Write to Delta table with checkpointing
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")
            .partitionBy("year", "month", "day")
            .trigger(processingTime="10 seconds")  # Micro-batch every 10 seconds
            .toTable(self.silver_table)
        )
        
        logger.info(f"Stream started. Query ID: {query.id}")
        
        return query
    
    def run(self):
        """Execute the streaming pipeline"""
        try:
            logger.info("Starting Raw to Silver streaming pipeline...")
            
            # Read stream from raw table
            raw_stream = self.read_stream()
            
            # Apply transformations
            silver_stream = self.apply_data_quality_rules(raw_stream)
            
            # Write to silver table
            query = self.write_stream(silver_stream)
            
            # Wait for termination (or run indefinitely)
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {str(e)}")
            raise


def main():
    """Main entry point for the streaming job"""
    
    # Configuration - Update these values according to your Databricks setup
    CATALOG_NAME = "your_catalog_name"  # Replace with your catalog name
    RAW_SCHEMA = "raw"  # Your raw/bronze schema name
    SILVER_SCHEMA = "silver"  # Your silver schema name
    
    # Initialize and run the pipeline
    pipeline = RawToSilverStream(
        catalog_name=CATALOG_NAME,
        raw_schema=RAW_SCHEMA,
        silver_schema=SILVER_SCHEMA
    )
    
    pipeline.run()


if __name__ == "__main__":
    main()
