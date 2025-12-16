"""Optimized Spark DataFrame-based data reader"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, explode, when, isnan, isnull
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
    ArrayType, MapType
)
import logging

logger = logging.getLogger(__name__)


class SparkDataReader:
    """Optimized data reader using Spark DataFrames for distributed processing"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize Spark-based data reader
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def read_bookings_dataframe(self, input_path: str) -> DataFrame:
        """
        Read booking JSON files as Spark DataFrame (distributed processing)
        
        This method reads JSON files and converts them to a flattened DataFrame
        where each row represents a passenger-flight combination.
        
        Args:
            input_path: Path to bookings directory (local or HDFS URI)
            
        Returns:
            Spark DataFrame with schema:
            - booking_timestamp: Timestamp of booking event
            - passenger_uci: Unique passenger identifier
            - passenger_type: ADT or CHD
            - passenger_age: Passenger age (nullable)
            - origin_airport: Origin airport IATA code
            - destination_airport: Destination airport IATA code
            - departure_date: Flight departure datetime
            - arrival_date: Flight arrival datetime
            - operating_airline: Operating airline code
            - booking_status: Booking status (CONFIRMED, CANCELLED, etc.)
        """
        is_hdfs = input_path.startswith('hdfs://') or input_path.startswith('hdfs:/')
        
        logger.debug(f"Reading bookings as Spark DataFrame from {'HDFS' if is_hdfs else 'local'}: {input_path}")
        
        # Read JSON lines as text first (handles both local and HDFS)
        if is_hdfs:
            text_df = self.spark.read.text(input_path)
        else:
            # For local files, use file:// protocol or absolute path
            from pathlib import Path
            path_obj = Path(input_path)
            if path_obj.is_file():
                # Single file
                local_path = str(path_obj.absolute())
            elif path_obj.is_dir():
                # Directory - Spark can read all JSON files
                local_path = str(path_obj.absolute())
            else:
                raise FileNotFoundError(f"Input path does not exist: {input_path}")
            
            text_df = self.spark.read.text(f"file://{local_path}")
        
        # Define schema for booking JSON structure
        booking_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event", StructType([
                StructField("DataElement", StructType([
                    StructField("travelrecord", StructType([
                        StructField("passengersList", ArrayType(StructType([
                            StructField("uci", StringType(), True),
                            StructField("passengerType", StringType(), True),
                            StructField("age", IntegerType(), True),
                            StructField("tattoo", IntegerType(), True)
                        ])), True),
                        StructField("productsList", ArrayType(StructType([
                            StructField("bookingStatus", StringType(), True),
                            StructField("flight", StructType([
                                StructField("originAirport", StringType(), True),
                                StructField("destinationAirport", StringType(), True),
                                StructField("departureDate", StringType(), True),
                                StructField("arrivalDate", StringType(), True),
                                StructField("operatingAirline", StringType(), True)
                            ]), True)
                        ])), True)
                    ]), True)
                ]), True)
            ]), True)
        ])
        
        # Parse JSON and extract nested structures
        # First, parse the JSON string
        parsed_df = text_df.select(
            from_json(col("value"), booking_schema).alias("booking")
        ).filter(col("booking").isNotNull())
        
        # Extract timestamp
        df_with_timestamp = parsed_df.select(
            col("booking.timestamp").alias("booking_timestamp"),
            col("booking.event.DataElement.travelrecord").alias("travelrecord")
        ).filter(col("travelrecord").isNotNull())
        
        # Explode passengers and products to create passenger-flight combinations
        # Each row will be one passenger on one flight
        exploded_df = df_with_timestamp.select(
            col("booking_timestamp"),
            explode(col("travelrecord.passengersList")).alias("passenger"),
            explode(col("travelrecord.productsList")).alias("product")
        ).filter(
            col("passenger").isNotNull() & 
            col("product").isNotNull() &
            col("product.flight").isNotNull()
        )
        
        # Flatten the structure
        result_df = exploded_df.select(
            col("booking_timestamp").cast("timestamp").alias("booking_timestamp"),
            col("passenger.uci").alias("passenger_uci"),
            col("passenger.passengerType").alias("passenger_type"),
            col("passenger.age").alias("passenger_age"),
            col("product.bookingStatus").alias("booking_status"),
            col("product.flight.originAirport").alias("origin_airport"),
            col("product.flight.destinationAirport").alias("destination_airport"),
            col("product.flight.departureDate").alias("departure_date_str"),
            col("product.flight.arrivalDate").alias("arrival_date_str"),
            col("product.flight.operatingAirline").alias("operating_airline")
        ).filter(
            col("passenger_uci").isNotNull() &
            col("origin_airport").isNotNull() &
            col("destination_airport").isNotNull() &
            col("operating_airline").isNotNull()
        )
        
        # Convert date strings to timestamps
        from pyspark.sql.functions import to_timestamp
        
        result_df = result_df.withColumn(
            "departure_date",
            to_timestamp(col("departure_date_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        ).withColumn(
            "arrival_date",
            to_timestamp(col("arrival_date_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        ).drop("departure_date_str", "arrival_date_str")
        
        # Filter out rows with invalid dates
        result_df = result_df.filter(
            col("departure_date").isNotNull() &
            col("arrival_date").isNotNull()
        )
        
        logger.debug(f"Read bookings DataFrame with {result_df.count()} passenger-flight combinations")
        
        return result_df

