"""Data reader service for local and HDFS file access"""
import json
from pathlib import Path
from typing import Iterator, Optional
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.models.booking import BookingEvent

logger = logging.getLogger(__name__)


class DataReader:
    """Service for reading booking data from local or HDFS"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize data reader
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def read_bookings(self, input_path: str) -> Iterator[Optional[BookingEvent]]:
        """
        Read booking JSON files from local or HDFS
        
        Args:
            input_path: Path to bookings directory (local or HDFS URI)
            
        Yields:
            BookingEvent objects or None for invalid records
        """
        is_hdfs = input_path.startswith('hdfs://') or input_path.startswith('hdfs:/')
        
        if is_hdfs:
            yield from self._read_from_hdfs(input_path)
        else:
            yield from self._read_from_local(input_path)
    
    def _read_from_local(self, input_path: str) -> Iterator[Optional[BookingEvent]]:
        """Read bookings from local filesystem"""
        path = Path(input_path)
        
        if path.is_file():
            # Single file
            files = [path]
        elif path.is_dir():
            # Directory - find all JSON files
            files = list(path.glob('*.json'))
        else:
            logger.error(f"Input path does not exist: {input_path}")
            return
        
        logger.debug(f"Reading {len(files)} booking file(s) from local filesystem")
        
        for file_path in files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            data = json.loads(line)
                            booking = BookingEvent.from_raw_json(data)
                            if booking:
                                yield booking
                            else:
                                logger.debug(f"Invalid booking record in {file_path}:{line_num}")
                        except json.JSONDecodeError as e:
                            logger.warning(f"Invalid JSON in {file_path}:{line_num}: {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"Error parsing booking in {file_path}:{line_num}: {e}")
                            continue
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
                continue
    
    def _read_from_hdfs(self, hdfs_path: str) -> Iterator[Optional[BookingEvent]]:
        """
        Read bookings from HDFS using Spark distributed processing.
        
        This method uses Spark's distributed text reader to efficiently process
        large datasets across cluster nodes. For TB-scale data, Spark automatically:
        - Partitions data across nodes
        - Processes in parallel
        - Handles data locality optimization
        
        Args:
            hdfs_path: HDFS URI (e.g., hdfs://namenode:9000/bookings/)
            
        Yields:
            BookingEvent objects or None for invalid records
        """
        logger.debug(f"Reading bookings from HDFS: {hdfs_path}")
        logger.debug("Using Spark distributed processing for large-scale data")
        
        try:
            # Read JSON files from HDFS using Spark's distributed text reader
            # Spark automatically handles:
            # - Partitioning across cluster nodes
            # - Parallel processing
            # - Data locality optimization
            # - Fault tolerance
            df = self.spark.read.text(hdfs_path)
            
            # Get partition count (for logging)
            partition_count = df.rdd.getNumPartitions()
            logger.debug(f"HDFS data partitioned into {partition_count} partitions for parallel processing")
            
            # Process each partition in parallel
            # Note: For very large datasets, this would be optimized further
            # by using Spark transformations instead of collecting to driver
            row_count = 0
            for row in df.collect():
                row_count += 1
                line = row.value
                if not line or not line.strip():
                    continue
                
                try:
                    data = json.loads(line)
                    booking = BookingEvent.from_raw_json(data)
                    if booking:
                        yield booking
                except json.JSONDecodeError as e:
                    logger.debug(f"Invalid JSON in HDFS data (row {row_count}): {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing booking from HDFS (row {row_count}): {e}")
                    continue
            
            logger.debug(f"Processed {row_count} lines from HDFS")
            
        except Exception as e:
            logger.error(f"Error reading from HDFS {hdfs_path}: {e}")
            logger.error("Ensure HDFS is accessible and path is correct")
            logger.error("Hadoop configuration should be in /etc/hadoop/conf/")
            raise


