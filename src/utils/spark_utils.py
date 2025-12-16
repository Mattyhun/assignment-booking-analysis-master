"""Spark utility functions"""
import os
import subprocess
from pathlib import Path
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def find_java_home(min_version: int = 17) -> str:
    """
    Find Java home directory with minimum version requirement
    
    Args:
        min_version: Minimum Java version required (default: 17)
        
    Returns:
        Path to Java home directory
        
    Raises:
        RuntimeError: If no suitable Java version is found
    """
    # Check if JAVA_HOME is already set and valid
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        java_version = _get_java_version(java_home)
        if java_version and java_version >= min_version:
            logger.debug(f"Using JAVA_HOME from environment: {java_home} (Java {java_version})")
            return java_home
    
    # Try to find Java using java_home utility (macOS/Linux)
    try:
        # Use java_home with specific version
        result = subprocess.run(
            ['/usr/libexec/java_home', '-v', f'{min_version}'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout:
            java_home_path = result.stdout.strip()
            if java_home_path and Path(java_home_path).exists():
                java_version = _get_java_version(java_home_path)
                if java_version is not None and java_version >= min_version:
                    logger.debug(f"Found Java {java_version} at: {java_home_path}")
                    return java_home_path
                elif java_version is None:
                    # If version check fails, still use it if path exists (trust java_home)
                    logger.warning(f"Could not verify Java version at {java_home_path}, but using it anyway")
                    return java_home_path
        
        # Fallback: list all versions and parse
        result = subprocess.run(
            ['/usr/libexec/java_home', '-V'],
            capture_output=True,
            text=True
        )
        
        # java_home outputs to stderr, not stdout
        output = result.stderr if result.stderr else result.stdout
        
        # Parse output to find Java versions
        # Format: "    17.0.15 (arm64) "Microsoft" - "OpenJDK 17.0.15" /path/to/Home"
        lines = output.split('\n')
        for line in lines:
            # Look for version number
            if f'{min_version}.' in line:
                # Try to extract path - it's usually at the end of the line
                # Format: ... /Users/.../JavaVirtualMachines/.../Contents/Home
                if '/Contents/Home' in line:
                    # Find the path part
                    parts = line.split('/Contents/Home')
                    if len(parts) >= 2:
                        path = parts[0] + '/Contents/Home'
                        if Path(path).exists():
                            java_version = _get_java_version(path)
                            if java_version and java_version >= min_version:
                                logger.debug(f"Found Java {java_version} at: {path}")
                                return path
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        logger.debug(f"Could not use java_home utility: {e}")
        pass
    
    # Try common Java installation paths
    common_paths = [
        '/Library/Java/JavaVirtualMachines',
        '/usr/lib/jvm',
        '/opt/java',
    ]
    
    for base_path in common_paths:
        if Path(base_path).exists():
            for jvm_path in Path(base_path).iterdir():
                java_home_path = jvm_path / 'Contents' / 'Home' if (jvm_path / 'Contents').exists() else jvm_path
                if (java_home_path / 'bin' / 'java').exists():
                    java_version = _get_java_version(str(java_home_path))
                    if java_version and java_version >= min_version:
                        logger.debug(f"Found Java {java_version} at: {java_home_path}")
                        return str(java_home_path)
    
    raise RuntimeError(
        f"Java {min_version}+ is required for PySpark. "
        f"Please install Java {min_version} or higher and set JAVA_HOME environment variable. "
        f"On macOS, you can install it with: brew install openjdk@{min_version}"
    )


def _get_java_version(java_home: str) -> int:
    """Get Java version number from JAVA_HOME"""
    try:
        java_bin = Path(java_home) / 'bin' / 'java'
        if not java_bin.exists():
            return None
        
        # java -version outputs to stderr, not stdout
        result = subprocess.run(
            [str(java_bin), '-version'],
            capture_output=True,
            text=True
        )
        
        # Parse version from output like "openjdk version "17.0.15""
        # java -version outputs to stderr
        output = result.stderr if result.stderr else result.stdout
        for line in output.split('\n'):
            if 'version "' in line:
                version_str = line.split('version "')[1].split('"')[0]
                version_major = int(version_str.split('.')[0])
                return version_major
    except Exception as e:
        logger.debug(f"Could not get Java version from {java_home}: {e}")
    return None


def create_spark_session(app_name: str = "KLM Booking Analysis") -> SparkSession:
    """
    Create Spark session with appropriate configuration
    
    Ensures Java 17+ is used for Spark compatibility.
    
    Args:
        app_name: Application name
        
    Returns:
        Configured SparkSession
        
    Raises:
        RuntimeError: If Java 17+ is not found
    """
    # Find and set Java 17+
    try:
        java_home = find_java_home(min_version=17)
        os.environ['JAVA_HOME'] = java_home
        
        # Also update PATH to use this Java
        java_bin = Path(java_home) / 'bin'
        current_path = os.environ.get('PATH', '')
        if str(java_bin) not in current_path:
            os.environ['PATH'] = f"{java_bin}:{current_path}"
        
        logger.debug(f"Configured Java: {java_home}")
    except RuntimeError as e:
        logger.error(str(e))
        raise
    
    # Verify Java version
    try:
        result = subprocess.run(
            ['java', '-version'],
            capture_output=True,
            text=True
        )
        # java -version outputs to stderr
        version_line = result.stderr.split('\n')[0] if result.stderr else 'OK'
        logger.debug(f"Java version: {version_line}")
    except Exception as e:
        logger.warning(f"Could not verify Java version: {e}")
    
    builder = SparkSession.builder.appName(app_name)
    
    # Configure for local and cluster execution
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Set Java home explicitly for Spark
    builder = builder.config("spark.driver.extraJavaOptions", f"-Djava.home={java_home}")
    builder = builder.config("spark.executor.extraJavaOptions", f"-Djava.home={java_home}")
    
    # For HDFS support
    try:
        # Try to load HDFS configuration
        builder = builder.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    except Exception:
        # If HDFS config not available, will use local filesystem
        pass
    
    spark = builder.getOrCreate()
    logger.debug(f"Spark session created: {spark.version}")
    return spark


