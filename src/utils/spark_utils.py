"""Spark utility functions"""
# Windows compatibility fix - must be imported before any PySpark imports
import src.utils.pyspark_windows_fix  # noqa: F401

import os
import platform
import subprocess
from pathlib import Path
import logging

from pyspark.sql import SparkSession

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
    
    # Try common Java installation paths (platform-specific)
    if platform.system() == "Windows":
        common_paths = [
            os.path.join(os.environ.get('ProgramFiles', 'C:\\Program Files'), 'Eclipse Adoptium'),
            os.path.join(os.environ.get('ProgramFiles', 'C:\\Program Files'), 'Java'),
            os.path.join(os.environ.get('ProgramFiles(x86)', 'C:\\Program Files (x86)'), 'Java'),
            'C:\\Program Files\\Eclipse Adoptium',
            'C:\\Program Files\\Java',
        ]
    else:
        common_paths = [
            '/Library/Java/JavaVirtualMachines',
            '/usr/lib/jvm',
            '/opt/java',
        ]
    
    for base_path in common_paths:
        base_path_obj = Path(base_path)
        if base_path_obj.exists():
            for jvm_path in base_path_obj.iterdir():
                # Windows: Java is usually directly in the path (e.g., jdk-17.0.x)
                # macOS/Linux: May have Contents/Home structure
                if platform.system() == "Windows":
                    java_home_path = jvm_path
                else:
                    java_home_path = jvm_path / 'Contents' / 'Home' if (jvm_path / 'Contents').exists() else jvm_path
                
                java_exe = java_home_path / 'bin' / 'java.exe' if platform.system() == "Windows" else java_home_path / 'bin' / 'java'
                if java_exe.exists():
                    java_version = _get_java_version(str(java_home_path))
                    if java_version and java_version >= min_version:
                        logger.debug(f"Found Java {java_version} at: {java_home_path}")
                        return str(java_home_path)
    
    # Platform-specific error message
    if platform.system() == "Windows":
        install_instructions = (
            f"Please install Java {min_version} or higher and set JAVA_HOME environment variable. "
            f"Download from https://adoptium.net/ or use Chocolatey: choco install openjdk{min_version}"
        )
    elif platform.system() == "Darwin":  # macOS
        install_instructions = (
            f"Please install Java {min_version} or higher and set JAVA_HOME environment variable. "
            f"On macOS, you can install it with: brew install openjdk@{min_version}"
        )
    else:  # Linux
        install_instructions = (
            f"Please install Java {min_version} or higher and set JAVA_HOME environment variable. "
            f"On Ubuntu/Debian: sudo apt-get install openjdk-{min_version}-jdk"
        )
    
    raise RuntimeError(
        f"Java {min_version}+ is required for PySpark. {install_instructions}"
    )


def _get_java_version(java_home: str) -> int:
    """Get Java version number from JAVA_HOME"""
    try:
        # Windows uses java.exe, Unix uses java
        java_exe_name = 'java.exe' if platform.system() == "Windows" else 'java'
        java_bin = Path(java_home) / 'bin' / java_exe_name
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
        path_separator = ';' if platform.system() == "Windows" else ':'
        if str(java_bin) not in current_path:
            os.environ['PATH'] = f"{java_bin}{path_separator}{current_path}"
        
        logger.debug(f"Configured Java: {java_home}")
    except RuntimeError as e:
        logger.error(str(e))
        raise
    
    # Set Python executable for PySpark (fixes "python3" not found on Windows)
    import sys
    python_executable = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable
    logger.debug(f"Set PySpark Python executable: {python_executable}")
    
    # Verify Java version and installation
    try:
        java_exe = Path(java_home) / 'bin' / ('java.exe' if platform.system() == "Windows" else 'java')
        if not java_exe.exists():
            raise RuntimeError(f"Java executable not found at {java_exe}")
        
        result = subprocess.run(
            [str(java_exe), '-version'],
            capture_output=True,
            text=True,
            timeout=10
        )
        # java -version outputs to stderr
        version_line = result.stderr.split('\n')[0] if result.stderr else 'OK'
        logger.debug(f"Java version: {version_line}")
        
        # Verify Java can actually run (check for security file issues)
        if result.returncode != 0:
            logger.warning(f"Java version check returned non-zero exit code: {result.returncode}")
    except subprocess.TimeoutExpired:
        logger.warning("Java version check timed out")
    except Exception as e:
        logger.warning(f"Could not verify Java version: {e}")
    
    builder = SparkSession.builder.appName(app_name)
    
    # Configure for local and cluster execution
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Set Java home and Python executable for Spark
    # Note: JAVA_HOME is already set as environment variable above, which Spark will use
    # We also set it in Spark config for executor processes
    builder = builder.config("spark.executorEnv.JAVA_HOME", java_home)
    
    # Set Python executable in Spark config
    builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", python_executable)
    builder = builder.config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable)
    
    # Windows-specific configuration
    if platform.system() == "Windows":
        # Use TCP sockets instead of Unix domain sockets on Windows
        import tempfile
        temp_dir = tempfile.gettempdir()
        spark_local_dir = os.path.join(temp_dir, "spark-temp")
        os.makedirs(spark_local_dir, exist_ok=True)
        
        builder = builder.config("spark.local.dir", spark_local_dir)
        builder = builder.config("spark.sql.warehouse.dir", os.path.join(spark_local_dir, "warehouse"))
        
        # Disable Unix socket usage on Windows
        builder = builder.config("spark.driver.host", "localhost")
        builder = builder.config("spark.driver.port", "0")  # Use random port
        
        # Additional Windows-specific Java configuration
        # Ensure Java path is properly set and doesn't cause security file issues
        java_bin_path = str(Path(java_home) / 'bin')
        builder = builder.config("spark.executorEnv.PATH", f"{java_bin_path};{os.environ.get('PATH', '')}")
        
        # Set master to local to avoid cluster-related issues on Windows
        builder = builder.master("local[*]")
        
        logger.debug("Applied Windows-specific Spark configuration")
    
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


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stop Spark session gracefully with proper cleanup.
    
    On Windows, Spark processes sometimes don't shut down cleanly,
    so we ensure proper cleanup of both SparkSession and SparkContext
    and give processes time to shut down gracefully.
    
    Args:
        spark: SparkSession instance to stop
    """
    import time
    
    try:
        logger.debug("Stopping Spark session...")
        
        # Stop SparkContext explicitly first (if available)
        try:
            spark_context = spark.sparkContext
            if spark_context:
                spark_context.stop()
        except Exception as e:
            logger.debug(f"Error stopping SparkContext: {e}")
        
        # Stop SparkSession
        spark.stop()
        
        # On Windows, give processes a moment to shut down gracefully
        # This helps prevent Windows from forcefully terminating processes
        if platform.system() == "Windows":
            time.sleep(0.5)
        
        logger.debug("Spark session stopped successfully")
    except Exception as e:
        logger.warning(f"Error during Spark cleanup: {e}")
        # Try to stop anyway
        try:
            spark.stop()
        except Exception:
            pass


