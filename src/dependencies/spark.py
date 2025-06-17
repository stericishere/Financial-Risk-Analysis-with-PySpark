"""
Spark session management and configuration utilities.

This module provides functions for starting and managing Spark sessions
with appropriate configuration for development and production environments.
"""

import os
import json
import logging
from os import listdir, path
from typing import Optional, Dict, List, Tuple, Any

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkFiles


def start_spark(
    app_name: str = "FinancialRiskAnalysis",
    master: str = "local[*]",
    jar_packages: Optional[List[str]] = None,
    files: Optional[List[str]] = None,
    spark_config: Optional[Dict[str, str]] = None,
) -> Tuple[SparkSession, logging.Logger, Optional[Dict[str, Any]]]:
    """
    Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration), into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    Args:
        app_name: Name of Spark app.
        master: Cluster connection details (defaults to local[*]).
        jar_packages: List of Spark JAR package names.
        files: List of files to send to Spark cluster.
        spark_config: Dictionary of config key-value pairs.

    Returns:
        A tuple of references to the Spark session, logger and config dict.
    """
    # detect execution environment
    flag_repl = not (hasattr(__builtins__, "__IPYTHON__"))
    flag_debug = "DEBUG" in os.environ.keys()

    if not (flag_repl or flag_debug):
        # if in interactive environment, use all provided arguments
        spark_builder = SparkSession.builder.appName(app_name).master(master)

        # add jar packages
        if jar_packages:
            spark_jars_packages = ",".join(list(jar_packages))
            spark_builder.config("spark.jars.packages", spark_jars_packages)

        # add files
        if files:
            spark_files = ",".join(list(files))
            spark_builder.config("spark.files", spark_files)

        # add custom config
        if spark_config:
            for key, val in spark_config.items():
                spark_builder.config(key, val)

        # create session and retrieve Spark logger object
        spark_sess = spark_builder.getOrCreate()
        spark_logger = logging.getLogger(__name__)

        # get config file if sent to cluster with --files
        spark_files_dir = SparkFiles.getRootDirectory()
        config_files = [
            filename
            for filename in listdir(spark_files_dir)
            if filename.endswith("config.json")
        ]

        if config_files:
            path_to_config_file = path.join(spark_files_dir, config_files[0])
            with open(path_to_config_file, "r") as config_file:
                config_dict = json.load(config_file)
            spark_logger.warn(f"loaded config from {config_files[0]}")
        else:
            spark_logger.warn("no config file found")
            config_dict = None

    else:
        # if running locally or in debug mode, read config from local file
        spark_sess = SparkSession.builder.appName(app_name).master(master).getOrCreate()

        spark_logger = logging.getLogger(__name__)

        # load config from local file
        try:
            config_path = "configs/etl_config.json"
            with open(config_path, "r") as config_file:
                config_dict = json.load(config_file)
            spark_logger.info(f"loaded config from {config_path}")
        except FileNotFoundError:
            spark_logger.warning("no config file found")
            config_dict = None

    return spark_sess, spark_logger, config_dict


def stop_spark(spark_session: SparkSession) -> None:
    """
    Stop the Spark session.

    Args:
        spark_session: The Spark session to stop.
    """
    spark_session.stop()


def configure_spark_session(config: Dict[str, Any]) -> SparkConf:
    """
    Configure Spark session based on provided configuration.

    Args:
        config: Configuration dictionary containing Spark settings.

    Returns:
        Configured SparkConf object.
    """
    conf = SparkConf()

    if "spark" in config and "config" in config["spark"]:
        for key, value in config["spark"]["config"].items():
            conf.set(key, value)

    return conf


def get_spark_context_info(spark: SparkSession) -> Dict[str, Any]:
    """
    Get information about the current Spark context.

    Args:
        spark: Active Spark session.

    Returns:
        Dictionary containing Spark context information.
    """
    sc = spark.sparkContext
    return {
        "application_id": sc.applicationId,
        "application_name": sc.appName,
        "master": sc.master,
        "version": sc.version,
        "default_parallelism": sc.defaultParallelism,
        "executor_cores": sc.getConf().get("spark.executor.cores", "Not set"),
        "executor_memory": sc.getConf().get("spark.executor.memory", "Not set"),
        "driver_memory": sc.getConf().get("spark.driver.memory", "Not set"),
    }
