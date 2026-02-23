import yaml
import boto3
import logging

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():

    logger.info("Starting Bank CDS ETL Job")

    spark = SparkSession.builder.appName("bank-cds-etl").getOrCreate()

    config = load_yaml("config/glue/dev_glue_job_config.yaml")
    mapping = load_yaml("mappings/cds_transform.yaml")

    logger.info("Config Loaded")
    logger.info("Mapping Loaded")

    logger.info("ETL framework initialized")

    spark.stop()


if __name__ == "__main__":
    main()