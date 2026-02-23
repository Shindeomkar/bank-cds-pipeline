import yaml
import logging

from pyspark.sql import SparkSession

from utils import (
    read_csv,
    apply_join,
    apply_filter,
    apply_derive
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():

    logger.info("Starting Bank CDS ETL Job")

    spark = SparkSession.builder.appName("bank-cds-etl").getOrCreate()

    # Load configs
    config = load_yaml("config/glue/dev_glue_job_config.yaml")
    mapping = load_yaml("mappings/cds_transform.yaml")

    landing_bucket = config["etl_settings"]["landing_bucket"]
    gold_bucket = config["etl_settings"]["gold_bucket"]

    # Build S3 paths
    cds_path = f"s3://{landing_bucket}/raw/cds/cds_trades_raw.csv"
    ref_path = f"s3://{landing_bucket}/raw/reference/"

    # Read data
    cds_df = read_csv(spark, f"{cds_path}")
    counterparty_df = read_csv(
        spark, f"{ref_path}/counterparty_master.csv"
    )
    fx_df = read_csv(
        spark, f"{ref_path}/fx_rates.csv"
    )

    dfs = {
        "cds": cds_df,
        "counterparty": counterparty_df,
        "fx": fx_df
    }

    df = dfs["cds"]

    # Apply transformations from YAML
    for step in mapping["transformations"]:

        if step["type"] == "join":
            right_df = dfs[step["right"]]

            df = apply_join(
                df,
                right_df,
                step["on"]
            )

        elif step["type"] == "filter":
            df = apply_filter(
                df,
                step["condition"]
            )

        elif step["type"] == "derive":
            df = apply_derive(
                df,
                step["column"],
                step["formula"]
            )

    # Write to Gold
    output_path = f"s3://{gold_bucket}/gold/cds/"

    df.write.mode("overwrite").parquet(output_path)

    logger.info(f"Data written to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()