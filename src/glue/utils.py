import logging
from pyspark.sql.functions import col, expr

logger = logging.getLogger(__name__)


def read_csv(spark, path):
    logger.info(f"Reading CSV from {path}")
    return spark.read.option("header", "true").csv(path)


def apply_join(left_df, right_df, on_col, how="left"):
    logger.info(f"Joining on {on_col}")
    return left_df.join(right_df, on_col, how)


def apply_filter(df, condition):
    logger.info(f"Filtering: {condition}")
    return df.filter(condition)


def apply_derive(df, column, formula):
    logger.info(f"Deriving column {column}")
    return df.withColumn(column, expr(formula))