from typing import Tuple

import click
import pandas as pd
from pyspark.sql import SparkSession

import bgbb_airflow
from bgbb import BGBB
from bgbb_airflow.bgbb_utils import PythonLiteralOption
from bgbb_airflow.sql_utils import (
    S3_DAY_FMT_DASH,
    BigQueryParameters,
    reduce_rec_freq_spk,
    run_rec_freq_spk,
)


def extract(
    ho_start: "YYYY-MM-dd",  # noqa: F821
    spark,
    ho_win=7,
    model_win=120,
    samp_fraction=0.1,
    sample_ids=[0],
    check_min_users=50000,
    source="hive",
    bigquery_parameters=None,
) -> Tuple[pd.DataFrame, int]:
    """
    check_min_users: minimum number of users that should be pulled
    TODO: heuristic to get a high enough sample size
    """
    dfs_all, _q = run_rec_freq_spk(
        model_win=model_win,
        ho_start=ho_start,
        sample_ids=sample_ids,
        spark=spark,
        holdout=True,
        ho_win=ho_win,
        source=source,
        bigquery_parameters=bigquery_parameters,
    )
    df = dfs_all.sample(fraction=samp_fraction)
    user_col = "n_custs"
    dfpr = (
        reduce_rec_freq_spk(df, rfn_cols=["Recency", "Frequency", "N"])
        .toPandas()
        # Rename to conform to api
        .rename(columns=str.lower)
        .rename(columns={"n_users": user_col})
    )
    n_users = dfpr[user_col].sum()
    print("{:,.0f} users pulled".format(n_users))
    assert (
        dfpr[user_col].sum() > check_min_users
    ), "Assuming we're training on at least {} clients".format(check_min_users)

    return dfpr, n_users


def transform(
    df, spark, penalizer_coef=0.01, start_params=[0.387, 0.912, 0.102, 1.504]
):
    bg = BGBB(
        # starting parameter values
        penalizer_coef=penalizer_coef,
        params=start_params,
    )
    bg.rfn.fit(df)
    params_df = spark.createDataFrame(
        pd.DataFrame({k: [v] for k, v in bg.params_.items()})
    )
    return params_df


def save(submission_date, bucket, prefix, params_df, bucket_protocol="s3"):
    path = f"{bucket_protocol}://{bucket}/{prefix}/submission_date_s3={submission_date}"
    print(f"Saving to: {path}")
    (params_df.repartition(1).write.parquet(path, mode="overwrite"))


@click.command("bgbb_fit")
@click.option("--submission-date", type=str, required=True)
@click.option("--model-win", type=int, default=120)
@click.option(
    "--start-params", cls=PythonLiteralOption, default="[0.387, 0.912, 0.102, 1.504]"
)
@click.option(
    "--sample-ids",
    cls=PythonLiteralOption,
    default="[42]",
    help="List of integer sample ids or None",
)
@click.option("--sample-fraction", type=float, default=0.1)
@click.option("--check-min-users", type=int, default=50000)
@click.option("--penalizer-coef", type=float, default=0.01)
@click.option("--bucket", type=str, default="telemetry-test-bucket")
@click.option("--prefix", type=str, default="bgbb/params/v1")
@click.option(
    "--bucket-protocol", type=click.Choice(["gs", "s3", "file"]), default="s3"
)
@click.option("--source", type=click.Choice(["bigquery", "hive"]), default="hive")
@click.option("--project-id", type=str, default="moz-fx-data-shared-prod")
@click.option("--dataset-id", type=str, default="telemetry")
@click.option("--view-materialization-project", type=str)
@click.option("--view-materialization-dataset", type=str)
def main(
    submission_date,
    model_win,
    start_params,
    sample_ids,
    sample_fraction,
    check_min_users,
    penalizer_coef,
    bucket,
    prefix,
    bucket_protocol,
    source,
    project_id,
    dataset_id,
    view_materialization_project,
    view_materialization_dataset,
):
    print(f"Running param fitting. bgbb_airflow version {bgbb_airflow.__version__}")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    ho_start = pd.to_datetime(submission_date).strftime(S3_DAY_FMT_DASH)

    df, _ = extract(
        ho_start,
        spark,
        ho_win=7,
        model_win=model_win,
        samp_fraction=sample_fraction,
        sample_ids=sample_ids,
        check_min_users=check_min_users,
        source=source,
        bigquery_parameters=BigQueryParameters(
            project_id,
            dataset_id,
            view_materialization_project,
            view_materialization_dataset,
        ),
    )
    df2 = transform(df, spark, penalizer_coef=penalizer_coef, start_params=start_params)
    save(submission_date, bucket, prefix, df2, bucket_protocol)
    print("Learning Success!")
