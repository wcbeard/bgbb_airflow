import datetime as dt
from os.path import join
from typing import Dict, List, Tuple, Union

import click
import pandas as pd
from pyspark.sql import SparkSession, functions as F

import bgbb_airflow
from bgbb import BGBB
from bgbb.sql.bgbb_udfs import add_mau, add_p_th, mk_n_returns_udf, mk_p_alive_udf
from bgbb_airflow.bgbb_utils import PythonLiteralOption
from bgbb_airflow.sql_utils import S3_DAY_FMT_DASH, BigQueryParameters, run_rec_freq_spk

pd.options.display.max_columns = 40
pd.options.display.width = 120
Dash_str = str

first_dims = ["locale", "normalized_channel", "os", "normalized_os_version", "country"]


def pull_most_recent_params(
    spark, max_sub_date: Dash_str, param_bucket, param_prefix, bucket_protocol="s3"
):
    "@max_sub_date: Maximum params date to pull; dashed format yyyy-MM-dd"
    pars_loc = f"{bucket_protocol}://" + join(param_bucket, param_prefix)
    print(f"Reading params from {pars_loc}")
    spars = spark.read.parquet(pars_loc)
    pars_df = (
        spars.filter(spars.submission_date_s3 <= max_sub_date)
        .orderBy(spars.submission_date_s3.desc())
        .limit(1)
        .withColumn("submission_date_s3", F.col("submission_date_s3").cast("timestamp"))
        .toPandas()
    )
    print(f"Using params: \n{pars_df}")
    return pars_df


def extract(
    spark,
    sub_date: Dash_str,
    param_bucket,
    param_prefix,
    model_win=90,
    sample_ids: Union[Tuple, List[int]] = (),
    first_dims=first_dims,
    bucket_protocol="s3",
    source="hive",
    bigquery_parameters=None,
):
    "TODO: increase ho_win to evaluate model performance"

    holdout_start_dt = dt.datetime.strptime(sub_date, S3_DAY_FMT_DASH) + dt.timedelta(
        days=1
    )
    holdout_start = holdout_start_dt.strftime(S3_DAY_FMT_DASH)

    df, q = run_rec_freq_spk(
        ho_win=1,
        model_win=model_win,
        ho_start=holdout_start,
        sample_ids=list(sample_ids),
        spark=spark,
        first_dims=first_dims,
        source=source,
        bigquery_parameters=bigquery_parameters,
    )

    # Hopefully not too far off from something like
    # [0.825, 0.68, 0.0876, 1.385]
    pars_df = pull_most_recent_params(
        spark=spark,
        max_sub_date=sub_date,
        param_bucket=param_bucket,
        param_prefix=param_prefix,
        bucket_protocol=bucket_protocol,
    )
    abgd_params = pars_df[["alpha", "beta", "gamma", "delta"]].iloc[0].tolist()
    return df, abgd_params


def transform(df, bgbb_params, return_preds=(14,)):
    """
    @return_preds: for each integer value `n`, make predictions
    for how many times a client is expected to return in the next `n`
    days.
    To see which columns are changed, see `test_transform_cols` and
    `test_preds_schema` in test_bgbb.
    """
    bgbb = BGBB(params=bgbb_params)

    # Create/Apply UDFs
    p_alive = mk_p_alive_udf(bgbb, params=bgbb_params, alive_n_days_later=0)
    n_returns_udfs = [
        (
            "e_total_days_in_next_{}_days".format(days),
            mk_n_returns_udf(bgbb, params=bgbb_params, return_in_next_n_days=days),
        )
        for days in return_preds
    ]

    df2 = df.withColumn("P_alive", p_alive(df.Frequency, df.Recency, df.N))
    for days, udf in n_returns_udfs:
        df2 = df2.withColumn(days, udf(df.Frequency, df.Recency, df.N))
    df2 = add_p_th(bgbb, dfs=df2, fcol="Frequency", rcol="Recency", ncol="N")
    df2 = add_mau(
        bgbb, dfs=df2, fcol="Frequency", rcol="Recency", ncol="N", n_days_future=28
    )

    def rename_cols(dfs, col_mapping: Dict[str, str]):
        for old_c, new_c in col_mapping.items():
            dfs = dfs.withColumnRenamed(old_c, new_c)
        return dfs

    renames = {
        "N": "num_opportunities",
        "P_alive": "prob_active",
        "p": "prob_daily_usage",
        "th": "prob_daily_leave",
    }
    df2 = rename_cols(df2, renames)
    to_lower_case = dict(zip(df2.columns, map(str.lower, df2.columns)))
    df2 = rename_cols(df2, to_lower_case)

    return df2


def save(spark, submission_date, pred_bucket, pred_prefix, df, bucket_protocol="s3"):
    path = f"{bucket_protocol}://" + join(
        pred_bucket, pred_prefix, f"submission_date_s3={submission_date}"
    )
    print(f"Saving to {path}...")
    (df.write.partitionBy("sample_id").parquet(path, mode="overwrite"))
    print("Done writing")
    df_out = spark.read.parquet(path).limit(5).toPandas()
    print("Sample of written data:\n", df_out)


@click.command("bgbb_pred")
@click.option("--submission-date", type=str, required=True)
@click.option("--model-win", type=int, default=120)
@click.option(
    "--sample-ids",
    cls=PythonLiteralOption,
    default="[]",
    help="List of integer sample ids or None",
)
@click.option("--pred-bucket", type=str, default="telemetry-test-bucket")
@click.option("--pred-prefix", type=str, default="bgbb/active_profiles/v1")
@click.option("--param-bucket", type=str, default="telemetry-test-bucket")
@click.option("--param-prefix", type=str, default="bgbb/params/v1")
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
    sample_ids,
    pred_bucket,
    pred_prefix,
    param_bucket,
    param_prefix,
    bucket_protocol,
    source,
    project_id,
    dataset_id,
    view_materialization_project,
    view_materialization_dataset,
):
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    print(
        f"Generating predictions with bgbb_airflow version {bgbb_airflow.__version__}"
    )

    df, abgd_params = extract(
        spark,
        sub_date=submission_date,
        param_bucket=param_bucket,
        param_prefix=param_prefix,
        model_win=model_win,
        sample_ids=sample_ids,
        bucket_protocol=bucket_protocol,
        source=source,
        bigquery_parameters=BigQueryParameters(
            project_id,
            dataset_id,
            view_materialization_project,
            view_materialization_dataset,
        ),
    )
    df2 = transform(df, abgd_params, return_preds=[7, 14, 21, 28])
    save(
        spark,
        submission_date,
        pred_bucket=pred_bucket,
        pred_prefix=pred_prefix,
        df=df2,
        bucket_protocol=bucket_protocol,
    )
    print("Success!")
