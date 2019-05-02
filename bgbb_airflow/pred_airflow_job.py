import datetime as dt
from os.path import join
from typing import Dict, List, Tuple, Union

import click
import pandas as pd
from bgbb import BGBB
from bgbb.sql.bgbb_udfs import (
    add_mau,
    add_p_th,
    mk_n_returns_udf,
    mk_p_alive_udf,
)
from bgbb.sql.sql_utils import run_rec_freq_spk, S3_DAY_FMT_DASH
from pyspark.sql import SparkSession

from bgbb_airflow.bgbb_utils import PythonLiteralOption
import bgbb_airflow


pd.options.display.max_columns = 20
pd.options.display.width = 120
Dash_str = str

default_pred_bucket = "s3://net-mozaws-prod-us-west-2-pipeline-analysis"
default_pred_prefix = "wbeard/active_profiles"
default_param_bucket = default_pred_bucket
default_param_prefix = "wbeard/bgbb_params"


def pull_most_recent_params(
    spark, max_sub_date: Dash_str, param_bucket, param_prefix
):
    "@max_sub_date: Maximum params date to pull; dashed format yyyy-MM-dd"
    pars_loc = join(param_bucket, param_prefix)
    print("Reading params from {}".format(pars_loc))
    spars = spark.read.parquet(pars_loc)
    pars_df = (
        spars.filter(spars.submission_date_s3 <= max_sub_date)
        .orderBy(spars.submission_date_s3.desc())
        .limit(1)
        .toPandas()
    )
    print("Using params: \n{}".format(pars_df))
    return pars_df


def extract(
    spark,
    sub_date: Dash_str,
    param_bucket,
    param_prefix,
    model_win=90,
    sample_ids: Union[Tuple, List[int]] = (),
):
    "TODO: increase ho_win to evaluate model performance"

    holdout_start_dt = dt.datetime.strptime(
        sub_date, S3_DAY_FMT_DASH
    ) + dt.timedelta(days=1)
    holdout_start = holdout_start_dt.strftime(S3_DAY_FMT_DASH)

    df, q = run_rec_freq_spk(
        ho_win=1,
        model_win=model_win,
        ho_start=holdout_start,
        sample_ids=list(sample_ids),
        spark=spark,
    )

    # Hopefully not too far off from something like
    # [0.825, 0.68, 0.0876, 1.385]
    pars_df = pull_most_recent_params(
        spark=spark,
        max_sub_date=sub_date,
        param_bucket=param_bucket,
        param_prefix=param_prefix,
    )
    abgd_params = pars_df[["alpha", "beta", "gamma", "delta"]].iloc[0].tolist()
    return df, abgd_params


def transform(df, bgbb_params, return_preds=(14,)):
    """
    @return_preds: for each integer value `n`, make predictions
    for how many times a client is expected to return in the next `n`
    days.
    """
    bgbb = BGBB(params=bgbb_params)

    # Create/Apply UDFs
    p_alive = mk_p_alive_udf(bgbb, params=bgbb_params, alive_n_days_later=0)
    n_returns_udfs = [
        (
            "e_total_days_in_next_{}_days".format(days),
            mk_n_returns_udf(
                bgbb, params=bgbb_params, return_in_next_n_days=days
            ),
        )
        for days in return_preds
    ]

    df2 = df.withColumn("P_alive", p_alive(df.Frequency, df.Recency, df.N))
    for days, udf in n_returns_udfs:
        df2 = df2.withColumn(days, udf(df.Frequency, df.Recency, df.N))
    df2 = add_p_th(bgbb, dfs=df2, fcol="Frequency", rcol="Recency", ncol="N")
    df2 = add_mau(
        bgbb,
        dfs=df2,
        fcol="Frequency",
        rcol="Recency",
        ncol="N",
        n_days_future=28,
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


def save(spark, submission_date, pred_bucket, pred_prefix, df):
    path = join(
        pred_bucket,
        pred_prefix,
        "submission_date_s3={}".format(submission_date),
    )
    print("Saving to {}...".format(path))
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
@click.option("--pred-bucket", type=str, default=default_pred_bucket)
@click.option("--pred-prefix", type=str, default=default_pred_prefix)
@click.option("--param-bucket", type=str, default=default_param_bucket)
@click.option("--param-prefix", type=str, default=default_param_prefix)
def main(
    submission_date,
    model_win,
    sample_ids,
    pred_bucket,
    pred_prefix,
    param_bucket,
    param_prefix,
):
    spark = SparkSession.builder.getOrCreate()
    print(
        "Generating predictions with bgbb_airflow version {}".format(
            bgbb_airflow.__version__
        )
    )

    df, abgd_params = extract(
        spark,
        sub_date=submission_date,
        param_bucket=param_bucket,
        param_prefix=param_prefix,
        model_win=model_win,
        sample_ids=sample_ids,
    )
    df2 = transform(df, abgd_params, return_preds=[7, 14, 21, 28])
    save(
        spark,
        submission_date,
        pred_bucket=pred_bucket,
        pred_prefix=pred_prefix,
        df=df2,
    )
    print("Success!")
