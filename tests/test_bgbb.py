import datetime as dt
from functools import partial
from itertools import count

import numpy.random as nr
import pandas as pd
from click.testing import CliRunner
from pandas import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from bgbb_airflow import fit_airflow_job as fit_job
from bgbb_airflow import pred_airflow_job as pred_job
from bgbb_airflow.pred_airflow_job import first_dims
from bgbb_airflow.sql_utils import S3_DAY_FMT, S3_DAY_FMT_DASH
from pytest import fixture

MODEL_WINDOW = 90
HO_WINDOW = 10
MODEL_START = pd.to_datetime("2018-10-10")
HO_START = MODEL_START + dt.timedelta(days=MODEL_WINDOW)
MODEL_LAST_DAY = HO_START - dt.timedelta(days=1)
HO_ENDp1 = HO_START + dt.timedelta(days=HO_WINDOW + 1)
day_range = pd.date_range(MODEL_START, HO_ENDp1)

N_CLIENTS_IN_SAMPLE = 10
N_CLIENTS_ALL = 2 * N_CLIENTS_IN_SAMPLE


@fixture()
def create_clients_daily_table(spark, dataframe_factory):
    clientsdaily_schema = StructType(
        [
            StructField("app_name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("sample_id", StringType(), True),
            StructField("submission_date_s3", StringType(), True),
            StructField("locale", StringType(), True),
            StructField("normalized_channel", StringType(), True),
            StructField("os", StringType(), True),
            StructField("normalized_os_version", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )

    default_sample = {
        "app_name": "Firefox",
        "channel": "release",
        "client_id": "client-id",
        "sample_id": "1",
        "submission_date_s3": "20181220",
        "locale": "en-IN",
        "normalized_channel": "release",
        "os": "Darwin",
        "normalized_os_version": "10",
        "country": "IN",
    }

    def generate_data(dataframe_factory):
        return partial(
            dataframe_factory.create_dataframe,
            base=default_sample,
            schema=clientsdaily_schema,
        )

    def coin_flip(p):
        return nr.binomial(1, p) == 1

    def gen_coins(n_coins, abgd=[1, 3, 4, 10]):
        a, b, g, d = abgd
        p = nr.beta(a, b, size=n_coins)
        th = nr.beta(g, d, size=n_coins)
        return p, th

    def client_2_daily_pings(client, days):
        client_days = []
        for day in days:
            client.update(submission_date_s3=day.strftime(S3_DAY_FMT))
            client_days.append(client.copy())
        return client_days

    def gen_client_days(
        client: dict, day_range, p: float, th: float, ensure_first=True
    ):
        """If `ensure_first`, add 1st day of day_range to their history
        so that every client will show up in `rfn`.
        """
        days_used_browser = []
        for day in day_range:
            # die coin
            if coin_flip(th):
                break
            return_today = coin_flip(p)
            if return_today:
                days_used_browser.append(day)
        if ensure_first and not days_used_browser:
            days_used_browser = [day_range[0]]
        return client_2_daily_pings(client, days_used_browser)

    def gen_client_dicts(n_clients_in_sample, abgd=[1, 1, 1, 10]):
        samples = ["1"] * n_clients_in_sample + ["2"] * n_clients_in_sample
        ps, θs = gen_coins(abgd=abgd, n_coins=len(samples))
        ps[0], θs[0] = 1, 0  # at least someone returns every day

        cids_rows = []
        for cid, samp, p, th in zip(count(), samples, ps, θs):
            row = default_sample.copy()
            row.update(dict(client_id=cid, sample_id=samp))

            cid_rows = gen_client_days(client=row, day_range=day_range, p=p, th=th)
            cids_rows.extend(cid_rows)
        return cids_rows

    cdaily_factory = generate_data(dataframe_factory)

    def gen_clients_daily(n_clients_in_sample, abgd=[1, 3, 1, 10], seed=0):
        nr.seed(seed)
        table_data = gen_client_dicts(
            n_clients_in_sample=n_clients_in_sample, abgd=abgd
        )

        dataframe = cdaily_factory(table_data)
        dataframe.createOrReplaceTempView("clients_daily")
        dataframe.cache()
        return dataframe

    gen_clients_daily(N_CLIENTS_IN_SAMPLE)


@fixture(autouse=True)
def mock_external_params(monkeypatch):
    def mocked_pars(spark, max_sub_date, param_bucket, param_prefix, bucket_protocol):
        pars_df = DataFrame(
            {"alpha": [0.825], "beta": [0.68], "gamma": [0.0876], "delta": [1.385]}
        )
        return pars_df

    monkeypatch.setattr(pred_job, "pull_most_recent_params", mocked_pars)


@fixture
def rfn(spark, create_clients_daily_table):
    rfn_sdf, pars = pred_job.extract(
        spark,
        sub_date=MODEL_LAST_DAY.strftime(S3_DAY_FMT_DASH),
        param_bucket="dummy_bucket",
        param_prefix="dummy_prefix",
        model_win=MODEL_WINDOW,
        sample_ids=[1],
    )
    rfn2 = pred_job.transform(rfn_sdf, pars, return_preds=[7, 14, 21, 28])
    return rfn2


@fixture
def rfn_pd(rfn):
    return rfn.toPandas().set_index("client_id").sort_index()


def test_max_preds(rfn_pd):
    """
    Coins for client 1 were set for immortality:
    these should have highest predict prob(alive)
    and # of returns.
    """
    pred_cols = [
        "e_total_days_in_next_7_days",
        "e_total_days_in_next_14_days",
        "prob_active",
    ]
    first_client_preds = rfn_pd.loc["0"][pred_cols]

    max_min = rfn_pd[pred_cols].apply(["max", "min"])

    assert (first_client_preds == max_min.loc["max"]).all()
    assert (first_client_preds > max_min.loc["min"]).all()
    assert len(rfn_pd) == N_CLIENTS_IN_SAMPLE


def test_get_params(spark, create_clients_daily_table):
    "TODO: test multiple preds"
    ho_start = HO_START.strftime(S3_DAY_FMT_DASH)
    rfn, n_users = fit_job.extract(
        ho_start,
        spark,
        ho_win=HO_WINDOW,
        model_win=MODEL_WINDOW,
        samp_fraction=1.0,
        check_min_users=1,
        sample_ids=range(100),
    )
    assert n_users == N_CLIENTS_ALL, "Windows should contain all created users"
    params = fit_job.transform(rfn, spark, penalizer_coef=0.01)
    assert params.count() == 1, "Returns single row"
    return params


def test_preds_schema(rfn):
    expected_cols = [
        "client_id",
        "sample_id",
        "recency",
        "frequency",
        "num_opportunities",
        "max_day",
        "min_day",
        "prob_active",
        "e_total_days_in_next_7_days",
        "e_total_days_in_next_14_days",
        "e_total_days_in_next_21_days",
        "e_total_days_in_next_28_days",
        "prob_daily_usage",
        "prob_daily_leave",
        "prob_mau",
        "locale",
        "normalized_channel",
        "os",
        "normalized_os_version",
        "country",
    ]
    assert sorted(rfn.columns) == sorted(expected_cols)


def test_transform_cols(spark, create_clients_daily_table):
    """Test new columns added by pred_airflow_job.transform"""
    rfn_sdf, pars = pred_job.extract(
        spark,
        sub_date=MODEL_LAST_DAY.strftime(S3_DAY_FMT_DASH),
        param_bucket="dummy_bucket",
        param_prefix="dummy_prefix",
        model_win=MODEL_WINDOW,
        sample_ids=[1],
        first_dims=first_dims,
    )
    cols1 = set(rfn_sdf.columns)
    rfn2 = pred_job.transform(rfn_sdf, pars, return_preds=[7, 14, 21, 28])
    cols2 = set(rfn2.columns)

    new_cols = cols2 - cols1
    expected_new_cols = {
        "frequency",
        "recency",
        "num_opportunities",
        "min_day",
        "max_day",
        "e_total_days_in_next_7_days",
        "e_total_days_in_next_14_days",
        "e_total_days_in_next_21_days",
        "e_total_days_in_next_28_days",
        "prob_mau",
        "prob_active",
        "prob_daily_leave",
        "prob_daily_usage",
    }
    assert new_cols == expected_new_cols

    expected_removed_cols = {"Frequency", "Recency", "N", "Max_day", "Min_day"}
    removed_cols = cols1 - cols2
    assert removed_cols == expected_removed_cols, "Some columns renamed"


def test_fit_airflow_job_cli(spark, tmp_path, create_clients_daily_table):
    output = str(tmp_path)
    result = CliRunner().invoke(
        fit_job.main,
        [
            "--submission-date",
            HO_START.strftime(S3_DAY_FMT_DASH),
            "--sample-fraction",
            1.0,
            "--check-min-users",
            1,
            "--sample-ids",
            str(list(range(100))),
            "--bucket",
            output,
            "--prefix",
            "bgbb/params/v1",
            "--bucket-protocol",
            "file",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    df = spark.read.parquet(str(tmp_path / "bgbb/params/v1"))
    df.show()
    df.count() == 1


def test_pred_airflow_job_cli(spark, tmp_path, create_clients_daily_table):
    output = str(tmp_path)
    result = CliRunner().invoke(
        pred_job.main,
        [
            "--submission-date",
            HO_START.strftime(S3_DAY_FMT_DASH),
            "--pred-bucket",
            output,
            "--param-bucket",
            output,
            "--bucket-protocol",
            "file",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    df = spark.read.parquet(str(tmp_path / "bgbb/active_profiles/v1"))
    assert df.count() == N_CLIENTS_ALL
