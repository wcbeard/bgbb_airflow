import click
import numba
print('numba==', numba.__version__)

import bgbb
print('bgbb==', bgbb.__version__)

from bgbb_airflow import (
    fit_airflow_job as bgbb_fit_job,
    pred_airflow_job as bgbb_pred_job,
)

print("in cli.py")


@click.group()
def entry_point():
    pass


entry_point.add_command(bgbb_fit_job.main, "bgbb_fit")
entry_point.add_command(bgbb_pred_job.main, "bgbb_pred")


if __name__ == "__main__":
    # main(auto_envvar_prefix='MOZETL')
    entry_point()
