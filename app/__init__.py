import warnings

from dagster import Definitions, ExperimentalWarning, in_process_executor

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from app.assets import assets
from app.dagster_job import jobs
# from esaj_dagster.resources import resource_defs
from app.scheduler import schedule

defs = Definitions(
    executor=in_process_executor,
    assets=assets,
    # resources=resource_defs,
    jobs=[*jobs],
    schedules=[schedule],
)