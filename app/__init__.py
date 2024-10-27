import warnings

from dagster import Definitions, ExperimentalWarning, in_process_executor

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from app.assets import assets
from app.dagster_job import jobs
from .resources import mongodb_resource
from app.scheduler import schedule

defs = Definitions(
    executor=in_process_executor,
    assets=assets,
    resources={
        "mongodb": mongodb_resource
    },
    jobs=[*jobs],
    schedules=[schedule],
)