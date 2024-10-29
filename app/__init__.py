import warnings
from dagster import Definitions, ExperimentalWarning, in_process_executor

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .jobs import jobs_defs
from .resources.resources import resource_defs
from .scheduler.scheduler import schedules

defs = Definitions(
    executor=in_process_executor,
    resources=resource_defs,
    jobs=jobs_defs,
    schedules=schedules,
)