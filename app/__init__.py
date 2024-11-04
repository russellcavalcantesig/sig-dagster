import warnings
from dagster import Definitions, ExperimentalWarning, in_process_executor

warnings.filterwarnings("ignore", category=ExperimentalWarning)

# from .jobs import jobs_defs
from .resources.resources import resource_defs
from .scheduler.scheduler import schedules

from .jobs.audit.normalize_job import audit_normalize_job
from .jobs.audit.humanize_job import humanize_job

jobs_defs = [audit_normalize_job, humanize_job]

defs = Definitions(
    executor=in_process_executor,
    resources=resource_defs,
    jobs=jobs_defs,
    schedules=schedules,
)