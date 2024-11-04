# from dagster import ScheduleDefinition, repository
# from app.dagster_job import mongo_normalize_job

# # # Definir o agendamento para rodar todos os dias às 21h
# # schedule = ScheduleDefinition(
# #     job=mongo_audit_job,
# #     cron_schedule="0 21 * * *"  # Todos os dias às 21h
# # )

# # Definir o agendamento para rodar a cada 1 minuto
# schedule = ScheduleDefinition(
#     job=mongo_normalize_job,
#     cron_schedule="* * * * *"  # A cada minuto
# )

# @repository
# def dagster_repo():
#     return [mongo_normalize_job, schedule]



from dagster import schedule
from ..jobs.audit.normalize_job import audit_normalize_job
from ..jobs.audit.humanize_job import humanize_job

@schedule(
    cron_schedule="* * * * *",  # Executa diariamente à meia-noite
    job=audit_normalize_job,
    execution_timezone="UTC",
)
def daily_normalize_schedule(context):
    return {}
@schedule(
    cron_schedule="* * * * *",  # Executa diariamente à meia-noite
    job=humanize_job,
    execution_timezone="UTC",
)
def daily_humanized_schedule(context):
    return {}

schedules = [daily_normalize_schedule, daily_humanized_schedule]