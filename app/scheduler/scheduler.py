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
from ..jobs.normalize_job import mongo_audit_job

@schedule(
    cron_schedule="* * * * *",  # Executa diariamente à meia-noite
    job=mongo_audit_job,
    execution_timezone="UTC",
)
def daily_audit_schedule(context):
    return {}

schedules = [daily_audit_schedule]