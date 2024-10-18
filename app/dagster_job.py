from dagster import job, op

@op
def hello_op():
    print("Hello, Dagster job executed!")
    return "Hello, Dagster job executed!"

@job
def scheduled_job():
    hello_op()


jobs = []