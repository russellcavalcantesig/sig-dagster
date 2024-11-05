# app/main.py

import uvicorn
import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
import pika
import aio_pika
from dagster import DagsterInstance
import subprocess
from .jobs.audit.normalize_job import audit_normalize_job
from .jobs.audit.humanize_job import humanize_job
from .config.dagster_config import setup_dagster_config

# Configurar Dagster antes de iniciar a aplicação
DAGSTER_HOME = setup_dagster_config()

async def send_to_rabbitmq(job_name: str, body:str):
    connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.17.106'))
    channel = connection.channel()
    channel.queue_declare(queue='EventTrigger')
    channel.basic_publish(exchange='', routing_key='EventTrigger', body=body)
    print(f" [x] Enviado pelo job de ->{job_name},\n doc -> {body}, \n'RabbitMQ!'")
    connection.close()

async def process_job(message: aio_pika.IncomingMessage):
    async with message.process():
        job_name = message.body.decode()
        if job_name == "mongo_normalize_job":
            print("Executando o job agendado...")
            instance = DagsterInstance.get()
            audit_normalize_job.execute_in_process(instance=instance)

async def start_worker():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("dagster_jobs", durable=True)
        await queue.consume(process_job)
        print("Worker RabbitMQ pronto para processar jobs...")
        await asyncio.Future()

def start_dagster_server():
    # Simplificando o comando do Dagster
    cmd = [
        "dagster",
        "dev",
        "-h", "0.0.0.0",
        "-p", "8100",
        # "-m", "app.jobs.normalize_job"  # Usando o módulo Python diretamente
    ]
    
    env = os.environ.copy()
    env["DAGSTER_HOME"] = DAGSTER_HOME
    
    process = subprocess.Popen(
        cmd,
        env=env,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Define o diretório de trabalho
    )
    
    print(f"Servidor Dagster iniciado na porta 8100 com DAGSTER_HOME={DAGSTER_HOME}")
    return process

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando o servidor FastAPI, worker RabbitMQ e servidor Dagster...")
    worker_task = asyncio.create_task(start_worker())
    dagster_process = start_dagster_server()
    yield
    worker_task.cancel()
    dagster_process.terminate()
    print("Encerrando o worker RabbitMQ e o servidor Dagster...")

app = FastAPI(lifespan=lifespan)

@app.post("/send-to-queue/")
async def run_job():
    await send_to_rabbitmq("mongo_audit_job")
    return {"message": "Job enfileirado para execução"}

@app.get("/run-job/")
async def run_job_direct():
    instance = DagsterInstance.get()
    result = audit_normalize_job.execute_in_process(instance=instance)
    return {"message": result.success}


@app.get("/humanize_job/")
async def run_job_direct():
    instance = DagsterInstance.get()
    result = humanize_job.execute_in_process(instance=instance)
    return {"message": result.success}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)