import uvicorn
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import pika
import aio_pika
from dagster import DagsterInstance
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load import load_workspace_process_context_from_yaml_paths
import subprocess
from app.dagster_job import mongo_normalize_job

# Função para enviar job para a fila RabbitMQ
async def send_to_rabbitmq(job_name: str):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='send_log_humanized')
    body = 'Mensagem padrao imst group!'
    channel.basic_publish(exchange='', routing_key='send_log_humanized', body=body)
    print(f" [x] Enviado {body}, 'RabbitMQ!'")
    connection.close()

# Worker para processar jobs no RabbitMQ
async def process_job(message: aio_pika.IncomingMessage):
    async with message.process():
        job_name = message.body.decode()
        if job_name == "mongo_audit_job":
            print("Executando o job agendado...")
            mongo_audit_job.execute_in_process()

# Função para iniciar o worker RabbitMQ
async def start_worker():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("dagster_jobs", durable=True)
        await queue.consume(process_job)
        print("Worker RabbitMQ pronto para processar jobs...")
        await asyncio.Future()

# Função para iniciar o servidor Dagster
def start_dagster_server():
    # Inicia o servidor Dagster em um processo separado
    process = subprocess.Popen(["dagster", "dev", "-h", "0.0.0.0", "-p", "8100"])
    print("Servidor Dagster iniciado na porta 3000")
    return process

# Contexto de lifespan para controlar a inicialização e encerramento
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando o servidor FastAPI, worker RabbitMQ e servidor Dagster...")
    worker_task = asyncio.create_task(start_worker())
    dagster_process = start_dagster_server()
    yield
    worker_task.cancel()
    dagster_process.terminate()
    print("Encerrando o worker RabbitMQ e o servidor Dagster...")

# Definindo a aplicação FastAPI com o lifespan
app = FastAPI(lifespan=lifespan)

# Rota para disparar o job manualmente
@app.post("/send-to-queue/")
async def run_job():
    await send_to_rabbitmq("mongo_audit_job")
    return {"message": "Job enfileirado para execução"}

# Opcional: Função para rodar o job diretamente pelo FastAPI
@app.get("/run-job/")
async def run_job_direct():
    result = mongo_audit_job.execute_in_process()
    return {"message": result.success}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)