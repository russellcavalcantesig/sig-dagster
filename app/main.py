from fastapi import FastAPI
import aio_pika
import asyncio
from contextlib import asynccontextmanager
from app.dagster_job import scheduled_job

# Função para enviar job para a fila RabbitMQ
async def send_to_rabbitmq(job_name: str):
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("dagster_jobs", durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=job_name.encode()),
            routing_key=queue.name,
        )
        print(f"Job {job_name} enviado para RabbitMQ")

# Worker para processar jobs no RabbitMQ
async def process_job(message: aio_pika.IncomingMessage):
    async with message.process():
        job_name = message.body.decode()
        if job_name == "scheduled_job":
            print("Executando o job agendado...")
            scheduled_job.execute_in_process()

# Função para iniciar o worker RabbitMQ
async def start_worker():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("dagster_jobs", durable=True)
        await queue.consume(process_job)
        print("Worker RabbitMQ pronto para processar jobs...")
        await asyncio.Future()  # Mantém o worker rodando

# Contexto de lifespan para controlar a inicialização e encerramento
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando o servidor FastAPI e o worker RabbitMQ...")
    worker_task = asyncio.create_task(start_worker())  # Inicia o worker paralelo
    yield  # Permite que a aplicação inicie durante o lifespan
    worker_task.cancel()  # Cancela o worker quando a aplicação for encerrada
    print("Encerrando o worker RabbitMQ...")

# Definindo a aplicação FastAPI com o lifespan
app = FastAPI(lifespan=lifespan)

# Rota para disparar o job manualmente
@app.post("/run-job/")
async def run_job():
    # Aqui enfileiramos o job no RabbitMQ
    await send_to_rabbitmq("scheduled_job")
    return {"message": "Job enfileirado para execução"}

# Opcional: Função para rodar o job diretamente pelo FastAPI
@app.get("/run-job-direct/")
async def run_job_direct():
    result = scheduled_job.execute_in_process()
    return {"message": result.success}
