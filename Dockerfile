# Use a imagem base do Python
FROM python:3.9

# Copiar arquivos do projeto para o contêiner
COPY . /app

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências
RUN pip install -r requirements.txt

# Expor a porta 8000 para o FastAPI
EXPOSE 8000

# Comando para rodar o FastAPI e o worker integrados
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
