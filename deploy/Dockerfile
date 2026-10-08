FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[ingest]"
ENV CNPJ_DATA_DIR=/data
CMD ["python", "-c", "import time; time.sleep(10**9)"]
