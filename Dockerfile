FROM python:3.12-slim

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1
ENV DATABASE_PATH=/app/data/market.db
ENV MODEL_CONFIG_PATH=/app/config/model_agents.yaml

RUN mkdir -p /app/data

EXPOSE 8080
CMD ["python3", "app.py", "--host", "0.0.0.0", "--port", "8080"]
