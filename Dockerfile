# Dockerfile
FROM python:3.9-slim
WORKDIR /app
RUN pip install faker
COPY . /app
CMD ["python", "log_generator.py"]