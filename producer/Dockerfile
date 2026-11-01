FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 --retries 10 -r requirements.txt
COPY *.py ./
# -u = unbuffered output so we see the logs straight away
CMD ["python", "-u", "producer.py"]
