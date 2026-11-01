# Built from the project root (see docker-compose.yml), because the Streamlit
# theme file .streamlit/config.toml lives at the root, where Streamlit Cloud expects it.
FROM python:3.12-slim
WORKDIR /app
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 --retries 10 -r requirements.txt
COPY .streamlit ./.streamlit
COPY dashboard/*.py ./
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
