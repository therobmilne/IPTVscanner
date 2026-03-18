FROM python:3.12-slim

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app/ ./app/
COPY templates/ ./templates/
COPY run.py .
COPY list_categories.py .

# Create data dir
RUN mkdir -p /app/data

# Port 8888
EXPOSE 8888

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8888/api/status', timeout=5)" || exit 1

CMD ["python", "run.py", "dashboard", "--port", "8888"]
