FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hello.py .

EXPOSE 5000

CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 hello:app
