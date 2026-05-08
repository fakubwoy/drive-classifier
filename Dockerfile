FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn gevent

COPY . .

EXPOSE 8080

ENV PYTHONUNBUFFERED=1
ENV EXCEL_PATH=/app/Query_sheet_alfaleus.xlsx

CMD gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --worker-class gevent --workers 1 --timeout 600 --graceful-timeout 30