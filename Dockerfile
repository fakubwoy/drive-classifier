FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

ENV PYTHONUNBUFFERED=1
ENV EXCEL_PATH=/app/Query_sheet_alfaleus.xlsx

CMD ["python", "app.py"]
