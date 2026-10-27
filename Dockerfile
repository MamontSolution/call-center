FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pymysql
COPY *.py *.html ./
COPY static ./static
ENV PYTHONUNBUFFERED=1 HOST=0.0.0.0
CMD ["python", "server.py", "/data/cf.db", "8000"]
