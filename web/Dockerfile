FROM python:3.12-slim
WORKDIR /app
RUN pip install flask pymysql gunicorn
COPY app.py .
COPY templates/ templates/
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]