FROM python:3.9-slim

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y default-libmysqlclient-dev build-essential
RUN pip install --no-cache-dir mysql-connector-python

CMD ["python", "main.py"]