FROM python:3.9-slim

RUN apt-get update && apt-get install -y libaio1 libgomp1

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app/main.py"]
