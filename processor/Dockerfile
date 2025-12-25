FROM python:3.11-slim

WORKDIR /app

RUN pip install kafka-python

COPY processor.py .

CMD ["python", "processor.py"]
