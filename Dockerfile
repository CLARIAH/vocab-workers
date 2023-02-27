FROM python:3.11-slim

ENV PYTHONPATH /app
ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /app/requirements.txt
RUN pip install --trusted-host pypi.python.org -r /app/requirements.txt

COPY ./vocab_workers /app/vocab_workers

CMD ["python", "/app/vocab_workers/app.py"]
