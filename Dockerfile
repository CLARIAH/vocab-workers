FROM python:3.11-slim AS worker

ENV PYTHONPATH /app
ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /app/requirements.txt
RUN pip install --trusted-host pypi.python.org -r /app/requirements.txt

COPY vocab /app/vocab

CMD ["python", "/app/vocab/app.py", "worker"]

FROM worker AS flower

CMD ["python", "/app/vocab/app.py", "flower"]
