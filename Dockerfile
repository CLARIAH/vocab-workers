FROM python:3.12-slim AS worker

ENV PYTHONPATH /app
ENV PYTHONUNBUFFERED 1

RUN pip3 install poetry

WORKDIR /app
COPY pyproject.toml /app

RUN poetry config virtualenvs.create false && \
    poetry install

COPY vocab /app/vocab

CMD ["python", "/app/vocab/app.py", "worker"]

FROM worker AS flower

CMD ["python", "/app/vocab/app.py", "flower"]
