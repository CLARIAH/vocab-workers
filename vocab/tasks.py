import enrich
from app import app


@app.task
def enrich_rdf(id):
    enrich_with_lov.delay(id)
    enrich_with_summarizer.delay(id)


@app.task
def enrich_with_lov(id):
    enrich.lov(id)


@app.task
def enrich_with_summarizer(id):
    enrich.summarizer(id)
