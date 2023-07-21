import enrich
import create
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


@app.task
def create_documentation(id):
    create.create_documentation(id)
