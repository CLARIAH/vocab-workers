import cache
import enrich
import create

from app import app


@app.task
def run_for(id):
    cache_files.delay(id)
    enrich_rdf.delay(id)
    create_documentation.delay(id)


@app.task
def cache_files(id):
    cache.cache_files(id)


@app.task
def enrich_rdf(id):
    enrich_with_lov.delay(id)
    # enrich_with_summarizer.delay(id)


@app.task
def enrich_with_lov(id):
    enrich.lov(id)


@app.task
def enrich_with_summarizer(id):
    enrich.summarizer(id)


@app.task
def create_documentation(id):
    create.create_documentation(id)
