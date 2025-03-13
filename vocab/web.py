from fastapi import FastAPI
from vocab.pipeline import run_pipeline_with_record

app = FastAPI()

@app.post("/trigger/{nr}")
async def call_pipeline(nr: int):
    run_pipeline_with_record(nr)
    return {"status": 200}
