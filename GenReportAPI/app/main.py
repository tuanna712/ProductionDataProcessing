from fastapi import FastAPI
from app.api import pgsql

app = FastAPI(title="Daily Oil Report API")

# Register routers
app.include_router(pgsql.router, prefix="/report", tags=["Oil Production Report"])

@app.get("/")
def root():
    return {"message": "Automatic Daily Oil Production Reporting API"}