from pydantic import BaseModel

class ReportRequest(BaseModel):
    query_date: str
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    HOST: str
    PORT: int
