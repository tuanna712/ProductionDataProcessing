from fastapi import APIRouter
from .pgdb import generate_report
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
from io import StringIO
from app.schemas.pgsql import ReportRequest

router = APIRouter()

@router.post("/df")
def gen_df_report(request: ReportRequest):
    """
    Return dataframe as JSON
    """
    report_df: pd.DataFrame = generate_report(
        request.query_date,
        request.POSTGRES_DB,
        request.POSTGRES_USER,
        request.POSTGRES_PASSWORD,
        request.HOST,
        request.PORT
    )
    return JSONResponse(content=report_df.to_dict(orient="records"))

@router.post("/csv")
def gen_csv_report(request: ReportRequest):
    """
    Return dataframe as CSV file
    """
    report_df: pd.DataFrame = generate_report(
        request.query_date,
        request.POSTGRES_DB,
        request.POSTGRES_USER,
        request.POSTGRES_PASSWORD,
        request.HOST,
        request.PORT
    )

    stream = StringIO()
    report_df.to_csv(stream, index=False)
    response = StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv"
    )
    response.headers["Content-Disposition"] = "attachment; filename=daily_oil_report.csv"
    return response