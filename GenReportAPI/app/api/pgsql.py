from fastapi import APIRouter
from .pgdb import generate_oil_report_w_latest_data, generate_gas_report_w_latest_data
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
from io import StringIO
from app.schemas.pgsql import ReportRequest

router = APIRouter()

@router.post("/oilreport")
def gen_df_oil_report(request: ReportRequest):
    report_df: pd.DataFrame = generate_oil_report_w_latest_data(
        request.query_date,
        request.POSTGRES_DB,
        request.POSTGRES_USER,
        request.POSTGRES_PASSWORD,
        request.HOST,
        request.PORT
    )
    return JSONResponse(content=report_df.to_dict(orient="records"))

@router.post("/gasreport")
def gen_df_gas_report(request: ReportRequest):
    report_df: pd.DataFrame = generate_gas_report_w_latest_data(
        request.query_date,
        request.POSTGRES_DB,
        request.POSTGRES_USER,
        request.POSTGRES_PASSWORD,
        request.HOST,
        request.PORT
    )
    return JSONResponse(content=report_df.to_dict(orient="records"))

# @router.post("/oilcsv")
# def gen_csv_report(request: ReportRequest):
#     """
#     Return dataframe as CSV file
#     """
#     report_df: pd.DataFrame = generate_oil_report(
#         request.query_date,
#         request.POSTGRES_DB,
#         request.POSTGRES_USER,
#         request.POSTGRES_PASSWORD,
#         request.HOST,
#         request.PORT
#     )

#     stream = StringIO()
#     report_df.to_csv(stream, index=False)
#     response = StreamingResponse(
#         iter([stream.getvalue()]),
#         media_type="text/csv"
#     )
#     response.headers["Content-Disposition"] = "attachment; filename=daily_oil_report.csv"
#     return response