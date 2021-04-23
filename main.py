from typing import List
from typing import Dict

from fastapi import FastAPI
from starlette.responses import RedirectResponse
from pydantic import BaseModel

from model.price_calc import PriceCalc
from model.data_layer import read_sql_queries
from model.data_layer import get_sql_connection
from jinjasql import JinjaSql

import uvicorn
import datetime as dt

app = FastAPI()
queries = ["queries/hotline",
            "queries/parsing",
            "queries/stock_sales_seb",
            "queries/zakup_prices",
            "queries/emi_last_price",
            "queries/summary"]


class ParsingParams(BaseModel):
    per_start: dt.date
    per_end: dt.date
    comp_list: List[str]


class EmiHotlineParams(BaseModel):
    per_start: dt.date
    per_end: dt.date
    comp_list: List[str]


class PricesZakupParams(BaseModel):
    partners: List[str]
    per_dates: List[dt.date]


class PriceCalcParams(BaseModel):
    div: str
    gfu: str
    emi_price_hotline: EmiHotlineParams
    parsing: ParsingParams
    p_zakup: PricesZakupParams


@app.get("/")
def read_root():
    response = RedirectResponse(url="/docs")
    return response


@app.post("/test/pricelist", response_model=List[Dict])
def calculate_price(c_params: PriceCalcParams):
    query_text = read_sql_queries(queries)
    j = JinjaSql()
    params = c_params.dict()
    params.update({'nds_rate': 1.2})
    query, bind_params = j.prepare_query(query_text, params)
    engine = get_sql_connection()
    sql_res = engine.execute(query, bind_params).fetchall()
    calc_obj = PriceCalc(req_params=params)
    calc_obj.set_sql_response(sql_res)
    calc_obj.calculate_prices()
    return calc_obj.get_calc_price()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1113)
