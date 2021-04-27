from typing import List
from typing import Dict

from fastapi import FastAPI
from starlette.responses import RedirectResponse
from model.requests_models import PriceCalcParams
from model.price_calc import PriceCalc
from model.data_layer import read_sql_queries
from model.data_layer import get_sql_connection
from jinjasql import JinjaSql

import uvicorn

app = FastAPI()
queries = ["queries/hotline",
            "queries/parsing",
            "queries/stock_sales_seb",
            "queries/zakup_prices",
            "queries/emi_last_price",
            "queries/summary"]


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

@app.post("/test/echo")
def echo_json(req):
    return req.json



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1113)
