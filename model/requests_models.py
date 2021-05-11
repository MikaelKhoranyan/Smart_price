from pydantic import BaseModel
from typing import List

import datetime as dt


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


class DopAddition(BaseModel):
    dop_sebest: float
    dop_indicativ: float
    dop_vxod_1: float
    dop_vxod_2: float
    dop_mrc: float
    dop_poruchenie: float
    dop_ustanovki: float
    dop_parsing: float
    dop_rinok: float
    kz_priplata_1: float
    kz_priplata_2: float


class PriceCalcParams(BaseModel):
    podr: str
    gfu: str
    emi_price_hotline: EmiHotlineParams
    parsing: ParsingParams
    p_zakup: PricesZakupParams
    priplati: DopAddition
