import sys
sys.path.append('././')


from model.price_calc import PriceCalc
from model.data_layer import read_sql_queries
from model.data_layer import get_sql_connection
from jinjasql import JinjaSql
import unittest


class testBaseAlg(unittest.TestCase):
    def setUp(self):
        self.calc_obj = PriceCalc(req_params=dict())

    def testalg_2_1(self):
        t_row = {'План_продаж': 22.6,
                 'Прогноз_продаж': 16,
                 'Текущий_остаток_путь': 0.0,
                 'Себестоимость_запаса': None,
                 'Счет': None,
                 'Прайс': None,
                 'Другое': None,
                 'Средняя_цена_парсинг': 62579,
                 'Цена_входа_тек_месяц': 60852,
                 'Цена_входа_след_месяц': 60852,
                 'Цена_поручения': None
                 }

        res = self.calc_obj.calculate_row_price(t_row)
        self.assertTrue(
            all([res['pr_case'] == 2, round(res['c_price'], 0) == 62579]))

    def testalg_7_1(self):
        t_row = {'План_продаж': 200,
                 'Прогноз_продаж': 500,
                 'Текущий_остаток_путь': 1000,
                 'Себестоимость_запаса': 58000,
                 'Счет': 59000,
                 'Прайс': None,
                 'Другое': None,
                 'Средняя_цена_парсинг': 61500,
                 'Цена_входа_тек_месяц': 61000,
                 'Цена_входа_след_месяц': 62000,
                 'Цена_поручения': None
                 }

        res = self.calc_obj.calculate_row_price(t_row)
        self.assertTrue(
            all([res['pr_case'] == 7, round(res['c_price'], 0) == 62000]))

    def testalg_6_1(self):
        t_row = {'План_продаж': 700,
                 'Прогноз_продаж': 500,
                 'Текущий_остаток_путь': 1000,
                 'Себестоимость_запаса': 58000,
                 'Счет': 59000,
                 'Прайс': None,
                 'Другое': None,
                 'Средняя_цена_парсинг': 61500,
                 'Цена_входа_тек_месяц': 61000,
                 'Цена_входа_след_месяц': 62000,
                 'Цена_поручения': None
                 }

        res = self.calc_obj.calculate_row_price(t_row)
        self.assertTrue(
            all([res['pr_case'] == 6, round(res['c_price'], 0) == 58000]))

    def testalg_3_1(self):
        t_row = {'План_продаж': 700,
                 'Прогноз_продаж': 500,
                 'Текущий_остаток_путь': 300,
                 'Себестоимость_запаса': 58000,
                 'Счет': 59000,
                 'Прайс': None,
                 'Другое': None,
                 'Средняя_цена_парсинг': 61500,
                 'Цена_входа_тек_месяц': 61000,
                 'Цена_входа_след_месяц': 62000,
                 'Цена_поручения': None
                 }

        res = self.calc_obj.calculate_row_price(t_row)
        self.assertTrue(
            all([res['pr_case'] == 3, round(res['c_price'], 0) == 62000]))

    def testalg_4_1(self):
        t_row = {'План_продаж': 500,
                 'Прогноз_продаж': 500,
                 'Текущий_остаток_путь': 380,
                 'Себестоимость_запаса': 58000,
                 'Счет': 59000,
                 'Прайс': None,
                 'Другое': None,
                 'Средняя_цена_парсинг': 61500,
                 'Цена_входа_тек_месяц': 61000,
                 'Цена_входа_след_месяц': 62000,
                 'Цена_поручения': None
                 }

        res = self.calc_obj.calculate_row_price(t_row)
        self.assertTrue(
            all([res['pr_case'] == 4, round(res['c_price'], 0) == 62000]))


class testCalc(unittest.TestCase):
    def setUp(self):

        queries = ["queries/hotline", "queries/parsing", "queries/stock_sales_seb", "queries/zakup_prices", "queries/emi_last_price", "queries/summary"
                   ]

        conc_list = ['Металлсервис', 'Металлоторг', 'СПК', 'А Групп']
        partners = ['ММК ПАО']

        params = {
            'div': 'Урал',
            'gfu': '05 Лист ГК',
            'nds_rate': 1.2,
            'emi_price_hotline': {'per_start': '20210401',
                                  'per_end': '20210414',
                                  'comp': conc_list,
                                  },
            'parsing': {'per_start': '20210401',
                        'per_end': '20210414',
                        'comp': conc_list,
                        },
            'p_zakup': {'partners': partners,
                        'per_dates': ['20210401', '20210501'],

                        }



        }

        query_text = read_sql_queries(queries)
        j = JinjaSql()
        query, bind_params = j.prepare_query(query_text, params)

        self.engine = get_sql_connection()
        sql_res = self.engine.execute(query, bind_params).fetchall()
        self.calc_obj = PriceCalc(req_params=dict(params))
        self.calc_obj.set_sql_response(sql_res)
        self.calc_obj.calculate_prices()

    def test1(self):
        c = self.calc_obj.get_calc_price()
        with open('test1', 'w') as f:
            f.write(str(c))


if __name__ == '__main__':
    unittest.main()
