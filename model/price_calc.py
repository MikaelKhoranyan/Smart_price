import sqlalchemy
import pandas as pd

from typing import List


class PriceCalc(object):
    def __init__(self, req_params):
        self.request_params = req_params
        self.price_factors = ['Счет', 'Прайс', 'Другое', 'Средняя_цена_парсинг', 'Цена_входа_тек_месяц', 'Цена_входа_след_месяц', 'Цена_поручения', 'Себестоимость_запаса']

    def set_sql_response(self, sql_res):
        # фильтруем строки, по котрым нет хотя бы одной цены
        filered_sql_data = []
        for x in sql_res:
            x = dict(x)
            for f in self.price_factors:
                if x.get(f):
                    filered_sql_data.append(x)
                    break
        self.sql_source_pvt_res: List[sqlalchemy.engine.result.RowProxy] = filered_sql_data
        self.sql_response_df: pd.DataFrame = pd.DataFrame(
            self.sql_source_pvt_res, columns=self.sql_source_pvt_res[0].keys())

    def set_podr_params(self):
        """
        Считаем показатели по подразделениям

        1. Выполнение плана > 98% (vip1)
        2. КЗ > 0.5 (kz1)
        3. КЗ > 1 (kz2)

        """
        df = self.sql_response_df
        plan_vip = df.groupby('Город')['Прогноз_продаж'].sum(
        )/df.groupby('Город')['План_продаж'].sum()  # выполнение планов
        kz = df.groupby('Город')['Прогноз_продаж'].sum(
        )/df.groupby('Город')['Текущий_остаток_путь'].sum()  # коэффициент_запаса
        self.podr_perf = pd.DataFrame(dict(vip1=plan_vip > 0.98, kz1=kz > 0.5, kz2=kz > 1))

    def calculate_row_price(self, row) -> dict:
        """
        Пока предполагаем, что КЗ и Выполнение считаем по подразеделения, а не по позициям.
        """
        # podr_perf = self.podr_perf.iloc[row['Город']]
        # kz1 = podr_perf['kz1']
        # kz2 = podr_perf['kz2']
        # vip = podr_perf['vip1']

        kz1 = (row.get('Текущий_остаток_путь')/row.get('Прогноз_продаж')) >= 0.5 if all(
            [row.get('Текущий_остаток_путь'), row.get('Прогноз_продаж')]) else False
        kz2 = (row.get('Текущий_остаток_путь')/row.get('Прогноз_продаж')) >= 1 if all(
            [row.get('Текущий_остаток_путь'), row.get('Прогноз_продаж')]) else False
        vip = (row.get('Прогноз_продаж')/row.get('План_продаж')) > 0.98 if all(
            [row.get('Прогноз_продаж'), row.get('План_продаж')]) else False

        ustanovka = False

        # сразу считаем мин и макс по факторам
        price_factors = [row.get(feature_name) for feature_name in self.price_factors if row.get(
            feature_name) is not None]
        min_factors = float(min(price_factors))
        max_factors = float(max(price_factors))

        calc_res = dict(kz1=kz1, kz2=kz2, vip=vip,
                        min_factors=min_factors, max_factors=max_factors)

        # определяем, в какой ситуации мы находимся, сперва рассматриваем граничные варианты
        if ustanovka:
           calc_res['pr_case'] = 1
           calc_res['c_price'] = 999999

        if not kz1:
            # второй сценарий, нет запаса совсем, берем максимальную цену среди факторов
            calc_res['pr_case'] = 2
            # + priplata_2 тут еще должна быть приплата
            calc_res['c_price'] = max_factors

        # if kz1 and market:
        #     pr_case = 5

        if kz1 and vip:
            calc_res['pr_case'] = 4
            # + priplata_1 тут еще должна быть приплата
            calc_res['c_price'] = max_factors

        if kz2 and vip:
            calc_res['pr_case'] = 7
            calc_res['c_price'] = max_factors

        if kz1 and not vip:
            calc_res['pr_case'] = 3
            # + priplata_1 тут еще должна быть приплата
            calc_res['c_price'] = max_factors

        if kz2 and not vip:
            calc_res['pr_case'] = 6
            calc_res['c_price'] = min_factors

        return calc_res

    def get_calc_price(self):
        return self.calc_res

    def calculate_prices(self):
        self.calc_res = []
        for elem in self.sql_source_pvt_res:
            c_price = self.calculate_row_price(dict(elem))
            s = elem.copy()
            s.update(c_price)
            self.calc_res.append(s)
