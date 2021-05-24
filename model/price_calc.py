import sqlalchemy
import pandas as pd
import decimal

from typing import List


class PriceCalc(object):
    def __init__(self, req_params):
        self.request_params = req_params
        self.price_factors = ['Счет', 'Прайс', 'Другое', 'Средняя_цена_парсинг', 'Цена_входа_тек_месяц', 'Цена_входа_след_месяц', 'Цена_поручения', 'Себестоимость_запаса', 'Прайс_ЕМИ']

    def set_sql_response(self, sql_res:sqlalchemy.engine.result.RowProxy):
        """
        Фильтрует результаты sql выборки, записывает данные в 
        сыром виде и в pandas dataframe.

        sql_res: выборка из БД
        """
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

    def _apply_priplata(self, feature_name, price):
        """ Применяет приплату к расчетному показателю.
            Значение приплаты может быть как в %, так и в рублях,
            принимаем, что если приплата меньше 100, то это процент
        """
        feature_map = {'Счет': 'dop_rinok',
                    'Прайс': 'dop_rinok',
                    'Другое': 'dop_rinok',
                    'Средняя_цена_парсинг': 'dop_parsing',
                    'Цена_входа_тек_месяц': 'dop_vxod_1',
                    'Цена_входа_след_месяц': 'dop_vxod_2',
                    'Цена_поручения': 'dop_poruchenie',
                    'Себестоимость_запаса': 'dop_sebest',
                    'МРЦ':'dop_mrc',
                    'Установки': 'dop_ustanovki',                    
                    }

        feature_req_name = feature_map.get(feature_name)
        if feature_req_name:
            dop_priplata = float(self.request_params['priplati'][feature_req_name])
            if not(dop_priplata > 0) or dop_priplata > 100:
                return float(price) + dop_priplata
            else:
                return float(price)*dop_priplata
        else:
            return float(price)
         
    def calculate_row_price(self, row: dict) -> dict:
        """
        Считает расчетные цены для каждой позиции,выполнение (vip) считаем по подразеделениям,
        а kz1 и kz2 по позициям.
        
        row: ответ из базы с параметрами для расчета
        returns: словарь с расчетными параметрами

        """
        podr_perf = self.podr_perf.loc[row['Город']]
        vip = bool(podr_perf['vip1'])
        # kz1 = podr_perf['kz1'] если вдруг КЗ будем считать по подразделениям.
        # kz2 = podr_perf['kz2']

        kz1 = (row.get('Текущий_остаток_путь')/row.get('Прогноз_продаж')) >= 0.5 if all(
            [row.get('Текущий_остаток_путь'), row.get('Прогноз_продаж')]) else False
        kz2 = (row.get('Текущий_остаток_путь')/row.get('Прогноз_продаж')) >= 1 if all(
            [row.get('Текущий_остаток_путь'), row.get('Прогноз_продаж')]) else False
        # vip = (row.get('Прогноз_продаж')/row.get('План_продаж')) > 0.98 if all( если вдруг выполнение будем считать по позициям
        #     [row.get('Прогноз_продаж'), row.get('План_продаж')]) else False

        ustanovka = False
        # формируем массив факторов с учетом приплат
        price_factors = [self._apply_priplata(feature_name, row.get(feature_name)) for feature_name in self.price_factors if row.get(
            feature_name) is not None]
        min_factors = float(min(price_factors))
        max_factors = float(max(price_factors))
   
        # отдельно считаем мин и макс факторы без учета приплат (для наглядности)
        price_factors_clean = [row.get(feature_name) for feature_name in self.price_factors if row.get(
            feature_name) is not None]
        min_factors_clean = float(min(price_factors_clean))
        max_factors_clean = float(max(price_factors_clean))

        calc_res = dict(kz1=kz1, kz2=kz2, vip=vip,
                        min_factors=min_factors, max_factors=max_factors,
                        min_factors_clean=min_factors_clean, max_factors_clean=max_factors_clean)

        # определяем, в какой ситуации мы находимся, сперва рассматриваем граничные варианты
        if ustanovka:
           calc_res['pr_case'] = 1
           calc_res['c_price'] = 999999

        if not kz1:
            # второй сценарий, нет запаса совсем, берем максимальную цену среди факторов
            calc_res['pr_case'] = 2
            priplata = float(self.request_params['priplati']['kz_priplata_2'])
            if not(priplata > 0) or priplata > 100:
                calc_res['c_price'] = max_factors  + priplata
            else:
                calc_res['c_price'] = max_factors * priplata
        
        # if kz1 and market:
        #     pr_case = 5

        if kz1 and vip:
            calc_res['pr_case'] = 4
            priplata = float(self.request_params['priplati']['kz_priplata_1'])
            if not(priplata > 0) or priplata > 100:
                calc_res['c_price'] = max_factors  + priplata
            else:
                calc_res['c_price'] = max_factors * priplata

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
        """
        Расчитывает цены и выходные параметры по массиву выборки умного прайса.
        """
        self.calc_res = []
        for elem in self.sql_source_pvt_res:
            c_price = self.calculate_row_price(dict(elem))
            s = elem.copy()
            s.update(c_price)
            self.calc_res.append(s)
