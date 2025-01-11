from rest_framework import status
from django.http import JsonResponse
from analytics.settings import  db
import pandas as pd

class DbHelper:

    def promo_data(self, start_timestamp, end_timestamp, service_type=0):
        try:
            assert False
            query = "SELECT promo_id, promo_name, promo_code, accounting, user_id " \
                    "FROM promo_consumption_history " \
                    "WHERE time_stamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
            if service_type:   query = query + "AND service_type == {}".format(service_type)
            result_data = sqlContext.sql(query)
            return result_data.toPandas()

        except:
            query = {"time_stamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
            if service_type:
                query["service_type"] = service_type
            params = {"promo_id": 1, "promo_name": 1, 'promo_code': 1, 'accounting': 1, "user_id": 1}
            print("query--->", query)
            result_data = pd.DataFrame(db.promo_consumption_history.find(query))
            return result_data
