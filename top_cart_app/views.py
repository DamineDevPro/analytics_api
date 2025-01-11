from django.shortcuts import render
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
import ast
from analytics.settings import  UTC, BASE_CURRENCY, _casandra
import requests
import json
import pytz
import traceback

class Cart(APIView):
    def get(self, request):
        try:
            # start_timestamp and end_timestamp in epoch(seconds)
            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "mandatory field 'start_timestamp' and 'end_timestamp' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {"message": "Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            start_date = str(datetime.fromtimestamp(start_timestamp, tz=UTC))
            end_data = str(datetime.fromtimestamp(end_timestamp, tz=UTC))
            # add on date time filter query
            date_query = "WHERE createdtimestamp BETWEEN '{start}' AND '{end}' ".format(start=start_date, end=end_data)

            # skip  and limit
            try:
                skip = int(request.GET.get("skip", 0))
                limit = int(request.GET.get("limit", 0))
            except:
                response = {"message": "skip and limit must be an integer value"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # store category id request to search the products
            # store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            # if store_categories_id != "0" and store_categories_id:
            #     store_categories_query = "AND storecategoryid == '{}' ".format(store_categories_id)

            # time_zone
            try:
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # sorting and ascending parameter
            try:
                sort = int(request.GET.get("sort", 1))  # {0: "Product Count", 1: "Date Time"}
                ascending = int(request.GET.get("ascending", 0))  # {0: "False", 1: "True"}
                if sort not in [0, 1] or ascending not in [0, 1]:
                    response = {"message": "unsupported sort or ascending, only support 0 or 1",
                                "sort support": {0: "Product Count", 1: "Date Time"},
                                "ascending support": {0: "False", 1: "True"}}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "sort and ascending must be integer",
                            "sort support": {0: "Product Count", 1: "Date Time"},
                            "ascending support": {0: "False", 1: "True"}}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            ascending = bool(ascending)

            addon_query = date_query + store_categories_query  # additional query's
            # spark sql query, to select all the variable for table sorted by time stamp
            query = "SELECT * from cartlogs {}ORDER BY createdtimestamp DESC".format(addon_query)
            try:
                assert False
                result_data = sqlContext.sql(query)  # data from spark sql context with respect to the query
                result_data = result_data.toPandas()  # converting the data in to pandas data set
            except:
                date_query = "WHERE createdtimestamp > '{start}' AND createdtimestamp < '{end}' ".format(
                    start=start_date, end=end_data)
                addon_query = date_query + store_categories_query  # additional query's
                query = "SELECT * from cartlogs {} ALLOW FILTERING".format(addon_query)
                query = query.replace("==", "=")
                query = query.replace("  ", " ")
                print("query ---------->", query)
                rows = _casandra.execute(query)
                result_data = pd.DataFrame(list(rows))
                result_data = result_data.sort_values(by="createdtimestamp", ascending=False).reset_index(drop=True)
            result_data["createdtimestamp"] = pd.to_datetime(result_data["createdtimestamp"])  # datetime convert
            grouped_df = result_data.loc[result_data.groupby(['cartid', "productname"]).createdtimestamp.idxmax()]  # group by cart id and product name which has latest time delta
            total_user_count = int(grouped_df.userid.nunique())  # calculating total user count
            aggregation = {'createdtimestamp': 'max', 'closingqty': 'sum'}
            response_data = grouped_df.groupby('productname').agg(aggregation).reset_index()
            response_data["createdtimestamp"] = response_data.createdtimestamp.dt.tz_localize('UTC').dt.tz_convert(time_zone)
            response_data["timestamp"] = response_data.createdtimestamp.values.astype(np.int64) // 10 ** 9
            response_data = response_data.rename(columns={"createdtimestamp": "datetime"})
            reverse_sort = {0: "closingqty", 1: "timestamp"}
            response_data = response_data.sort_values(by=reverse_sort[sort], ascending=ascending)
            total_product = int(response_data.productname.nunique())
            total_count = int(response_data.shape[0])
            response_data = response_data.to_dict(orient="records")[(skip * limit): (skip * limit) + limit]
            response = {'message': 'success',
                        'data': {"table": response_data, "total_product": total_product,
                                 "total_user": total_user_count, "total_count": total_count}}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
