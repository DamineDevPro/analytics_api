from django.shortcuts import render
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
import ast
from analytics.settings import UTC, _casandra
import requests
import json
# import pyspark.sql.functions as f
import pytz
import traceback


class WishList(APIView):
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
            date_query = "WHERE createdTimeStamp BETWEEN '{start}' AND '{end}' ".format(start=start_date, end=end_data)

            # time_zone
            try:
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # skip  and limit
            try:
                skip = int(request.GET.get("skip", 0))
                limit = int(request.GET.get("limit", 0))
            except:
                response = {"message": "skip and limit must be an integer value"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # store categories
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = "AND storecategoryid == '{}' ".format(store_categories_id)

            # store id
            store_id = str(request.GET.get("store_id", ""))
            store_id_query = ""
            if store_id != "0" and store_id:
                store_id_query = "AND storeid == '{}' ".format(store_id)

            try:
                sort = int(request.GET.get("sort", 1))
                ascending = int(request.GET.get("ascending", 0))
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

            addon_query = date_query + store_categories_query + store_id_query

            query = "SELECT * from favouriteproductsuserwise {}ORDER BY createdtimestamp DESC".format(addon_query)
            try:
                assert False
                result_sql_df = sqlContext.sql(query)
                result_data = result_sql_df.toPandas()
            except:
                date_query = "WHERE createdTimeStamp > '{start}' AND createdTimeStamp < '{end}' ".format(start=start_date,
                                                                                                         end=end_data)
                addon_query = date_query + store_categories_query + store_id_query
                query = "SELECT * from favouriteproductsuserwise {}".format(addon_query)
                query = query + " ALLOW FILTERING"
                query = query.replace("==", "=")
                print("query ---------->", query)
                rows = _casandra.execute(query)
                result_data = pd.DataFrame(list(rows))
            if not result_data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
            result_data = result_data.sort_values(by="createdtimestamp", ascending=False).reset_index(drop=True)
            product = result_data[["parentproductid", "productname"]].drop_duplicates()
            print("product----------->")
            print(product)
            try:
                assert False
                wishlist = result_sql_df.groupBy(["parentproductid"]). \
                    agg(f.countDistinct('userid'), f.max('createdtimestamp')).orderBy(
                    "max(createdtimestamp)", ascending=False).toPandas()
            except:
                wishlist = result_data.groupby("parentproductid").agg(
                    {"createdtimestamp": max, "userid": 'nunique'}
                ).sort_values(by="createdtimestamp", ascending=False).reset_index()
                rename = {"createdtimestamp": "max(createdtimestamp)", "userid": "count(DISTINCT userid)"}
                wishlist = wishlist.rename(columns=rename)
                print(wishlist)
            wishlist = pd.merge(wishlist, product, on="parentproductid", how="left")
            try:
                wishlist['productname'] = wishlist['productname'].apply(lambda x: ast.literal_eval(x))
                wishlist['productname'] = wishlist['productname'].apply(lambda x: x['en'])
            except:
                pass
            wishlist['max(createdtimestamp)'] = wishlist['max(createdtimestamp)'].dt.tz_localize('UTC').dt.tz_convert(
                time_zone)
            total_product = len(result_data["parentproductid"].unique())

            total_wishlist = len(result_data["userid"].unique())
            total_count = total_wishlist
            sort_col = {0: "count(DISTINCT userid)", 1: "max(createdtimestamp)"}
            wishlist = wishlist.sort_values(by=sort_col[sort], ascending=bool(ascending))
            wishlist = wishlist.rename(columns={"count(DISTINCT userid)": "count(parentproductid)"})
            wishlist = wishlist.to_dict(orient='record')[(skip * limit): (skip * limit) + limit]
            response = {'message': 'success', 'data': wishlist, "total_product": total_product,
                        "total_wishlist": total_wishlist, "total_count": total_count}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)

