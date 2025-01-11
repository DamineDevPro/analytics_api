import re
import sys
import ast
import json
import pytz
import requests
import numpy as np
import pandas as pd
from rest_framework import status
from django.http import JsonResponse
from analytics.function import Process
from rest_framework.views import APIView
from analytics.settings import  db
from datetime import datetime, timedelta, date
import traceback


class Map(APIView):
    def get(self, request):
        try:
            """
            GET API: Heat MAP api
            
            :param store_id: Mongo Id store id
            :param store_categories_id: Mongo Id store_categories_id
            :param timezone: timezone
            :param group_by: int
            :param start_timestamp: int
            :param end_timestamp: int
            :param zone_id: Mongo Id
            :param city_id: Mongo Id
            :param country_id: Mongo Id
            :param status_code: status of an order
            
            :return: Json Response data
            """
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            # Store Id param check - mandatory field
            try:
                store_id = str(request.GET['store_id'])
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_id = "" if store_id == "0" else store_id

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = store_categories_id if store_categories_id != "0" else ""

            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month",
                              4: "quarter", 5: 'year', 6: "hour_of_day", 7: "day_of_week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                assert group_by_value[group_by]
            except:
                return JsonResponse({"message": "'group_by' must be integer", "support": group_by_value},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

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

            # search query with respect to city_id, country_id and zone_id
            zone_id = request.GET.get("zone_id", "").strip()
            city_id = request.GET.get("city_id", "").strip()
            country_id = request.GET.get("country_id", "").strip()
            status_code = int(request.GET.get("status_code", 0))

            # Spark SQL query construction
            query = "SELECT * from storeOrder"
            query = query + " WHERE createdTimeStamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
            if zone_id: query = query + "AND deliveryAddress.zoneId == '{}' ".format(zone_id)
            if city_id: query = query + "AND deliveryAddress.cityId == '{}' ".format(city_id)
            if country_id: query = query + "AND deliveryAddress.countryId == '{}' ".format(country_id)
            if store_categories_id: query = query + "AND storeCategoryId == '{}' ".format(store_categories_id)
            try:
                assert False
                query = query.strip()
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if zone_id: query["deliveryAddress.zoneId"] = zone_id
                if city_id: query["deliveryAddress.cityId"] = city_id
                if country_id: query["deliveryAddress.countryId"] = country_id
                if store_categories_id: query = query["storeCategoryId"] = storeCategoryId

                result_data = pd.DataFrame(db.storeOrder.find(query))
            print("Shape of Data Set", result_data.shape)
            if not result_data.shape[0]:
                response = {'message': 'No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            columns = ["_id", "createdDate", "masterOrderId", "parentOrderId", "storeCategoryId",
                       "deliveryAddress", "status"]
            data_frame = result_data[columns]
            data_frame["status_code"] = data_frame["status"].apply(lambda x: x["status"])
            data_frame['lat'] = data_frame["deliveryAddress"].apply(lambda x: x["latitude"])
            data_frame['lng'] = data_frame["deliveryAddress"].apply(lambda x: x["longitude"])

            if status_code:
                heat_location = data_frame[data_frame.status_code == status_code][['lat', 'lng']]
            else:
                heat_location = data_frame[['lat', 'lng']]
            # ---------- Centroid ----------
            centroid = {"lat": heat_location["lat"].mean(), "lng": heat_location["lng"].mean()}
            # ---------- intensity ----------
            heat_location["intensity"] = 1
            heat_location = heat_location.groupby(by=["lat", "lng"]).sum().reset_index()
            heat_location = heat_location.to_dict(orient="records")

            data_frame["status_text"] = data_frame["status"].apply(lambda x: x["statusText"])
            table_df = data_frame[["createdDate", "status_text"]]
            status_text = pd.get_dummies(table_df["status_text"])
            column_rename = {}
            for col in status_text.columns:
                column_rename[col] = col.replace("status_text", "")
            status_text = status_text.rename(columns=column_rename)
            table_df = pd.concat([table_df.drop("status_text", axis=1), status_text], axis=1)
            table_df["dateTime"] = table_df.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            # date filler (add missing date time in an data frame)
            data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                       time_zone=time_zone, data_frame=table_df, date_column="dateTime",
                                       group_by=group_by)

            # date converter as per group by
            data = Process.date_conversion(group_by=group_by, data_frame=data, date_col='dateTime')
            data = data.groupby(['dateTime']).sum().reset_index()

            # data frame sorting
            data = data.sort_values(by='dateTime', ascending=True)
            if group_by == 3: data = Process.month_sort(data_frame=data, date_column="dateTime")
            if group_by == 4: data = Process.quarter_sort(data_frame=data, date_column="dateTime")
            if group_by == 7: data = Process.day_sort(data_frame=data, date_column="dateTime")

            graph = {
                "xaxis": {"title": group_by_value[group_by],
                          'categories': list(data['dateTime'].astype(str))},
                "yaxis": {"title": "Status Count"}
            }
            data_columns = list(data.columns)
            data_columns.remove('dateTime')
            series = [{"name": col, "data": list(data[col])} for col in data_columns]
            graph["series"] = series

            tabular_df = pd.DataFrame(result_data["masterOrderId"])
            tabular_df['name'] = result_data["deliveryAddress"].apply(lambda x: x["name"])
            tabular_df['zoneId'] = result_data["deliveryAddress"].apply(lambda x: x["zoneId"])
            tabular_df['cityName'] = result_data["deliveryAddress"].apply(lambda x: x.get("cityName", ""))
            # tabular_df['cityName'] = result_data["deliveryAddress"].apply(lambda x: x["cityName"] if x["cityName"] else "")
            tabular_df['countryName'] = result_data["deliveryAddress"].apply(lambda x: x.get("countryName", ""))
            tabular_df['state'] = result_data["deliveryAddress"].apply(lambda x: x.get("state", ""))
            tabular_df['taxableAmount'] = result_data["accounting"].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]))
            tabular_df = tabular_df.to_dict(orient="records")
            data = {"graph": graph,
                    "heatmap": heat_location,
                    "centroid": centroid,
                    "table": tabular_df}
            response = {"message": "success", "data": data}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class Countries(APIView):
    def get(self, request):
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            query = "SELECT _id, countryName from countries WHERE isDeleted == false"
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                result_data = pd.DataFrame(db.countries.find({"isDeleted": False}))
            print("data shape: ", result_data.shape)
            if not result_data.shape[0]:
                response = {"message": "No Data found", "data": []}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

            result_data["id"] = result_data["_id"].astype(str)
            result_data = result_data.drop("_id", axis=1, errors="ignore")
            result_data = result_data.rename(columns={"countryName": "name"})
            result_data = result_data[['name','id']]
            response = {"message": "success", "data": result_data.to_dict(orient="records"),
                        "count": result_data.shape[0]}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class Cities(APIView):
    def get(self, request):
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            country_id = request.GET.get("country_id", "").strip()
            if not country_id:
                response = {"message": "Mandatory field 'country_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            query = "SELECT _id, cityName from cities WHERE isDeleted == false AND countryId == '{}'".format(country_id)
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"isDeleted": False, "countryId": country_id}
                params = {"_id": 1, "cityName": 1}
                result_data = pd.DataFrame(db.cities.find(query, params))

            if not result_data.shape[0]:
                response = {"message": "No Data found", "data": []}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

            try:
                result_data["id"] = result_data["_id"].apply(lambda x: x.oid)
            except:
                result_data["id"] = result_data["_id"].astype(str)

            result_data = result_data.drop("_id", axis=1, errors="ignore")
            result_data = result_data.rename(columns={"cityName": "name"})
            response = {"message": "success", "data": result_data.to_dict(orient="records"),
                        "count": result_data.shape[0]}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class Zones(APIView):
    def get(self, request):
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            city_id = request.GET.get("city_id", "").strip()
            if not city_id:
                response = {"message": "Mandatory field 'city_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                assert False
                query = "SELECT _id, title from zones WHERE status == 1 AND city_ID == '{}'".format(city_id)
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"status": 1, "city_ID": city_id}
                params = {"_id": 1, "title": 1}
                result_data = pd.DataFrame(db.zones.find(query, params))

            if not result_data.shape[0]:
                response = {"message": "No Data found", "data": []}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

            try:
                result_data["id"] = result_data["_id"].apply(lambda x: x.oid)
            except:
                result_data["id"] = result_data["_id"].astype(str)

            result_data = result_data.drop("_id", axis=1, errors="ignore")
            result_data = result_data.rename(columns={"title": "name"})
            response = {"message": "success", "data": result_data.to_dict(orient="records"),
                        "count": result_data.shape[0]}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
