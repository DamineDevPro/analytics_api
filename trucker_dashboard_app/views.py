import os
import sys
import traceback
import requests
import pandas as pd
import pytz
import numpy as np
from django.http import JsonResponse
from django.db import reset_queries
from datetime import datetime
import json
from rest_framework import status
from rest_framework.views import APIView
from analytics.function import Process
from analytics.settings import db, BASE_CURRENCY, BOOKING_STATUS, DEVICE_SUPPORT, CURRENCY_API,UTC
from .trucker_dashboard_db_helper import DbHelper
from .trucker_dashboard_operations_helper import TruckerOperations
from .trucker_dashboard_response_helper import TruckerResponses
from django.core.wsgi import get_wsgi_application

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

application = get_wsgi_application()
DbHelper = DbHelper()
TruckerResponses = TruckerResponses()
TruckerOperations = TruckerOperations()
device_support = DEVICE_SUPPORT

class TotalSales(APIView):
    """
    Overview dashboard API to provide the total sales data as per the time period and grouping with respect to the
    currency provided
    """

    def get(self, request):
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            # Store Id param check - mandatory field
            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            if 'timezone' not in request.GET:
                response = {'message': 'timezone is missing'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "start_timestamp or end_timestamp missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    start_timestamp = int(request.GET["start_timestamp"])
                    end_timestamp = int(request.GET["end_timestamp"])
                except:
                    response = {"message": "Incorrect timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month",
                              4: "quarter", 5: 'year', 6: "hour_of_day", 7: "day_of_week"}
            if "group_by" not in request.GET:
                group_by = 0
            else:
                group_by = request.GET["group_by"]
                try:
                    group_by = int(group_by)
                    if group_by not in list(group_by_value.keys()):
                        response = {
                            "message": "'group_by' value must be in range(0, {})".format(
                                max(list(group_by_value.keys())))}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                except:
                    return JsonResponse({"message": "'group_by' must be integer"},
                                        safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # currency support
            currency_symbol = request.GET.get("currency_symbol", "₹")
            if 'currency' not in request.GET:
                conversion_rate = 1
                currency = BASE_CURRENCY
            else:
                try:
                    currency = str(request.GET["currency"]).upper()
                except:
                    response = {'message': 'Bad request Currency'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if currency == BASE_CURRENCY:
                    conversion_rate = 1
                else:
                    try:
                        url = CURRENCY_API
                        parameters = {"from_currency": BASE_CURRENCY, "to_currency": currency, "fetchSize": 1}
                        auth = b'{"userId": "1", "userType": "admin", "metaData": {} }'
                        headers = {"Authorization": auth, "lan": 'en'}
                        currency_response = requests.get(url, params=parameters, headers=headers)
                        if currency_response.status_code != 200:
                            response = {'message': 'Error while fetching currency rate',
                                        'error': currency_response.content}
                            return JsonResponse(response, safe=False, status=currency_response.status_code)
                        currency_data = json.loads(currency_response.content.decode('utf-8'))
                        if currency_data.get("data").get('data'):
                            conversion_rate = float(
                                currency_data.get("data").get('data')[0].get('exchange_rate'))
                        else:
                            response = {"message": "currency conversion not found"}
                            return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                    except:
                        response = {'message': 'Internal server issue with exchange rate API'}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # store query addition

            # Spark SQL query for respective data from mongo database
            try:
                assert False
                store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
                query = "SELECT * from storeOrder" + " " + date_range_query + store_query + store_categories_query
                query = query.strip()
                data = sqlContext.sql(query)
                data = data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp},"status.status": 7, "storeType" : 23}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                projection = {"createdDate":1, "accounting":1}
                print("query--------> ",query)
                data = pd.DataFrame(db.storeOrder.find(query, projection))
                
            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data['createdDate'] = pd.to_datetime(data['createdDate'])
            data['createdDate'] = data['createdDate'].apply(
                lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))  # converting time to local time zone
            data["dateTime"] = data.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            # only need total sales record
            data["taxableAmount"] = data['accounting'].apply(lambda x: x["finalTotal"] * conversion_rate)
            # data["order_status"] = data["status"].apply(lambda x: x["statusText"])
            data = data[["dateTime", "taxableAmount"]]
            # agg_order = ["order_status_" + col for col in list(data["order_status"].unique()) if
            #              col not in ["Cancelled", "Completed", "Delivered"]]
            # agg_order.append("order_status_Cancelled")
            # data = pd.get_dummies(data, columns=["order_status"])
            # for col_zero in agg_order:
            #     if col_zero in list(data.columns):
            #         data["taxableAmount"] = np.where(data[col_zero] == 1, 0, data["taxableAmount"])
            data = data[["dateTime", "taxableAmount"]]

            # date filler (add missing date time in an data frame)
            data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                       time_zone=time_zone, data_frame=data, date_column="dateTime",
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
                "series": [
                    {"name": "Total Sales", "data": list(data['taxableAmount'])},
                ],
                "xaxis": {"title": group_by_value[group_by].replace("_", " ").capitalize(),
                          'categories': list(data['dateTime'].astype(str))},
                "yaxis": {"title": 'Sales Amount ({})'.format(currency_symbol)}
            }
            response = {'message': 'success',
                        'graph': graph,
                        'total': float(data['taxableAmount'].sum())}
            reset_queries()
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            reset_queries()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TotalOrders(APIView):
    """
        Overview dashboard API to provide the total Order data as per the time period and grouping with respect to the
        currency provided
        """

    def get(self, request):
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            # Store Id param check - mandatory field
            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != '0' and store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            if 'timezone' not in request.GET:
                response = {'message': 'timezone is missing'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "start_timestamp or end_timestamp missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    start_timestamp = int(request.GET["start_timestamp"])
                    end_timestamp = int(request.GET["end_timestamp"])
                except:
                    response = {"message": "Incorrect timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month",
                              4: "quarter", 5: 'year', 6: "hour_of_day", 7: "day_of_week"}
            if "group_by" not in request.GET:
                group_by = 0
            else:
                group_by = request.GET["group_by"]
                try:
                    group_by = int(group_by)
                    if group_by not in list(group_by_value.keys()):
                        response = {
                            "message": "'group_by' value must be in range(0, {})".format(
                                max(list(group_by_value.keys())))}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                except:
                    return JsonResponse({"message": "'group_by' must be integer"},
                                        safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # currency support
            currency_symbol = request.GET.get("currency_symbol", "₹")
            if 'currency' not in request.GET:
                conversion_rate = 1
                currency = BASE_CURRENCY
            else:
                try:
                    currency = str(request.GET["currency"]).upper()
                except:
                    response = {'message': 'Bad request Currency'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if currency == BASE_CURRENCY:
                    conversion_rate = 1
                else:
                    try:
                        url = CURRENCY_API
                        parameters = {"from_currency": BASE_CURRENCY, "to_currency": currency, "fetchSize": 1}
                        auth = b'{"userId": "1", "userType": "admin", "metaData": {} }'
                        headers = {"Authorization": auth, "lan": 'en'}
                        currency_response = requests.get(url, params=parameters, headers=headers)
                        if currency_response.status_code != 200:
                            response = {'message': 'Error while fetching currency rate',
                                        'error': currency_response.content}
                            return JsonResponse(response, safe=False, status=currency_response.status_code)
                        currency_data = json.loads(currency_response.content.decode('utf-8'))
                        if currency_data.get("data").get('data'):
                            conversion_rate = float(
                                currency_data.get("data").get('data')[0].get('exchange_rate'))
                        else:
                            response = {"message": "currency conversion not found"}
                            return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                    except:
                        response = {'message': 'Internal server issue with exchange rate API'}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            try:
                assert False
                # store query addition
                store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

                # Spark SQL query for respective data from mongo database
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
                query = "SELECT * from storeOrder" + " " + date_range_query + store_query + store_categories_query
                query = query.strip()
                data = sqlContext.sql(query)
                data = data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}, "status.status": {'$nin':[3]}, "storeType" : 23}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                print("query---------->", query)
                data = pd.DataFrame(db.storeOrder.find(query))

            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data['createdDate'] = pd.to_datetime(data['createdDate'])
            data['createdDate'] = data['createdDate'].apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))
            data["dateTime"] = data.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            # only need total sales record
            data["taxableAmount"] = data['accounting'].apply(lambda x: x["finalTotal"] * conversion_rate)
            # data["order_status"] = data["status"].apply(lambda x: x["statusText"])
            data = data[["dateTime", "taxableAmount"]]
            # agg_order = ["order_status_" + col for col in list(data["order_status"].unique()) if
            #              col != "Cancelled"]
            # data = pd.get_dummies(data, columns=["order_status"])
            # if "order_status_Cancelled" in list(data.columns):
            #     data["taxableAmount"] = np.where(data.order_status_Cancelled == 1, 0, data["taxableAmount"])
            # data["order"] = data[agg_order].apply(lambda x: sum(x), axis=1)
            data = data[["dateTime", "taxableAmount"]]
            print(data)

            # date filler (add missing date time in an data frame)
            data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                       time_zone=time_zone, data_frame=data, date_column="dateTime",
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
                "series": [
                    {"name": "Total Order", "data": list(data['taxableAmount'])},
                ],
                "xaxis": {"title": group_by_value[group_by].replace("_", " ").capitalize(),
                          'categories': list(data['dateTime'].astype(str))},
                "yaxis": {"title": 'Order Amount ({})'.format(currency_symbol)}
            }
            response = {'message': 'success',
                        'graph': graph,
                        'total': float(data['taxableAmount'].sum())}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class UserSessionLogs(APIView):
    def get(self, request):
        try:
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'Missing/TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "mandatory field 'start_timestamp' and 'end_timestamp' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    start_timestamp = int(request.GET["start_timestamp"])
                    end_timestamp = int(request.GET["end_timestamp"])
                except:
                    response = {"message": "Incorrect timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month", 4: "quarter", 5: 'year', 6: "Hour of day",
                              7: "Day of week"}
            if "group_by" not in request.GET:
                response = {'message': 'mandatory Field group_by'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                group_by = request.GET["group_by"]
                try:
                    group_by = int(group_by)
                    if group_by not in list(group_by_value.keys()):
                        response = {
                            "message": "'group_by' value must be in range(0, {})".format(
                                max(list(group_by_value.keys())))}
                        return JsonResponse(response, safe=False, status=422)
                except:
                    return JsonResponse({"message": "'group_by' must be integer"}, safe=False, status=422)

            try:
                assert False
                date_range_query = "WHERE sessionStart BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
                query = "SELECT sessionStart, userid from sessionLogs" + " " + date_range_query
                query = query.strip()
                data = sqlContext.sql(query)
                data = data.toPandas()
            except:
                query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp}}
                data = pd.DataFrame(db.sessionLogs.find(query, {"sessionStart": 1, "userid": 1}))

            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data['Total Count'] = 1
            data = data.rename(columns={"sessionStart": "dateTime"})

            data['dateTime'] = data['dateTime'].astype(int)
            data['dateTime'] = data['dateTime'].apply(lambda x: datetime.fromtimestamp(x, tz=time_zone))
            data["dateTime"] = data["dateTime"].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            data = data.fillna(0)

            # Date filler function called from Process Class, setting.py
            data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                       time_zone=time_zone, data_frame=data,
                                       date_column="dateTime", group_by=group_by)

            data = Process.date_conversion(group_by=group_by, data_frame=data, date_col="dateTime")
            drop_data_frame = data.drop_duplicates(subset=["dateTime", "userid"], keep='last')
            drop_data_frame = drop_data_frame.drop("userid", axis=1)
            drop_data_frame = drop_data_frame.rename(columns={"Total Count": "Unique Count"})
            drop_data_frame = drop_data_frame.groupby(['dateTime']).sum().reset_index()
            drop_data_frame = drop_data_frame.sort_values(by='dateTime', ascending=True)
            data = data.drop("userid", axis=1)
            group_data_frame = data.groupby(['dateTime']).sum().reset_index()
            group_data_frame = group_data_frame.sort_values(by='dateTime', ascending=True)
            merge_data_frame = pd.merge(group_data_frame, drop_data_frame, on='dateTime')
            if group_by == 3: merge_data_frame = Process.month_sort(data_frame=merge_data_frame, date_column="dateTime")
            if group_by == 4: merge_data_frame = Process.quarter_sort(data_frame=merge_data_frame,
                                                                      date_column="dateTime")
            graph = {
                "series": [
                    {"name": "Total Count", "data": list(merge_data_frame['Total Count'])},
                    {"name": "Unique Count", "data": list(merge_data_frame['Unique Count'])}
                ],
                "xaxis": {"title": group_by_value[group_by], "categories": list(merge_data_frame['dateTime'])},
                "yaxis": {"title": "Number Of Session"}
            }
            response = {'message': 'success', 'graph': graph}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class StoreCategory(APIView):
    def get(self, request):
        """
        Store Categories API
        :param request:
        :return:
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            data = pd.DataFrame(db.storeCategory.find({"visibility": 1}, {"name": 1, "_id": 1}))

            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            response_data = data[["_id", "name"]]
            response_data["id"] = response_data["_id"].apply(lambda x: str(x))
            response_data["name"] = response_data["name"].apply(lambda x: x[0] if len(x) != 0 else "")
            response_data = response_data.drop("_id", axis=1, errors="ignore")
            response_data = response_data.to_dict(orient="records")
            response_data.insert(0, {"id": "0", "name": "All"})
            response = {'message': 'success', 'data': response_data}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class OrderPaymentOverView(APIView):
    def get(self, request):
        """
        Order activity with respect to card and cash aggregated as per time period drill down to store categories

        :param request:
        :return:
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            # Store Id param check - mandatory field
            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {"message": "Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                              4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                group_by_period = group_by_value[group_by]
            except:
                return JsonResponse({"message": "'group_by' must be integer", "support": group_by_value},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = "" if store_categories_id == "0" else store_categories_id
            store_categories_query = ""
            if store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
            # -------------------------------------- createdTimeStamp ------------------------------------------
            try:
                assert False
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
                query = "SELECT createdTimeStamp, storeCategoryId, storeCategory, paymentType from storeOrder" \
                        + " " + date_range_query + store_categories_query + store_query
                query = query.strip()
                order = sqlContext.sql(query)
                order = order.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                order = pd.DataFrame(db.storeOrder.find(query))

            if not order.shape[0]:
                response = {'message': 'No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
            # order = pd.DataFrame(order)
            store = order[["storeCategoryId", "storeCategory"]].drop_duplicates(subset=["storeCategoryId"], keep='last')

            order['createdTimeStamp'] = order['createdTimeStamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            order['paymentType'] = order["paymentType"].astype(int)
            order['paymentType'] = order["paymentType"].replace({1: "card", 2: "cash"})
            order['createdTimeStamp'] = order['createdTimeStamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            # Graph Data Construction

            order = order[['createdTimeStamp', 'storeCategoryId', 'paymentType']]
            unique_order = list(order.paymentType.unique())
            if len(unique_order) == 1:
                if unique_order[0] == 'card':
                    order = order.rename(columns={'paymentType': 'paymentType_card'})
                    order['paymentType_card'] = 1
                    order['paymentType_cash'] = 0
                elif unique_order[0] == 'cash':
                    order = order.rename(columns={'paymentType': 'paymentType_cash'})
                    order['paymentType_card'] = 0
                    order['paymentType_cash'] = 1
                else:
                    pass
            else:
                order = pd.get_dummies(order, columns=['paymentType'])

            order = order[['createdTimeStamp', 'paymentType_card', 'paymentType_cash']]
            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='createdTimeStamp',
                                        group_by=group_by)
            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="createdTimeStamp")
            bookings_group_graph = order.groupby(['createdTimeStamp']).sum().reset_index()
            del order
            bookings_group_graph = bookings_group_graph.sort_values(by='createdTimeStamp', ascending=True)

            if group_by == 3:
                bookings_group_graph = Process.month_sort(data_frame=bookings_group_graph,
                                                          date_column="createdTimeStamp")
            if group_by == 4:
                bookings_group_graph = Process.quarter_sort(data_frame=bookings_group_graph,
                                                            date_column="createdTimeStamp")
            if group_by == 7:
                bookings_group_graph = Process.day_sort(data_frame=bookings_group_graph, date_column="createdTimeStamp")

            total = int(bookings_group_graph['paymentType_card'].sum() + bookings_group_graph['paymentType_cash'].sum())
            graph = {
                'series': [
                    {
                        'name': 'Card',
                        'data': list(bookings_group_graph['paymentType_card'])
                    },
                    {
                        'name': 'Cash',
                        'data': list(bookings_group_graph['paymentType_cash'])
                    }
                ],
                'xaxis': {
                    'title': 'Time Line',
                    'categories': list(bookings_group_graph['createdTimeStamp'])
                },
                'yaxis': {
                    'title': 'Payment Count'
                }
            }
            data = {
                'graph': graph,
                "total": total
            }
            response = {'message': 'success', 'data': data}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class AverageSales(APIView):
    """
    Overview dashboard API to provide the total sales data as per the time period and grouping with respect to the
    currency provided
    """

    def get(self, request):
        try:

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
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            if 'timezone' not in request.GET:
                response = {'message': 'timezone is missing'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "start_timestamp or end_timestamp missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
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

            group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year', 6: "Hour of Day",
                              7: "Day of Week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                group_check = group_by_value[group_by]
            except:
                return JsonResponse({"message": "Un-supported 'group_by'", "support": group_by_value},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # currency support
            currency_symbol = request.GET.get("currency_symbol", "₹")
            if 'currency' not in request.GET:
                conversion_rate = 1
                currency = BASE_CURRENCY
            else:
                currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
                if currency == BASE_CURRENCY:
                    conversion_rate = 1
                else:
                    try:
                        url = CURRENCY_API
                        parameters = {"from_currency": BASE_CURRENCY, "to_currency": currency, "fetchSize": 1}
                        auth = b'{"userId": "1", "userType": "admin", "metaData": {} }'
                        headers = {"Authorization": auth, "lan": 'en'}
                        currency_response = requests.get(url, params=parameters, headers=headers)
                        if currency_response.status_code != 200:
                            response = {'message': 'Error while fetching currency rate',
                                        'error': currency_response.content}
                            return JsonResponse(response, safe=False, status=currency_response.status_code)
                        currency_data = json.loads(currency_response.content.decode('utf-8'))
                        if currency_data.get("data").get('data'):
                            conversion_rate = float(
                                currency_data.get("data").get('data')[0].get('exchange_rate'))
                        else:
                            response = {"message": "currency conversion not found"}
                            return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                    except:
                        response = {'message': 'Internal server issue with exchange rate API'}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # Spark SQL query for respective data from mongo database
            date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT * from storeOrder" + " " + date_range_query + store_query + store_categories_query
            query = query.strip()
            try:
                assert False
                data = sqlContext.sql(query)
                data = data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}, "status.status": 7, "storeType" : 23}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                data = pd.DataFrame(db.storeOrder.find(query))

            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data['createdDate'] = pd.to_datetime(data['createdDate'])
            data['createdDate'] = data['createdDate'].apply(
                lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))  # converting time to local time zone
            data["dateTime"] = data.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            # only need total sales record
            data["taxableAmount"] = data['accounting'].apply(lambda x: x["finalTotal"] * conversion_rate)
            # data["order_status"] = data["status"].apply(lambda x: x["statusText"])
            data = data[["dateTime", "taxableAmount"]]
            # agg_order = ["order_status_" + col for col in list(data["order_status"].unique()) if
            #              col not in ["Cancelled", "Completed"]]
            # agg_order.append("order_status_Cancelled")
            # data = pd.get_dummies(data, columns=["order_status"])
            # for col_zero in agg_order:
            #     if col_zero in list(data.columns):
            #         data["taxableAmount"] = np.where(data[col_zero] == 1, 0, data["taxableAmount"])
            data = data[["dateTime", "taxableAmount"]]
            total_taxable_amount = float(data["taxableAmount"].sum())
            total_count = int(data["taxableAmount"].astype(bool).sum())
            average_taxable_amt = 0
            if total_taxable_amount and total_count:
                average_taxable_amt = total_taxable_amount / total_count

            # date filler (add missing date time in an data frame)
            data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                       time_zone=time_zone, data_frame=data, date_column="dateTime",
                                       group_by=group_by)

            # date converter as per group by
            data = Process.date_conversion(group_by=group_by, data_frame=data, date_col='dateTime')
            data['taxableAmount'] = data['taxableAmount'].replace(0, np.NaN)
            data = data.groupby(['dateTime']).mean().reset_index()
            data['taxableAmount'] = data['taxableAmount'].fillna(0)

            # data frame sorting
            data = data.sort_values(by='dateTime', ascending=True)
            if group_by == 3: data = Process.month_sort(data_frame=data, date_column="dateTime")
            if group_by == 4: data = Process.quarter_sort(data_frame=data, date_column="dateTime")
            if group_by == 7: data = Process.day_sort(data_frame=data, date_column="dateTime")

            graph = {
                "series": [
                    {"name": "Average Sales", "data": list(data['taxableAmount'])},
                ],
                "xaxis": {"title": group_by_value[group_by], 'categories': list(data['dateTime'].astype(str))},
                "yaxis": {"title": 'Average Sales Amount ({})'.format(currency_symbol)}
            }

            response = {'message': 'success',
                        'graph': graph,
                        'total': float(average_taxable_amt) if average_taxable_amt else 0,
                        'count': int(total_count) if total_count else 0}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class SessionConversion(APIView):
    def get(self, request):

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
            store_id = "" if store_id == "0" else store_id
        except:
            response = {"message": "mandatory field 'store_id' missing"}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        # Store Category Id param check
        store_categories_id = str(request.GET.get("store_categories_id", ""))
        store_categories_id = "" if store_categories_id == "0" else store_categories_id

        # start_timestamp and end_timestamp parameter check
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
        except:
            response = {"message": "Missing/Incorrect timestamp, start_timestamp and end_timestamp must be integer"}
            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        # ---------------------------------------------------------------------------------------------------
        # Spark sessionLogs query to count out the unique session in respective time frame
        date_time_query = "WHERE sessionStart BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
        query = "SELECT COUNT(_id) FROM sessionLogs"
        try:
            assert False
            query = " ".join([query, date_time_query])  # count the number of mongo ids in respective time frame
            session_logs = sqlContext.sql(query)
            session_logs = session_logs.toPandas()
            session_logs_count = session_logs.loc[0, "count(_id)"]
        except:
            query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp}}
            session_logs_count = db.sessionLogs.find(query).count()
        # ---------------------------------------------------------------------------------------------------
        # Spark cart query to count out the unique session in respective time frame
        query = "SELECT _id, sellers, cartLogs, sessionId FROM cart"
        date_time_query = "WHERE cartLogs.timestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
        store_filter_query = ""
        try:
            assert False
            store_category_filer_query = "AND storeCategoryId == '{}'".format(
                store_categories_id) if store_categories_id else ""
            query = " ".join([query, date_time_query, store_filter_query, store_category_filer_query])
            cart = sqlContext.sql(query)
            cart = cart.toPandas()
        except:
            query = {"cartLogs.timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
            if store_categories_id: query["storeCategoryId"] = store_categories_id
            cart = pd.DataFrame(db.cart.find(query, {"_id": 1, "sellers": 1, "cartLogs": 1, "sessionId": 1}))

        cart_index = []
        for index in range(cart.shape[0]):
            for seller in cart.sellers.loc[index]:
                _dict = {
                    'cartId': cart["_id"].loc[index],
                    'status': cart.cartLogs.loc[index]["status"],
                    'seller_id': seller.get("fullFillMentCenterId", "0"),
                    # 'seller_id': seller["fullFillMentCenterId"] if seller["fullFillMentCenterId"] else "0", 
                    'session_id': cart["sessionId"].loc[index]
                }
                cart_index.append(_dict)
        del cart
        cart_index = pd.DataFrame(cart_index)
        if not cart_index.shape[0]:
            cart_session_count = 0
            checkout_count = 0
        else:
            if store_id: cart_index = cart_index[cart_index.seller_id == store_id]
            cart_session_count = len(cart_index.session_id.unique()) if cart_index.shape[0] else 0
            cart_index = cart_index[cart_index.status == 3]
            checkout_count = len(cart_index.session_id.unique()) if cart_index.shape[0] else 0
        del cart_index
        # ---------------------------------------------------------------------------------------------------
        query = "SELECT COUNT(DISTINCT sessionId) FROM storeOrder"
        date_time_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
        store_filter_query = "AND storeId == '{}'".format(store_id) if store_id else ""
        store_category_filer_query = "AND storeCategoryId == '{}'".format(
            store_categories_id) if store_categories_id else ""

        complete_order = "AND status.status == 7"
        query = " ".join([query, date_time_query, store_filter_query, store_category_filer_query,
                          complete_order])
        try:
            assert False
            store_order = sqlContext.sql(query)
            store_order = store_order.toPandas()
            order_count = int(store_order.loc[0, "count(DISTINCT sessionId)"])
        except:
            query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
            if store_id: query["storeId"] = store_id
            if store_categories_id: query["storeCategoryId"] = store_categories_id
            order_count = len(db.storeOrder.distinct("sessionId", query))

        percent_cart = float((cart_session_count / session_logs_count) * 100) if session_logs_count else 0
        percent_checkout = float((checkout_count / session_logs_count) * 100) if session_logs_count else 0
        percent_conversion = float((order_count / session_logs_count) * 100) if session_logs_count else 0

        data = {
            "overall_percent": round(percent_conversion, 2),
            "cart": int(cart_session_count),
            "percent_cart": round(percent_cart, 2),
            "checkout": int(checkout_count),
            "percent_checkout": round(percent_checkout, 2),
            "conversion": int(order_count),
            "percent_conversion": round(percent_conversion, 2),
        }

        response = {"message": "success", "data": data}
        return JsonResponse(response, safe=False, status=status.HTTP_200_OK)


class TruckerRevenue(APIView):
    def get(self, request):
        """
        GET API: 
        """
        # ---------------------------- Auth ----------------------------
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return TruckerResponses.get_status_401()
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return TruckerResponses.get_status_401()
        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status:
                status_check = BOOKING_STATUS[booking_status]
        except:
            return TruckerResponses.get_status_422('booking status issue')

        # ------------------ start_timestamp and end_timestamp in epoch(seconds) ----------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return TruckerResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        # ------------------ currency rate ------------------
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return TruckerResponses.get_status(message=_currency["response_message"],
                                                status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]
        currency_symbol = request.GET.get("currency_symbol", "₹")

        # ------------------ timezone ------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return TruckerResponses.get_status_400(response)
        # ------------------ group_by ------------------
        group_by_value = {
            0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_check = group_by_value[group_by]
        except:
            response = {"message": "'group_by' must be integer", "support": group_by_value}
            return TruckerResponses.get_status_400(response)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        # ------------------ DATA ------------------
        result_data = DbHelper.trucker_trip_data(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              booking_status=booking_status,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id
                                              )

        if not result_data.shape[0]: return TruckerResponses.get_status_204()
        # ------------------ Operation ------------------
        return TruckerOperations.trucker_fare_graph(result_data=result_data,
                                              time_zone=time_zone,
                                              start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              group_by=group_by,
                                              conversion_rate=conversion_rate,
                                              currency_symbol=currency_symbol)


class TruckerCount(APIView):
    def get(self, request):
        """
        GET API:
        """
        # ---------------------------- Auth ----------------------------
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return TruckerResponses.get_status_401()
        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return TruckerResponses.get_status_422('booking status issue')

        # ------------------ start_timestamp and end_timestamp in epoch(seconds) ----------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return TruckerResponses.get_status_400(response, params=["start_timestamp", "end_timestamp"])
        # ------------------ timezone ------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:

            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return TruckerResponses.get_status_400(response)
        # ------------------ group_by ------------------
        group_by_value = {
            0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_check = group_by_value[group_by]
        except:
            response = {"message": "'group_by' must be integer", "support": group_by_value}
            return TruckerResponses.get_status_400(response)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        # ------------------ DATA ------------------
        result_data = DbHelper.trucker_trip_data(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              booking_status=booking_status,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id
                                              )

        if not result_data.shape[0]: return TruckerResponses.get_status_204()
        # ------------------ Operation ------------------
        return TruckerOperations.trucker_count_graph(result_data=result_data,
                                               time_zone=time_zone,
                                               start_timestamp=start_timestamp,
                                               end_timestamp=end_timestamp,
                                               group_by=group_by)


class TruckerStatus(APIView):
    def get(self, request):
        """
        GET API to provide the graphical data with respect to the trucker cancellation with respect to driver and customer
        :param request:
        :return:
        """
        # ---------------------------- Auth ----------------------------

        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return TruckerResponses.get_status_401()
        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ---------------------------- time_zone ----------------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return TruckerResponses.get_status_400(message=response, params=["timezone"])
        # ----------------- start_timestamp and end_timestamp in epoch(seconds) -----------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return TruckerResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return TruckerResponses.get_status_422('booking status issue')
        # ---------------------------- group_by ----------------------------
        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return TruckerResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        # ---------------------------- DATA ----------------------------
        order = DbHelper.trucker_status(start_timestamp=start_timestamp,
                                     end_timestamp=end_timestamp,
                                     device_type=device_type,
                                     country_id=country_id,
                                     city_id=city_id,
                                     zone_id=zone_id,
                                     booking_status=booking_status)
        if not order.shape[0]: return TruckerResponses.get_status_204()
        # ------------------ Operation ------------------
        return TruckerOperations.trucker_status(order=order,
                                          time_zone=time_zone,
                                          start_timestamp=start_timestamp,
                                          end_timestamp=end_timestamp,
                                          group_by=group_by)


class TruckerPayment(APIView):
    def get(self, request):
        """
        Order activity with respect to payment method aggregated as per time period drill down to vehicle type

        :param request:
        :return:
        """
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return TruckerResponses.get_status_401()
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return TruckerResponses.get_status_422('booking status issue')

        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return TruckerResponses.get_status_400(message=response, params=["timezone"])
        # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return TruckerResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return TruckerResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        vehicle_type_id = str(request.GET.get("vehicle_type_id", ""))
        vehicle_type_id = "" if vehicle_type_id == "0" else vehicle_type_id
        vehicle_type_query = ""
        vehicle_mongo_query = {}
        if vehicle_type_id:
            vehicle_type_query = " AND vehicleType.typeId typeId == '{}'".format(vehicle_type_id)
            vehicle_mongo_query = {"vehicleType.typeId": vehicle_type_id}
        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        order = DbHelper.trucker_payment(start_timestamp=start_timestamp,
                                      end_timestamp=end_timestamp,
                                      vehicle_type_query=vehicle_type_query,
                                      device_type=device_type,
                                      country_id=country_id,
                                      city_id=city_id,
                                      zone_id=zone_id,
                                      booking_status=booking_status,
                                      vehicle_mongo_query=vehicle_mongo_query
                                      )
        if not order.shape[0]:
            return TruckerResponses.get_status_204()
        return TruckerOperations.trucker_payment(order, time_zone, start_timestamp, end_timestamp, group_by)


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
                if not result_data.shape[0]:
                    response = {"message": "No Data found", "data": []}
                    return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
                result_data["id"] = result_data["_id"].apply(lambda x: x.oid)
            except:
                result_data = pd.DataFrame(db.countries.find({"isDeleted": False}))
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
            try:
                assert False
                query = "SELECT _id, cityName from cities WHERE isDeleted == false AND countryId == '{}'".format(
                    country_id)
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
                if not result_data.shape[0]:
                    response = {"message": "No Data found", "data": []}
                    return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
                result_data["id"] = result_data["_id"].apply(lambda x: x.oid)
            except:
                result_data = pd.DataFrame(db.cities.find({"isDeleted": False, "countryId": country_id}))
                if not result_data.shape[0]:
                    response = {"message": "No Data found", "data": []}
                    return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
                result_data["id"] = result_data["_id"].astype(str)
            result_data = result_data.drop("_id", axis=1, errors="ignore")
            result_data = result_data.rename(columns={"cityName": "name"})
            result_data = result_data[['name','id']]
            response = {"message": "success", "data": result_data.to_dict(orient="records"),
                        "count": result_data.shape[0]}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
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

            # city_id = request.GET.get("city_id", "").strip()
            # if not city_id:
            #     response = {"message": "Mandatory field 'city_id' missing"}
            #     return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            #
            # query = "SELECT _id, title from zones WHERE status == 1 AND city_ID == '{}'".format(city_id)
            # result_data = sqlContext.sql(query)
            # result_data = result_data.toPandas()

            query = {}
            projection = {"name": 1}
            result_data = pd.DataFrame(db["operationZones_truckers"].find(query, projection))
            if not result_data.shape[0]:
                response = {"message": "No Data found", "data": []}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
            result_data["id"] = result_data["_id"].apply(lambda x: str(x))
            result_data = result_data.drop("_id", axis=1, errors="ignore")
            response = {"message": "success",
                        "data": result_data.to_dict(orient="records"),
                        "count": result_data.shape[0]}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
