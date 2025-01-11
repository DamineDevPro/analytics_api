import sys
import pandas as pd
import numpy as np
from datetime import datetime
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
from analytics.settings import  UTC, db, BASE_CURRENCY, CURRENCY_API, _casandra
import requests
from analytics.function import Process
import json
import pytz
import os, sys
import traceback
from django.db import reset_queries

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()

class TopNProduct(APIView):
    def get(self, request):
        """
        GET API: Top N Product overview dashboard api
        :param request:
        :return:
        """
        try:
            reset_queries()
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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

            # start_timestamp and end_timestamp in epoch(seconds)
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
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # currency support
            currency = str(request.GET["currency"]).upper()
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
                        conversion_rate = float(currency_data.get("data").get('data')[0].get('exchange_rate'))
                    else:
                        response = {"message": "currency conversion not found"}
                        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                except Exception as ex:
                    response = {'message': 'Internal server issue with exchange rate API',
                                "error": type(ex).__name__}
                    return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            try:
                top_product = int(request.GET.get("top_product", 5))
            except:
                response = {"message": "'top_product' should be integer"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # spark sql query
            query = "SELECT * from storeOrder WHERE createdTimeStamp BETWEEN {} AND {} ".format(
                start_timestamp, end_timestamp)
            if store_categories_id: query = query + "AND storeCategoryId == '{}'".format(store_categories_id)
            query = query + store_query
            try:
                assert False
                query = query.strip()
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                projection = {"cartId":1, "products":1, "paymentType":1}
                result_data = pd.DataFrame(db.storeOrder.find(query, projection))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            product_df = result_data[["cartId", "products", "paymentType"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    _dict = {
                        'cartId': product_df.cartId.loc[index],
                        'name': product['name'].capitalize(),
                        'centralProductId': product['centralProductId'],
                        'quantity': product['quantity']['value'],
                        'status': int(product['status']['status']),
                        'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                        "paymentType": product_df.paymentType.loc[index],
                        "cancelled": int(product['timestamps']['cancelled']),
                        "completed": int(product['timestamps']['completed'])
                    }
                    invoice_product_list.append(_dict)
            del product_df
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            del invoice_product_list
            # ============================= If product payment not received =============================
            # invoice_df["quantity"][((invoice_df.status == 3) &
            #                         ((invoice_df.paymentType == 2) &
            #                          ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0
            # invoice_df["taxableAmount"][((invoice_df.status == 3) &
            #                              ((invoice_df.paymentType == 2) &
            #                               ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0
            invoice_df = invoice_df[~((invoice_df.status == 3) &
                                      ((invoice_df.paymentType == 2) &
                                       ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))]
            invoice_df = invoice_df.drop(["paymentType", "cancelled", "completed"], axis=1, errors="ignore")
            # ===========================================================================================
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0, axis=1)
            product = invoice_df[["centralProductId", "name"]].drop_duplicates()
            grouped_df = invoice_df.groupby(['centralProductId']).sum().reset_index()
            del invoice_df
            grouped_df = pd.merge(grouped_df, product, on='centralProductId', how='left')
            columns = ["name", "taxableAmount"]
            grouped_df = grouped_df[columns]
            rename_col = {"taxableAmount": "Revenue"}
            grouped_df = grouped_df.rename(columns=rename_col)
            grouped_df = grouped_df[grouped_df["Revenue"] != 0]
            grouped_df = grouped_df.sort_values(by="Revenue", ascending=False)
            total_value = grouped_df.Revenue.sum()
            grouped_df['taxableAmount'] = grouped_df['Revenue'].astype(float)
            grouped_df['% Revenue'] = grouped_df['Revenue'].apply(lambda x: round((x / total_value) * 100, 2))
            grouped_df = grouped_df.sort_values(by='Revenue', ascending=False)
            grouped_df = grouped_df[:top_product]
            grouped_df = grouped_df[['name', 'Revenue', '% Revenue']].to_dict(orient='record')
            response = {'message': 'success', 'data': grouped_df}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TotalSales(APIView):
    """
    Overview dashboard API to provide the total sales data as per the time period and grouping with respect to the
    currency provided
    """

    def get(self, request):
        reset_queries()
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
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # Spark SQL query for respective data from mongo database
            try:
                assert False
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
                query = "SELECT * from storeOrder" + " " + date_range_query + store_query + store_categories_query
                query = query.strip()
                data = sqlContext.sql(query)
                data = data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp},"status.status": 7}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                projection = {"createdDate":1, "accounting":1}
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
            data["taxableAmount"] = data['accounting'].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]) * conversion_rate)
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
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}, "status.status": {'$nin':[3]}}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                data = pd.DataFrame(db.storeOrder.find(query))

            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data['createdDate'] = pd.to_datetime(data['createdDate'])
            data['createdDate'] = data['createdDate'].apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))
            data["dateTime"] = data.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            # only need total sales record
            data["taxableAmount"] = data['accounting'].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]) * conversion_rate)
            # data["order_status"] = data["status"].apply(lambda x: x["statusText"])
            data = data[["dateTime", "taxableAmount"]]
            # agg_order = ["order_status_" + col for col in list(data["order_status"].unique()) if
            #              col != "Cancelled"]
            # data = pd.get_dummies(data, columns=["order_status"])
            # if "order_status_Cancelled" in list(data.columns):
            #     data["taxableAmount"] = np.where(data.order_status_Cancelled == 1, 0, data["taxableAmount"])
            # data["order"] = data[agg_order].apply(lambda x: sum(x), axis=1)
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

class TopWish(APIView):
    def get(self, request):
        """
        GET API to calculate the top products in wish-list in respective time duration by it's count
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

            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # if store_id:
            #     response = {"message": "Action not allowed, unsupported 'Store_id'", "store_id": store_id}
            #     return JsonResponse(response, safe=False, status=status.HTTP_403_FORBIDDEN)

            # store id
            store_id = str(request.GET.get("store_id", ""))
            store_id_query = ""
            if store_id != "0" and store_id:
                store_id_query = " AND storeid == '{}' ".format(store_id)

            # Store Categories Id param check - mandatory field
            store_categories_id = str(request.GET.get('store_categories_id'))
            store_categories_id = "" if store_categories_id == "0" else store_categories_id

            #         start_timestamp and end_timestamp request
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                start_timestamp = str(datetime.fromtimestamp(start_timestamp, tz=UTC))
                end_timestamp = str(datetime.fromtimestamp(end_timestamp, tz=UTC))
            except:
                response = {"message": "Missing/Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                top = int(request.GET['top']) if 'top' in request.GET else 5
            except:
                response = {'message': '"top" must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # store query addition
            store_categories_query = " AND storecategoryid == '{}'".format(
                store_categories_id) if store_categories_id else ""

            try:
                assert False
                date_query = " WHERE createdTimeStamp BETWEEN '{start}' AND '{end}'".format(
                    start=start_timestamp, end=end_timestamp)
                query = "SELECT * from favouriteproductsuserwise" + date_query + store_categories_query + store_id_query
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                date_query = " WHERE createdTimeStamp > '{start}' AND createdTimeStamp < '{end}'".format(
                    start=start_timestamp, end=end_timestamp)
                query = "SELECT * from favouriteproductsuserwise" + \
                        date_query + \
                        store_categories_query + \
                        store_id_query + " ALLOW FILTERING"

                rows = _casandra.execute(query)
                result_data = pd.DataFrame(list(rows))

            if not result_data.shape[0]:
                response = {'message': 'No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            # result_data['productname'] = result_data['productname'].apply(lambda x: ast.literal_eval(x))
            # result_data['productname'] = result_data['productname'].apply(lambda x: x['en'])
            result_data['productname'] = result_data['productname'].astype(str)
            product_df = result_data[["childproductid", "productname"]]
            unique_users = len(result_data['userid'].unique())
            unique_products = len(result_data['childproductid'].unique())
            product_df = product_df.drop_duplicates(keep='last')
            wish_list_df = result_data[["userid", "childproductid"]]
            del result_data
            wish_list_df = wish_list_df.drop_duplicates(keep='last')
            wish_list_df = wish_list_df.drop("userid", axis=1)
            wish_list_df['Count'] = 1
            wish_list_df_group = wish_list_df.groupby(['childproductid']).sum().reset_index()
            wish_list_df_group = wish_list_df_group.sort_values(by="Count", ascending=False)
            wish_list_count = wish_list_df_group["Count"].sum()
            wish_list_df_group["% Count"] = wish_list_df_group["Count"].apply(
                lambda x: round((x / wish_list_count) * 100, 2))
            # wish_list_df_group = wish_list_df_group.head(top)
            wish_list_df_group = wish_list_df_group.merge(product_df, on="childproductid")
            wish_list_df_group = wish_list_df_group[["productname", "Count", "% Count"]]
            wish_list_df_group["Count"] = wish_list_df_group['Count'].astype(int)
            wish_list_df_group["% Count"] = wish_list_df_group['% Count'].astype(float)
            wish_list_df_group = wish_list_df_group.head(top)
            wish_list_df_group = wish_list_df_group.to_dict(orient='records')
            response = {'message': 'success',
                        'data': wish_list_df_group,
                        "unique_users": int(unique_users),
                        "unique_products": int(unique_products),
                        "product_count": int(wish_list_count)}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TopCart(APIView):
    def get(self, request):
        """
        GET API to calculate the top products in wish-list in respective time duration by it's count
        :param request:
        :return:
        """
        # TODO: Store id and store Categories id missing in cassandra "cartlogs" collection
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
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            if store_id:
                response = {"message": "Action not allowed, unsupported 'Store_id'", "store_id": store_id}
                return JsonResponse(response, safe=False, status=status.HTTP_403_FORBIDDEN)

            #         start_timestamp and end_timestamp request
            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "start_timestamp or end_timestamp missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    start_timestamp = int(request.GET["start_timestamp"])
                    end_timestamp = int(request.GET["end_timestamp"])
                except:
                    response = {"message": "Incorrect timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                try:
                    start_timestamp = str(datetime.fromtimestamp(start_timestamp, tz=UTC))
                    end_timestamp = str(datetime.fromtimestamp(end_timestamp, tz=UTC))
                except:
                    response = {"message": "Timestamp conversion issue"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            try:
                top = int(request.GET['top']) if 'top' in request.GET else 5
            except:
                response = {'message': '"top" must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            column_map = {1: "Quantity", 2: "Price"}
            if "column" not in request.GET:
                column = 1
            else:
                try:
                    column = int(request.GET["column"])
                    if column not in list(column_map.keys()):
                        return JsonResponse({"message": "Unsupported column", "support": column_map}, safe=False,
                                            status=422)
                except:
                    return JsonResponse({"message": "column must be integer", "support": column_map}, safe=False,
                                        status=400)
            column = column_map[column]

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = "" if store_categories_id == "0" else store_categories_id

            try:
                assert False
                date_query = " WHERE createdtimestamp BETWEEN '{start}' AND '{end}'".format(start=start_timestamp,
                                                                                            end=end_timestamp)
                query = "SELECT * from cartlogs" + date_query
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()

            except:
                date_query = " WHERE createdtimestamp > '{start}' AND createdtimestamp < '{end}'".format(
                    start=start_timestamp,
                    end=end_timestamp)

                query = "SELECT * from cartlogs" + date_query
                if store_categories_id: query = query + "AND storeCategoryId == '{}'".format(store_categories_id)
                query += " ALLOW FILTERING"
                rows = _casandra.execute(query)
                result_data = pd.DataFrame(list(rows))

            if not result_data.shape[0]:
                response = {'message': 'No Data Found'}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
            result_data = result_data[["createdtimestamp", "cartid", "productname", "unitprice", "closingqty"]]
            result_data["createdtimestamp"] = pd.to_datetime(result_data["createdtimestamp"])
            result_data = result_data.sort_values(by="createdtimestamp", ascending=True)
            result_data = result_data.drop_duplicates(subset=["cartid", "productname"], keep="last")
            result_data = result_data.drop(["cartid", "createdtimestamp"], axis=1)
            product_group = result_data.groupby(['productname']).sum().reset_index()
            del result_data
            rename_columns = {"productname": "Product Name", "closingqty": "Quantity", "unitprice": "Price"}
            product_group = product_group.rename(columns=rename_columns)
            total = {"Quantity": product_group["Quantity"].sum(), "Price": product_group["Price"].sum()}
            product_group = product_group[["Product Name", column]]
            product_group = product_group.sort_values(by=column, ascending=False)
            percent_column = "% " + column
            product_group[percent_column] = product_group[column].apply(lambda x: round((x / total[column]) * 100, 2))
            product_group = product_group.head(top)
            product_group = product_group.to_dict(orient="records")
            response = {"message": "success", "data": product_group, "total": total}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
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


class TopBrand(APIView):
    def get(self, request):
        """
        GET API to show data of top brand in respective time period
        :param request: store_categories(string): Mongo ID of respective store categories (optional)
                        start_timestamp(integer): epoch time in seconds (mandatory)
                        end_timestamp(integer)  : epoch time in seconds (mandatory)
                        sort_by(string)         : column name by which table to be sorted (optional, default: "Product Revenue")
                        currency(string)        : currency name ex. INR, USD, etc. (optional)
                        top_brand(integer)      : provide top number of brand to be displayed
        :return: tabular data with 200 response status
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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            top_brand = int(request.GET.get("top_brand", 10))
            sort_by = int(request.GET.get("sort_by", 1))
            if sort_by not in [1, 2]:
                response = {"message": "unsupported 'sort_by'", "support": {1: "revenue", 2: "quantity"}}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

            # start_timestamp and end_timestamp in epoch(seconds)
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

            # currency support
            if 'currency' not in request.GET:
                conversion_rate = 1
            else:
                currency = str(request.GET["currency"]).upper()
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
                            conversion_rate = float(currency_data.get("data").get('data')[0].get('exchange_rate'))
                        else:
                            response = {"message": "currency conversion not found"}
                            return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                    except Exception as ex:
                        response = {'message': 'Internal server issue with exchange rate API'}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # spark sql query
            query = "SELECT * from storeOrder WHERE createdTimeStamp BETWEEN {} AND {} ".format(
                start_timestamp, end_timestamp)
            if store_categories_id: query = query + "AND storeCategoryId == '{}'".format(store_categories_id)
            query = query + store_query
            # list all store categories related to food
            restaurant_store_categories = tuple(Process.restaurant_store_categories(db=db))

            if restaurant_store_categories:
                query = query + " AND storeCategoryId NOT IN {}".format(restaurant_store_categories)
            try:
                assert False
                query = query.strip()
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "products", "paymentType"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    _dict = {
                        'cartId': product_df.cartId.loc[index],
                        'brandName': product['brandName'],
                        'quantity': product['quantity']['value'],
                        'status': int(product['status']['status']),
                        'taxableAmount': float(product['accounting']['taxableAmount']) * conversion_rate,
                        "paymentType": product_df.paymentType.loc[index],
                        "cancelled": int(product['timestamps']['cancelled']),
                        "completed": int(product['timestamps']['completed'])
                    }
                    invoice_product_list.append(_dict)
            del product_df
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            del invoice_product_list
            # ============================= If product payment not received =============================
            # invoice_df["quantity"][((invoice_df.status == 3) &
            #                         ((invoice_df.paymentType == 2) &
            #                          ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0
            # invoice_df["taxableAmount"][((invoice_df.status == 3) &
            #                              ((invoice_df.paymentType == 2) &
            #                               ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0
            invoice_df = invoice_df[~((invoice_df.status == 3) &
                                      ((invoice_df.paymentType == 2) &
                                       ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))]
            invoice_df = invoice_df.drop(["paymentType", "cancelled", "completed"], axis=1, errors="ignore")
            # ===========================================================================================
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0, axis=1)
            invoice_df["unique_purchase"] = 1
            grouped_df = invoice_df.groupby(['brandName']).sum().reset_index()
            del invoice_df
            columns = ["brandName", "taxableAmount", "unique_purchase", "quantity"]
            grouped_df = grouped_df[columns]
            rename_col = {"brandName": "Product Brand", "taxableAmount": "Product Revenue",
                          "unique_purchase": "Unique Purchases", "quantity": "Quantity"}
            grouped_df = grouped_df.rename(columns=rename_col)
            sort_column = {1: "Product Revenue", 2: "Quantity"}
            grouped_df = grouped_df.sort_values(by=sort_column[sort_by], ascending=False)
            grouped_df = grouped_df[["Product Brand", sort_column[sort_by]]]
            grouped_df = grouped_df.head(top_brand).to_dict(orient="records")
            response = {"message": "success", "data": grouped_df}
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
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}, "status.status": 7}
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
            data["taxableAmount"] = data['accounting'].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]) * conversion_rate)
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
        
        try:
            assert False
            query = "SELECT _id, sellers, cartLogs, sessionId FROM cart"
            date_time_query = "WHERE cartLogs.timestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            store_filter_query = ""
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
            cart_session_count = len(cart_index.session_id) if cart_index.shape[0] else 0
            cart_index = cart_index[cart_index.status == 3]
            checkout_count = len(cart_index.session_id) if cart_index.shape[0] else 0
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
