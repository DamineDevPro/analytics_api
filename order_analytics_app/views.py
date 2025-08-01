from django.shortcuts import render
import sys
import pandas as pd
import numpy as np
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext
# from pyspark.sql import functions as F
import ast
import datetime
from analytics.settings import  UTC, BASE_CURRENCY, db, CURRENCY_API
# from currency_converter import CurrencyConverter
import requests
import json
import os, sys
from analytics.function import Process
from dateutil.relativedelta import relativedelta
import pytz
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()


class CurrentReport(APIView):
    def get(self, request):
        try:
            column_dict = {
                1: 'Store Name', 2: 'Store Phone', 3: 'Store Email', 4: 'Store Type', 5: 'Payment Method',
                6: 'Pay By Wallet', 7: 'Order Id', 8: 'Order Type', 9: 'Order Status', 10: 'Order Completion',
                11: 'Order Dispatch Type', 12: 'Customer Name', 13: 'Customer Email', 14: 'Customer Type',
                15: 'Gross Order Value', 16: 'Offer Discount Value', 17: 'Promo Discount Value',
                18: 'Gross Value(After offer Discount)', 19: 'Add On Amount', 21: 'Delivery Fee',
                22: 'Tax Value', 23: 'Value(After tax and Discount)', 25: 'Final Total', 26: 'App Earning',
                27: 'App Earning With Tax', 28: 'Store Earning', 29: 'Pickup Locality', 30: 'Pickup City',
                31: 'Pickup Postal Code', 32: 'Pickup Country', 33: 'shipping Locality', 34: 'Shipping City',
                35: 'Shipping Postal Code', 36: 'Shipping State', 37: 'Billing Locality', 38: 'Billing City',
                39: 'Billing State'}

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

            try:
                time_zone = pytz.timezone(request.GET.get('time_zone', "Asia/Calcutta"))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # Column names
            default_agg_columns = [26, 27, 28, 15, 16, 18, 19, 21, 22, 23, 25]

            if "column" not in request.GET:
                requested_columns = default_agg_columns
            else:
                requested_columns = request.GET["column"]
                try:
                    requested_columns = ast.literal_eval(requested_columns)
                except:
                    response = {"message": "Column must be a list",
                                "example": [15, 16, 17, 18, 19, 21]}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                for value in requested_columns:
                    if not isinstance(value, int):
                        response = {"message": "column value must be integer only"}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                    elif value not in list(column_dict.keys()):
                        response = {"message": "columns out of scope"}
                        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                    else:
                        pass
            aggregate_columns = [column for column in requested_columns if column in default_agg_columns]
            non_aggregate_columns = [column for column in requested_columns if column not in default_agg_columns]

            #         start_timestamp and end_timestamp request
            if "start_timestamp" not in request.GET and "end_timestamp" not in request.GET:
                start_timestamp = datetime.date.today()
                end_timestamp = start_timestamp - datetime.timedelta(days=-1)
                start_timestamp = datetime.datetime(
                    year=start_timestamp.year, month=start_timestamp.month, day=start_timestamp.day)
                start_timestamp = int(datetime.datetime.timestamp(start_timestamp))
                end_timestamp = datetime.datetime(
                    year=end_timestamp.year, month=end_timestamp.month, day=end_timestamp.day)
                end_timestamp = int(datetime.datetime.timestamp(end_timestamp))
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

            # Skip and Limit
            try:
                skip = int(request.GET.get("skip", 0))
                limit = int(request.GET.get("limit", 100))
            except:
                response = {"message": "skip and limit must be integer type"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            try:
                export = int(request.GET.get("export", 0))
                if export not in [0, 1]:
                    response = {'message': 'unsupported export, only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # order by
            if ("order_by" not in request.GET) and ("ascending" not in request.GET):
                order_by = "dateTime"
                ascending = True
            else:
                try:
                    order_by = int(request.GET["order_by"])
                    ascending = int(request.GET["ascending"])
                except:
                    response = {"message": "order_by and ascending must be integer"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                if order_by not in requested_columns:
                    response = {"message": "ordering out of scope"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if ascending not in [0, 1]:
                    response = {"message": "ascending must be 0 or 1"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                order_by = column_dict[order_by]
                ascending = bool(ascending)

            # currency
            currency_symbol = request.GET.get("currency_symbol", "₹")
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
                        conversion_rate = float(currency_data.get("data").get('data')[0].get('exchange_rate'))
                    else:
                        response = {"message": "currency conversion not found"}
                        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                    # conversion_rate = CurrencyConverter().convert(1, BASE_CURRENCY, currency)
                except:
                    response = {'message': 'Internal server issue with exchange rate API'}
                    return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # Query
            date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp,
                                                                                 end_timestamp)
            query = "SELECT * from storeOrder" + " " + date_range_query + store_categories_query + store_query
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            if not result_data.shape[0]:
                response = {"message": "Data not found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            result_data['createdDate'] = result_data.createdDate.apply(
                lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

            # Aggregation Process
            agg_column_value = {
                15: 'unitPrice', 16: 'offerDiscount', 17: 'promoDiscount',
                18: 'finalUnitPrice', 19: 'addOnsAmount', 20: 'taxableAmount', 21: 'deliveryFee',
                22: 'taxAmount', 23: 'subTotal', 25: 'finalTotal', 26: 'appEarning',
                27: 'appEarningWithTax', 28: 'storeEarning'
            }
            result_data["dateTime"] = result_data.createdDate.apply(
                lambda x: datetime.datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            for key in aggregate_columns:
                result_data[agg_column_value[key]] = result_data['accounting'].apply(lambda x: x[agg_column_value[key]])
            result_data["taxableAmount"] = result_data['accounting'].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]))
            result_data["order_status"] = result_data["status"].apply(lambda x: x.statusText)

            table_column = ["dateTime", "order_status", "taxableAmount"]
            table_column.extend([agg_column_value[value] for value in aggregate_columns])

            table_data = result_data[table_column]
            agg_order = ["order_status_" + col for col in list(table_data["order_status"].unique()) if
                         col not in ["Cancelled", "Completed"]]
            table_data = pd.get_dummies(table_data, columns=["order_status"])
            alter_columns = [agg_column_value[value] for value in aggregate_columns]
            alter_columns.append("taxableAmount")

            for zero_col in agg_order:
                if zero_col in list(table_data.columns):
                    for col in alter_columns:
                        table_data[col] = np.where(table_data[zero_col] == 1, 0, table_data[col])
            if "order_status_Cancelled" in list(table_data.columns):
                for col in alter_columns:
                    table_data[col] = np.where(table_data.order_status_Cancelled == 1, 0, table_data[col])
                    table_data[col] = table_data[col].astype(float) * conversion_rate

            table_data = table_data.drop(agg_order, axis=1)
            table_data = table_data.rename(columns={"order_status_Cancelled": "Cancelled",
                                                    "order_status_Completed": "order"})

            aggregate_data = table_data.groupby(['dateTime']).sum().reset_index()

            aggregate_data = Process.filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                            time_zone=time_zone, data_frame=aggregate_data,
                                            date_column="dateTime", group_by=0)

            aggregate_data = aggregate_data.sort_values(by="dateTime", ascending=ascending)

            graph = aggregate_data[["dateTime", "taxableAmount"]]

            rename_columns = {"unitPrice": "Gross Order Value({})".format(currency_symbol),
                              "deliveryFee": "Delivery Fee({})".format(currency_symbol),
                              "finalTotal": "Final Order Value({})".format(currency_symbol),
                              "finalUnitPrice": "Gross Value(After offer Discount)({})".format(currency_symbol),
                              "offerDiscount": "Offer Discount Value({})".format(currency_symbol),
                              "subTotal": "Value(After tax and Discount)({})".format(currency_symbol),
                              "taxAmount": "Tax Value({})".format(currency_symbol),
                              "taxableAmount": "Total Sales({})".format(currency_symbol),
                              "addOnsAmount": "Add On Amount({})".format(currency_symbol),
                              "promoDiscount": "Promo Discount({})".format(currency_symbol),
                              "appEarning": "App Earning({})".format(currency_symbol),
                              "appEarningWithTax": "App Earning With Tax({})".format(currency_symbol),
                              "storeEarning": "Store Earning({})".format(currency_symbol)}
            aggregate_data = aggregate_data.rename(columns=rename_columns)
            summary = dict(aggregate_data.sum(axis=0, numeric_only=True))

            # added null columns
            final_column = [column_dict[col] for col in non_aggregate_columns]
            final_column.reverse()
            final_column.extend(list(aggregate_data.columns))
            if "dateTime" in final_column: final_column.remove("dateTime")
            final_column.insert(0, "dateTime")
            if "order" in final_column: final_column.remove("order")
            final_column.insert(1, "order")
            if "Total Sales({})".format(currency_symbol) in final_column: final_column.remove(
                "Total Sales({})".format(currency_symbol))
            final_column.append("Total Sales({})".format(currency_symbol))

            aggregate_data = aggregate_data.reindex(columns=final_column)

            aggregate_data = aggregate_data.sort_values(by=order_by, ascending=ascending)
            if "Cancelled" not in aggregate_data.columns:
                aggregate_data["Cancelled"] = 0
            if "order" not in aggregate_data.columns:
                aggregate_data["order"] = 0
            aggregate_data.fillna(value="N/A", inplace=True)
            new_summary = {}
            for col in aggregate_data.columns:
                if col not in list(summary.keys()):
                    new_summary[col] = 'N/A'
                else:
                    new_summary[col] = summary[col]
            total_count = int(aggregate_data.shape[0])
            aggregate_data = aggregate_data.to_dict(orient="records")
            graph = {"xcat": list(graph["dateTime"]),
                     "series": [{"data": list(graph["taxableAmount"]),
                                 "name": "Total Sales ({})".format(currency_symbol)}]}
            # added skip and limit
            if export:
                data = {"table": aggregate_data}
                response = {"message": "success", "data": data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                show_message = 0
                if len(aggregate_data) > 1000:
                    show_message = 1
                aggregate_data = aggregate_data[skip * limit: (skip * limit) + limit]
                data = {"graph": graph, "table": aggregate_data, "summary": new_summary, "count": total_count}
                response = {"message": "success", "data": data, "show_message": show_message}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": "Internal server error"}
            return JsonResponse(error, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class HistoricalReport(APIView):
    def get(self, request):
        try:
            column_dict = {
                1: 'Store Name', 2: 'Store Phone', 3: 'Store Email', 4: 'Store Type', 5: 'Payment Method',
                6: 'Pay By Wallet', 7: 'Order Id', 8: 'Order Type', 9: 'Order Status', 10: 'Order Completion',
                11: 'Order Dispatch Type', 12: 'Customer Name', 13: 'Customer Email', 14: 'Customer Type',
                15: 'Gross Order Value', 16: 'Offer Discount Value', 17: 'Promo Discount Value',
                18: 'Gross Value(After offer Discount)', 19: 'Add On Amount', 21: 'Delivery Fee',
                22: 'Tax Value', 23: 'Value(After tax and Discount)', 25: 'Final Total', 26: 'App Earning',
                27: 'App Earning With Tax', 28: 'Store Earning', 29: 'Pickup Locality', 30: 'Pickup City',
                31: 'Pickup Postal Code', 32: 'Pickup Country', 33: 'shipping Locality', 34: 'Shipping City',
                35: 'Shipping Postal Code', 36: 'Shipping State', 37: 'Billing Locality', 38: 'Billing City',
                39: 'Billing State'}

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
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                              4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                if group_by not in list(group_by_value.keys()):
                    response = {
                        "message": "'group_by' value must be in range(0, {})".format(
                            max(list(group_by_value.keys())))}
                    return JsonResponse(response, safe=False, status=422)
            except:
                return JsonResponse({"message": "'group_by' must be integer"}, safe=False, status=422)

            # default_agg_columns = [15, 16, 17, 18, 19, 21, 22, 23, 25, 26, 27, 28]
            default_agg_columns = [26, 27, 28, 15, 16, 18, 19, 21, 22, 23, 25]

            if "column" not in request.GET:
                requested_columns = default_agg_columns
            else:
                requested_columns = request.GET["column"]
                try:
                    requested_columns = ast.literal_eval(requested_columns)
                except:
                    response = {"message": "Column must be a list",
                                "example": [15, 16, 17, 18, 19, 21]}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                for value in requested_columns:
                    if not isinstance(value, int):
                        response = {"message": "column value must be integer only"}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                    elif value not in list(column_dict.keys()):
                        response = {"message": "columns out of scope"}
                        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                    else:
                        pass
            aggregate_columns = [column for column in requested_columns if column in default_agg_columns]
            non_aggregate_columns = [column for column in requested_columns if column not in default_agg_columns]

            #         start_timestamp and end_timestamp request
            if "start_timestamp" not in request.GET and "end_timestamp" not in request.GET:
                response = {"message": "mandatory fields are missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

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

            # Skip and Limit
            try:
                skip = int(request.GET.get("skip", 0))
                limit = int(request.GET.get("limit", 100))
            except:
                response = {"message": "skip and limit must be integer type"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # order by
            if ("order_by" not in request.GET) and ("ascending" not in request.GET):
                order_by = "dateTime"
                ascending = True
            else:
                try:
                    order_by = int(request.GET["order_by"])
                    ascending = int(request.GET["ascending"])
                except:
                    response = {"message": "order_by and ascending must be integer"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                if order_by not in requested_columns:
                    response = {"message": "ordering out of scope"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if ascending not in [0, 1]:
                    response = {"message": "ascending must be 0 or 1"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                order_by = column_dict[order_by]
                ascending = bool(ascending)

            # currency
            currency_symbol = request.GET.get("currency_symbol", "₹")
            try:
                currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
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
                        conversion_rate = float(currency_data.get("data").get('data')[0].get('exchange_rate'))
                    else:
                        response = {"message": "currency conversion not found"}
                        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                    # conversion_rate = CurrencyConverter().convert(1, BASE_CURRENCY, currency)
                except:
                    response = {'message': 'Internal server issue with exchange rate API'}
                    return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            try:
                export = int(request.GET.get("export", 0))
                if export not in [0, 1]:
                    response = {'message': 'unsupported export, only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            date_range_query = "WHERE timeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT * from totalSales" + " " + date_range_query
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            if not result_data.shape[0]:
                response = {'message': 'No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['period'] = result_data.period.apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

            agg_column_value = {
                15: 'unitPrice', 16: 'offerDiscount', 17: 'promoDiscount',
                18: 'finalUnitPrice', 19: 'addOnsAmount', 20: 'taxableAmount', 21: 'deliveryFee',
                22: 'taxAmount', 23: 'subTotal', 25: 'finalTotal', 26: 'appEarning',
                27: 'appEarningWithTax', 28: 'storeEarning'
            }
            result_data["dateTime"] = result_data.period.apply(
                lambda x: datetime.datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            result_data = Process.date_conversion(group_by=group_by, data_frame=result_data, date_col="dateTime")

            table_column = ["dateTime", "taxableAmount", "order", "cancelled"]
            table_column.extend([agg_column_value[value] for value in aggregate_columns])
            for col in [agg_column_value[value] for value in aggregate_columns]:
                result_data[col] = result_data[col].astype(float).apply(lambda x: x * conversion_rate)
            result_data["taxableAmount"] = result_data["taxableAmount"].astype(float).apply(
                lambda x: x * conversion_rate)
            table_data = result_data[table_column]

            aggregate_data = table_data.groupby(['dateTime']).sum().reset_index()

            aggregate_data = Process.filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                            time_zone=time_zone, data_frame=aggregate_data,
                                            date_column="dateTime", group_by=group_by)

            aggregate_data = aggregate_data.sort_values(by="dateTime", ascending=ascending)

            if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            graph = aggregate_data[["dateTime", "taxableAmount"]]

            rename_columns = {"unitPrice": "Gross Order Value({})".format(currency_symbol),
                              "deliveryFee": "Delivery Fee({})".format(currency_symbol),
                              "finalTotal": "Final Order Value({})".format(currency_symbol),
                              "finalUnitPrice": "Gross Value(After offer Discount)({})".format(currency_symbol),
                              "offerDiscount": "Offer Discount Value({})".format(currency_symbol),
                              "subTotal": "Value(After tax and Discount)({})".format(currency_symbol),
                              "taxAmount": "Tax Value({})".format(currency_symbol),
                              "taxableAmount": "Total Sales({})".format(currency_symbol),
                              "addOnsAmount": "Add On Amount({})".format(currency_symbol),
                              "promoDiscount": "Promo Discount({})".format(currency_symbol),
                              "appEarning": "App Earning({})".format(currency_symbol),
                              "appEarningWithTax": "App Earning With Tax({})".format(currency_symbol),
                              "storeEarning": "Store Earning({})".format(currency_symbol)}
            aggregate_data = aggregate_data.rename(columns=rename_columns)
            summary = dict(aggregate_data.sum(axis=0, numeric_only=True))

            # added null columns
            final_column = [column_dict[col] for col in non_aggregate_columns]
            final_column.reverse()
            final_column.extend(list(aggregate_data.columns))
            if "dateTime" in final_column: final_column.remove("dateTime")
            final_column.insert(0, "dateTime")
            if "order" in final_column: final_column.remove("order")
            final_column.insert(1, "order")
            if "Total Sales({})".format(currency_symbol) in final_column: final_column.remove(
                "Total Sales({})".format(currency_symbol))
            final_column.append("Total Sales({})".format(currency_symbol))

            aggregate_data = aggregate_data.reindex(columns=final_column)

            aggregate_data = aggregate_data.sort_values(by=order_by, ascending=ascending)

            if order_by == "dateTime":
                if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
                if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data,
                                                                        date_column="dateTime")
                if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            if "Cancelled" not in aggregate_data.columns:
                aggregate_data["Cancelled"] = 0
            if "order" not in aggregate_data.columns:
                aggregate_data["order"] = 0
            aggregate_data.fillna(value="N/A", inplace=True)
            new_summary = {}
            for col in aggregate_data.columns:
                if col not in list(summary.keys()):
                    new_summary[col] = 'N/A'
                else:
                    new_summary[col] = summary[col]
            total_count = int(aggregate_data.shape[0])
            aggregate_data = aggregate_data.to_dict(orient="records")
            graph = {"xcat": list(graph["dateTime"]),
                     "series": [{"data": list(graph["taxableAmount"]),
                                 "name": "Total Sales ({})".format(currency_symbol)}]}
            # added skip and limit
            if export:
                data = {"table": aggregate_data}
                response = {"message": "success", "data": data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                show_message = 0
                if len(aggregate_data) > 1000:
                    show_message = 1
                aggregate_data = aggregate_data[skip * limit: (skip * limit) + limit]
                data = {"graph": graph, "table": aggregate_data, "summary": new_summary, "count": total_count}
                response = {"message": "success", "data": data, "show_message": show_message}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": "Internal server error"}
            return JsonResponse(error, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class MergeReport(APIView):
    def get(self, request):
        try:
            column_dict = {
                1: 'Store Name', 2: 'Store Phone', 3: 'Store Email', 4: 'Store Type', 5: 'Payment Method',
                6: 'Pay By Wallet', 7: 'Order Id', 8: 'Order Type', 9: 'Order Status', 10: 'Order Completion',
                11: 'Order Dispatch Type', 12: 'Customer Name', 13: 'Customer Email', 14: 'Customer Type',
                15: 'Gross Order Value', 16: 'Offer Discount Value', 17: 'Promo Discount Value',
                18: 'Gross Value(After offer Discount)', 19: 'Add On Amount', 21: 'Delivery Fee',
                22: 'Tax Value', 23: 'Value(After tax and Discount)', 25: 'Final Total', 26: 'App Earning',
                27: 'App Earning With Tax', 28: 'Store Earning', 29: 'Pickup Locality', 30: 'Pickup City',
                31: 'Pickup Postal Code', 32: 'Pickup Country', 33: 'shipping Locality', 34: 'Shipping City',
                35: 'Shipping Postal Code', 36: 'Shipping State', 37: 'Billing Locality', 38: 'Billing City',
                39: 'Billing State'}

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            time_zone = request.GET.get('time_zone', "Asia/Calcutta")
            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                              4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                if group_by not in list(group_by_value.keys()):
                    response = {
                        "message": "'group_by' value must be in range(0, {})".format(
                            max(list(group_by_value.keys())))}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                return JsonResponse({"message": "'group_by' must be integer"},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            default_agg_columns = [26, 27, 28, 15, 16, 18, 19, 21, 22, 23, 25]

            if "column" not in request.GET:
                requested_columns = default_agg_columns
            else:
                requested_columns = request.GET["column"]
                try:
                    requested_columns = ast.literal_eval(requested_columns)
                except:
                    response = {"message": "Column must be a list",
                                "example": [15, 16, 17, 18, 19, 21]}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                for value in requested_columns:
                    if not isinstance(value, int):
                        response = {"message": "column value must be integer only"}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                    elif value not in list(column_dict.keys()):
                        response = {"message": "columns out of scope"}
                        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                    else:
                        pass
            aggregate_columns = [column for column in requested_columns if column in default_agg_columns]
            non_aggregate_columns = [column for column in requested_columns if column not in default_agg_columns]

            #         start_timestamp and end_timestamp request
            if "start_timestamp" not in request.GET and "end_timestamp" not in request.GET:
                response = {"message": "mandatory fields are missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

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

            current_date = datetime.datetime.utcnow()
            current_date = datetime.datetime(day=current_date.day, month=current_date.month, year=current_date.year)
            current_date = current_date.replace(tzinfo=UTC)
            current_timestamp = int(datetime.datetime.timestamp(current_date))

            # Skip and Limit
            try:
                skip = int(request.GET.get("skip", 0))
                limit = int(request.GET.get("limit", 100))
            except:
                response = {"message": "skip and limit must be integer type"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # order by
            if ("order_by" not in request.GET) and ("ascending" not in request.GET):
                order_by = "dateTime"
                ascending = True
            else:
                try:
                    order_by = int(request.GET["order_by"])
                    ascending = int(request.GET["ascending"])
                except:
                    response = {"message": "order_by and ascending must be integer"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                if order_by not in requested_columns:
                    response = {"message": "ordering out of scope"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if ascending not in [0, 1]:
                    response = {"message": "ascending must be 0 or 1"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                order_by = column_dict[order_by]
                ascending = bool(ascending)

            # currency
            currency_symbol = request.GET.get("currency_symbol", "₹")
            try:
                currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
            except:
                response = {'message': 'Bad request Currency'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            if currency == BASE_CURRENCY:
                convert = 0

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
                    convert = 1
                except:
                    response = {'message': 'Internal server issue with exchange rate API'}
                    return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            try:
                export = int(request.GET.get("export", 0))
                if export not in [0, 1]:
                    response = {'message': 'unsupported export, only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # Query
            date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(current_timestamp, end_timestamp)
            query = "SELECT * from storeOrder" + " " + date_range_query
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()

            agg_column_value = {
                15: 'unitPrice', 16: 'offerDiscount', 17: 'promoDiscount',
                18: 'finalUnitPrice', 19: 'addOnsAmount', 20: 'taxableAmount', 21: 'deliveryFee',
                22: 'taxAmount', 23: 'subTotal', 25: 'finalTotal', 26: 'appEarning',
                27: 'appEarningWithTax', 28: 'storeEarning'
            }
            if result_data.shape[0]:
                result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
                result_data['createdDate'] = result_data.createdDate.apply(
                    lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

                # Aggregation Process

                result_data["dateTime"] = result_data.createdDate.apply(
                    lambda x: datetime.datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

                for key in aggregate_columns:
                    result_data[agg_column_value[key]] = result_data['accounting'].apply(
                        lambda x: x[agg_column_value[key]])
                result_data["taxableAmount"] = result_data['accounting'].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]))
                result_data["order_status"] = result_data["status"].apply(lambda x: x.statusText)

                table_column = ["dateTime", "order_status", "taxableAmount"]
                table_column.extend([agg_column_value[value] for value in aggregate_columns])

                table_data = result_data[table_column]
                agg_order = ["order_status_" + col for col in list(table_data["order_status"].unique()) if
                             col not in ["Cancelled", "Completed"]]
                table_data = pd.get_dummies(table_data, columns=["order_status"])
                alter_columns = [agg_column_value[value] for value in aggregate_columns]
                alter_columns.append("taxableAmount")

                for zero_col in agg_order:
                    if zero_col in list(table_data.columns):
                        for col in alter_columns:
                            table_data[col] = np.where(table_data[zero_col] == 1, 0, table_data[col])
                if "order_status_Cancelled" in list(table_data.columns):
                    for col in alter_columns:
                        table_data[col] = np.where(table_data.order_status_Cancelled == 1, 0, table_data[col])

                table_data = table_data.drop(agg_order, axis=1)
                current_data = table_data.rename(columns={"order_status_Cancelled": "Cancelled",
                                                          "order_status_Completed": "order"})

                current_data = current_data.rename(columns={"order_status_Cancelled": "Cancelled"})
                current_data = current_data.groupby(['dateTime']).sum().reset_index()
            else:
                current_data = pd.DataFrame()

            # ######################################### HISTORICAL TABLE ###############################################
            date_range_query = "WHERE timeStamp BETWEEN {} AND {}".format(start_timestamp, current_timestamp)
            query = "SELECT * from totalSales" + " " + date_range_query
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            if current_data.shape[0] == 0 and result_data.shape[0] == 0:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            result_data['period'] = pd.to_datetime(result_data['period'])
            result_data['period'] = result_data.period.apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

            result_data["dateTime"] = result_data.period.apply(
                lambda x: datetime.datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            # drop
            historical_data = result_data.drop(["_id", "timeStamp"], axis=1)

            if current_data.shape[0]:
                aggregate_data = pd.concat([historical_data, current_data], axis=0)
            else:
                aggregate_data = historical_data.copy()

            aggregate_data = Process.date_conversion(group_by=group_by, data_frame=aggregate_data, date_col="dateTime")

            table_column = ["dateTime", "taxableAmount", "order", "cancelled"]
            table_column.extend([agg_column_value[value] for value in aggregate_columns])
            if convert:
                for col in [agg_column_value[value] for value in aggregate_columns]:
                    aggregate_data[col] = aggregate_data[col].astype(float).apply(lambda x: x * conversion_rate)
                aggregate_data["taxableAmount"] = aggregate_data["taxableAmount"].astype(float).apply(
                    lambda x: x * conversion_rate)

            aggregate_data = aggregate_data[table_column]
            aggregate_data = aggregate_data.groupby(['dateTime']).sum().reset_index()

            aggregate_data = Process.filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                            time_zone=time_zone, data_frame=aggregate_data,
                                            date_column="dateTime", group_by=group_by)

            aggregate_data = aggregate_data.sort_values(by="dateTime", ascending=ascending)
            summary = dict(aggregate_data.sum(axis=0, numeric_only=True))

            if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            graph = aggregate_data[["dateTime", "taxableAmount"]]

            rename_columns = {"unitPrice": "Gross Order Value({})".format(currency_symbol),
                              "deliveryFee": "Delivery Fee({})".format(currency_symbol),
                              "finalTotal": "Final Order Value({})".format(currency_symbol),
                              "finalUnitPrice": "Gross Value(After offer Discount)({})".format(currency_symbol),
                              "offerDiscount": "Offer Discount Value({})".format(currency_symbol),
                              "subTotal": "Value(After tax and Discount)({})".format(currency_symbol),
                              "taxAmount": "Tax Value({})".format(currency_symbol),
                              "taxableAmount": "Total Sales({})".format(currency_symbol),
                              "addOnsAmount": "Add On Amount({})".format(currency_symbol),
                              "promoDiscount": "Promo Discount({})".format(currency_symbol),
                              "appEarning": "App Earning({})".format(currency_symbol),
                              "appEarningWithTax": "App Earning With Tax({})".format(currency_symbol),
                              "storeEarning": "Store Earning({})".format(currency_symbol)}
            aggregate_data = aggregate_data.rename(columns=rename_columns)
            summary = dict(aggregate_data.sum(axis=0, numeric_only=True))

            # added null columns
            final_column = [column_dict[col] for col in non_aggregate_columns]
            final_column.reverse()
            final_column.extend(list(aggregate_data.columns))
            if "dateTime" in final_column: final_column.remove("dateTime")
            final_column.insert(0, "dateTime")
            if "order" in final_column: final_column.remove("order")
            final_column.insert(1, "order")
            if "Total Sales({})".format(currency_symbol) in final_column: final_column.remove(
                "Total Sales({})".format(currency_symbol))
            final_column.append("Total Sales({})".format(currency_symbol))

            aggregate_data = aggregate_data.reindex(columns=final_column)

            aggregate_data = aggregate_data.sort_values(by=order_by, ascending=ascending)
            if order_by == "dateTime":

                if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
                if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data,
                                                                        date_column="dateTime")
                if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            if "Cancelled" not in aggregate_data.columns:
                aggregate_data["Cancelled"] = 0
            if "order" not in aggregate_data.columns:
                aggregate_data["order"] = 0
            aggregate_data.fillna(value="N/A", inplace=True)
            new_summary = {}
            for col in aggregate_data.columns:
                if col not in list(summary.keys()):
                    new_summary[col] = 'N/A'
                else:
                    new_summary[col] = summary[col]

            total_count = int(aggregate_data.shape[0])

            aggregate_data = aggregate_data.to_dict(orient="records")

            if export:
                data = {"table": aggregate_data}
                response = {"message": "success", "data": data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                show_message = 0
                if aggregate_data.shape[0] > 1000:
                    show_message = 1
                graph = {"xcat": list(graph["dateTime"]),
                         "series": [{"data": list(graph["taxableAmount"]),
                                     "name": "Total Sales ({})".format(currency_symbol)}]}
                # added skip and limit
                aggregate_data = aggregate_data[skip * limit: (skip * limit) + limit]
                data = {"graph": graph, "table": aggregate_data, "summary": new_summary, "count": total_count}
                response = {"message": "success", "data": data, "show_message": show_message}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": "Internal server error"}
            return JsonResponse(error, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class FilterReport(APIView):
    def get(self, request):
        try:
            column_dict = {
                1: 'Store Name', 2: 'Store Phone', 3: 'Store Email', 4: 'Store Type', 5: 'Payment Method',
                6: 'Pay By Wallet', 7: 'Order Id', 8: 'Order Type', 9: 'Order Status', 10: 'Order Completion',
                11: 'Order Dispatch Type', 12: 'Customer Name', 13: 'Customer Email', 14: 'Customer Type',
                15: 'Gross Order Value', 16: 'Offer Discount Value', 17: 'Promo Discount Value',
                18: 'Gross Value(After offer Discount)', 19: 'Add On Amount', 21: 'Delivery Fee',
                22: 'Tax Value', 23: 'Value(After tax and Discount)', 25: 'Final Total', 26: 'App Earning',
                27: 'App Earning With Tax', 28: 'Store Earning', 29: 'Pickup Locality', 30: 'Pickup City',
                31: 'Pickup Postal Code', 32: 'Pickup Country', 33: 'shipping Locality', 34: 'Shipping City',
                35: 'Shipping Postal Code', 36: 'Shipping State', 37: 'Billing Locality', 38: 'Billing City',
                39: 'Billing State'}

            column_key_dict = {
                1: 'storeName', 2: 'storePhone', 3: 'storeEmail', 4: 'storeTypeMsg', 5: 'paymentTypeText',
                6: 'payByWallet', 7: '_id', 8: 'orderTypeMsg', 9: 'status.statusText', 10: 'status.partialStatus',
                11: 'autoDispatch', 12: 'customerDetails.firstName', 13: 'customerDetails.email',
                14: 'customerDetails.userTypeText', 15: 'accounting.unitPrice', 16: 'accounting.offerDiscount',
                17: 'accounting.promoDiscount', 18: 'accounting.finalUnitPrice', 19: 'accounting.addOnsAmount',
                21: 'accounting.deliveryFee', 22: 'accounting.taxAmount', 23: 'accounting.subTotal',
                25: 'accounting.finalTotal', 26: 'accounting.appEarning', 27: 'accounting.appEarningWithTax',
                28: 'accounting.storeEarning', 29: 'pickupAddress.locality', 30: 'pickupAddress.cityName',
                31: 'pickupAddress.postal_code', 32: 'pickupAddress.country', 33: 'deliveryAddress.locality',
                34: 'deliveryAddress.city', 35: 'deliveryAddress.pincode', 36: 'deliveryAddress.state',
                37: 'billingAddress.locality', 38: 'billingAddress.city', 39: 'billingAddress.state'}

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)
            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            try:
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # Group  by
            group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month",
                              4: "quarter", 5: 'year', 6: "hour_of_day", 7: "day_of_week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                if group_by not in list(group_by_value.keys()):
                    response = {
                        "message": "'group_by' value must be in range(0, {})".format(
                            max(list(group_by_value.keys())))}
                    return JsonResponse(response, safe=False, status=422)
            except:
                return JsonResponse({"message": "Unsupported 'group_by'", "support": group_by_value}, safe=False,
                                    status=422)

            # Column names
            default_agg_columns = [26, 27, 28, 15, 16, 18, 19, 21, 22, 23, 25]

            if "column" not in request.GET:
                requested_columns = default_agg_columns
            else:
                requested_columns = request.GET["column"]
                try:
                    requested_columns = ast.literal_eval(requested_columns)
                except:
                    response = {"message": "Column must be a list",
                                "example": [15, 16, 17, 18, 19, 21]}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                for value in requested_columns:
                    if not isinstance(value, int):
                        response = {"message": "column value must be integer only"}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                    elif value not in list(column_dict.keys()):
                        response = {"message": "columns out of scope"}
                        return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                    else:
                        pass
            aggregate_columns = [column for column in requested_columns if column in default_agg_columns]
            non_aggregate_columns = [column for column in requested_columns if column not in default_agg_columns]

            #         start_timestamp and end_timestamp request
            if "start_timestamp" not in request.GET and "end_timestamp" not in request.GET:
                start_timestamp = datetime.date.today()
                end_timestamp = start_timestamp - datetime.timedelta(days=-1)
                start_timestamp = datetime.datetime(
                    year=start_timestamp.year, month=start_timestamp.month, day=start_timestamp.day)
                start_timestamp = int(datetime.datetime.timestamp(start_timestamp))
                end_timestamp = datetime.datetime(
                    year=end_timestamp.year, month=end_timestamp.month, day=end_timestamp.day)
                end_timestamp = int(datetime.datetime.timestamp(end_timestamp))
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

            # Skip and Limit
            try:
                skip = int(request.GET.get("skip", 0))
                limit = int(request.GET.get("limit", 100))
            except:
                response = {"message": "skip and limit must be integer type"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            try:
                export = int(request.GET.get("export", 0))
                if export not in [0, 1]:
                    response = {'message': 'unsupported export, only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # order by
            if ("order_by" not in request.GET) and ("ascending" not in request.GET):
                order_by = "dateTime"
                ascending = True
            else:
                try:
                    order_by = int(request.GET["order_by"])
                    ascending = int(request.GET["ascending"])
                except:
                    response = {"message": "order_by and ascending must be integer"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                if order_by not in requested_columns:
                    response = {"message": "ordering out of scope"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if ascending not in [0, 1]:
                    response = {"message": "ascending must be 0 or 1"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                order_by = column_dict[order_by]
                ascending = bool(ascending)

            # filter
            if "filter" not in request.GET:
                filter_query = ""
            else:
                filter_request = request.GET["filter"]
                try:
                    filter_request = ast.literal_eval(filter_request)

                except:
                    return JsonResponse({"message": "filter must be list"}, safe=False,
                                        status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                if not isinstance(filter_request, list):
                    response = {"message": "filter must be list",
                                "example": [{"column": "storeTypeMsg", "condition_operator": 0,
                                             "parameter_value": "e-CommercePartner"},
                                            {"column": "orderTypeMsg", "condition_operator": 1,
                                             "parameter_value": "Delivery"}]}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                filter_query = ""
                for value in filter_request:
                    try:
                        column = int(value["column"])
                        condition_operator = int(value["condition_operator"])
                        if condition_operator not in [0, 1]:
                            response = {"message": "condition operator must be 0 or 1"}
                            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                        elif condition_operator:
                            condition_operator = "IN"
                        else:
                            condition_operator = "NOT IN"
                        parameter_value = value["parameter_value"]
                        if not parameter_value:
                            response = {'message': 'parameter should not be empty'}
                            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                        if not isinstance(parameter_value, list):
                            response = {"message": 'parameter_value should be list'}
                            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                        if len(parameter_value) > 1:
                            add_column_query = '{column} {operator} {parameter_value}'.format(
                                column=column_key_dict[column], operator=condition_operator,
                                parameter_value=tuple(parameter_value))
                        else:
                            add_column_query = '{column} {operator} ("{parameter_value}")'.format(
                                column=column_key_dict[column], operator=condition_operator,
                                parameter_value=parameter_value[0])
                        if filter_query == "":
                            filter_query = "AND {}".format(add_column_query)
                        else:
                            filter_query = filter_query + " AND " + add_column_query
                        filter_query = " " + filter_query
                    except:
                        response = {"message": "filter must be list",
                                    "example": [{
                                        "column": "storeTypeMsg",
                                        "condition_operator": 0,
                                        "parameter_value": ["e-CommercePartner"]},
                                        {
                                            "column": "orderTypeMsg",
                                            "condition_operator": 1,
                                            "parameter_value": ["search 1", "search 2"]}]}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # currency
            currency_symbol = request.GET.get("currency_symbol", "₹")
            if 'currency' not in request.GET:
                convert = 0
                currency = BASE_CURRENCY
            else:
                try:
                    currency = str(request.GET["currency"]).upper()
                except:
                    response = {'message': 'Bad request Currency'}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if currency == BASE_CURRENCY:
                    convert = 0
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
                        # conversion_rate = CurrencyConverter().convert(1, 'INR', currency)
                        convert = 1
                    except:
                        response = {'message': 'Internal server issue with exchange rate API'}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""
            store_categories_query = ""
            if store_categories_id:
                store_categories_query = " AND storecategoryid == '{}' ".format(store_categories_id)

            # Query
            date_range_query = " WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT * from storeOrder" + date_range_query + filter_query + store_categories_query \
                    + store_query
            query = query.strip()
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if request.GET.get("seller"): query['storeId'] = request.GET.get("seller")
                if store_id and store_id != "0": query['storeId'] = store_id
                if store_categories_id and store_categories_id != "0": query['storeCategoryId'] = store_categories_id

                try:
                    filter_request = ast.literal_eval(request.GET.get("filter", "[]"))
                except:
                    return JsonResponse({"message": "filter must be list"}, safe=False,
                                        status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                for value in filter_request:
                    try:
                        column = int(value["column"])
                        condition_operator = int(value["condition_operator"])
                        if condition_operator not in [0, 1]:
                            response = {"message": "condition operator must be 0 or 1"}
                            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                        elif condition_operator:
                            condition_operator = "$in"
                        else:
                            condition_operator = "$nin"
                        parameter_value = value["parameter_value"]
                        if not parameter_value:
                            response = {'message': 'parameter should not be empty'}
                            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                        if not isinstance(parameter_value, list):
                            response = {"message": 'parameter_value should be list'}
                            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

                        query[column_key_dict[column].replace("accounting", accounting)] = {
                            condition_operator: parameter_value}
                    except:
                        response = {"message": "filter must be list",
                                    "example": [{
                                        "column": "storeTypeMsg",
                                        "condition_operator": 0,
                                        "parameter_value": ["e-CommercePartner"]},
                                        {
                                            "column": "orderTypeMsg",
                                            "condition_operator": 1,
                                            "parameter_value": ["search 1", "search 2"]}]}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                print("query ----->", query)
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if not result_data.shape[0]:
                response = {"message": "No data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            result_data['createdDate'] = result_data.createdDate.apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

            # Aggregation Process
            agg_column_value = {
                15: 'unitPrice', 16: 'offerDiscount', 17: 'promoDiscount',
                18: 'finalUnitPrice', 19: 'addOnsAmount', 20: 'taxableAmount', 21: 'deliveryFee',
                22: 'taxAmount', 23: 'subTotal', 25: 'finalTotal', 26: 'appEarning',
                27: 'appEarningWithTax', 28: 'storeEarning'
            } if accounting == "accounting" else {
                15: 'unitPrice', 16: 'offerDiscount', 17: 'promoDiscount',
                18: 'finalUnitPrice', 19: 'addOnsAmount', 20: 'taxableAmount',
                21: 'deliveryFee', 22: 'taxAmount', 23: 'subTotal', 25: 'finalTotal'
                }

            result_data["dateTime"] = result_data.createdDate.apply(
                lambda x: datetime.datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            for key in aggregate_columns:
                if key in agg_column_value.keys():
                    result_data[agg_column_value[key]] = result_data[accounting].apply(
                        lambda x: x[agg_column_value[key]])

            result_data["taxableAmount"] = result_data[accounting].apply(lambda x: (x["taxableAmount"]+x["serviceFeeTotal"]))
            result_data["order_status"] = result_data["status"].apply(lambda x: x["statusText"])

            table_column = ["dateTime", "order_status", "taxableAmount"]
            table_column.extend(
                [agg_column_value[value] for value in aggregate_columns if value in agg_column_value.keys()])

            result_data = Process.date_conversion(group_by=group_by, data_frame=result_data, date_col="dateTime")

            table_data = result_data[table_column]
            agg_order = ["order_status_" + col for col in list(table_data["order_status"].unique()) if
                         col not in ["Cancelled", "Completed"]]
            table_data = pd.get_dummies(table_data, columns=["order_status"])
            alter_columns = [agg_column_value[value] for value in aggregate_columns if value in agg_column_value.keys()]
            alter_columns.append("taxableAmount")

            # for zero_col in agg_order:
            #     if zero_col in list(table_data.columns):
            #         for col in alter_columns:
            #             table_data[col] = np.where(table_data[zero_col] == 1, 0, table_data[col])

            if "order_status_Cancelled" in list(table_data.columns):
                for col in alter_columns:
                    table_data[col] = np.where(table_data.order_status_Cancelled == 1, 0, table_data[col])
            if convert:
                for col in alter_columns:
                    table_data[col] = table_data[col].astype(float).apply(lambda x: x * conversion_rate)
            if  "order_status_Completed" not in list(table_data.columns):
                table_data["order_status_Completed"] = 0
            table_data["order_status_Completed"] = table_data[agg_order + ["order_status_Completed"]].apply(
                lambda x: sum(x), axis=1)
            table_data = table_data.drop(agg_order, axis=1)
            table_data = table_data.rename(columns={"order_status_Cancelled": "Cancelled",
                                                    "order_status_Completed": "order"})

            aggregate_data = table_data.groupby(['dateTime']).sum().reset_index()

            aggregate_data = Process.filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                            time_zone=time_zone, data_frame=aggregate_data,
                                            date_column="dateTime", group_by=group_by)

            aggregate_data = aggregate_data.sort_values(by="dateTime", ascending="dateTime")

            if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            graph = aggregate_data[["dateTime", "taxableAmount"]]

            if store_id:
                aggregate_data = aggregate_data.drop(["appEarning", "appEarningWithTax"], axis=1, errors="ignore")

            rename_columns = {"unitPrice": "Gross Order Value({})".format(currency_symbol),
                              "deliveryFee": "Delivery Fee({})".format(currency_symbol),
                              "finalTotal": "Final Order Value({})".format(currency_symbol),
                              "finalUnitPrice": "Gross Value(After offer Discount)({})".format(currency_symbol),
                              "offerDiscount": "Offer Discount Value({})".format(currency_symbol),
                              "subTotal": "Value(After tax and Discount)({})".format(currency_symbol),
                              "taxAmount": "Tax Value({})".format(currency_symbol),
                              "taxableAmount": "Total Sales({})".format(currency_symbol),
                              "addOnsAmount": "Add On Amount({})".format(currency_symbol),
                              "promoDiscount": "Promo Discount({})".format(currency_symbol),
                              "appEarning": "App Earning({})".format(currency_symbol),
                              "appEarningWithTax": "App Earning With Tax({})".format(currency_symbol),
                              "storeEarning": "Store Earning({})".format(currency_symbol)}
            aggregate_data = aggregate_data.rename(columns=rename_columns)
            summary = dict(aggregate_data.sum(axis=0, numeric_only=True))

            # added null columns
            if not aggregate_data.shape[0]:
                response = {"message": "Data not found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            final_column = [column_dict[col] for col in non_aggregate_columns]
            final_column.reverse()
            final_column.extend(list(aggregate_data.columns))
            if "dateTime" in final_column: final_column.remove("dateTime")
            final_column.insert(0, "dateTime")
            if "order" in final_column: final_column.remove("order")
            final_column.insert(1, "order")
            if "Total Sales({})".format(currency_symbol) in final_column:
                final_column.remove("Total Sales({})".format(currency_symbol))
            final_column.append("Total Sales({})".format(currency_symbol))

            aggregate_data = aggregate_data.reindex(columns=final_column)

            aggregate_data = aggregate_data.sort_values(by=order_by, ascending=ascending)
            if order_by == "dateTime":
                if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
                if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data,
                                                                        date_column="dateTime")
                if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            if "Cancelled" not in aggregate_data.columns:
                aggregate_data["Cancelled"] = 0
            if "order" not in aggregate_data.columns:
                aggregate_data["order"] = 0
            aggregate_data.fillna(value="N/A", inplace=True)
            new_summary = {}
            for col in aggregate_data.columns:
                if col not in list(summary.keys()):
                    new_summary[col] = 'N/A'
                else:
                    new_summary[col] = summary[col]

            total_count = int(aggregate_data.shape[0])
            aggregate_data = aggregate_data.to_dict(orient="records")

            if export:
                data = {"table": aggregate_data}
                response = {"message": "success", "data": data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                graph = {"xcat": list(graph["dateTime"]),
                         "series": [{"data": list(graph["taxableAmount"]),
                                     "name": "Total Sales ({})".format(currency_symbol)}]}
                # added skip and limit
                aggregate_data = aggregate_data[skip * limit: (skip * limit) + limit]
                data = {"graph": graph, "table": aggregate_data, "summary": new_summary, "count": total_count}
                response = {"message": "success", "data": data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": "Internal server error"}
            return JsonResponse(error, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DescriptiveSales(APIView):
    def get(self, request):
        """
        ##############################################################################################
        ######################################### DEPRECATED #########################################
        ##############################################################################################
        descriptive sales report comparison with respect to previous month sales in monthly time frame
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
                store_id = str(request.GET.get("store_categories_id", ""))
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            # currency support
            if 'currency' not in request.GET:
                conversion_rate = 1
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

            today = datetime.datetime.now()
            month_first = datetime.datetime(day=1, month=today.month, year=today.year)
            last_month_first = datetime.datetime(day=1, month=today.month, year=today.year) - relativedelta(months=1)
            last_month_date = datetime.datetime(day=today.day, month=today.month, year=today.year) - \
                              relativedelta(months=1) + datetime.timedelta(days=1)
            today = datetime.datetime.timestamp(today)
            month_first = datetime.datetime.timestamp(month_first)
            last_month_first_d = last_month_first
            last_month_first = datetime.datetime.timestamp(last_month_first)
            last_month_date_d = last_month_date
            last_month_date = datetime.datetime.timestamp(last_month_date)

            # spark sql query to calculate the sum of taxableAmount in current time frame
            current_sum = 'SELECT SUM(accounting.taxableAmount) FROM storeOrder WHERE createdTimeStamp BETWEEN {} AND {} AND status.status == 7'.format(
                int(month_first), int(today))
            current_sum = current_sum + store_query + store_categories_query
            current_sum = sqlContext.sql(current_sum)
            current_sum = current_sum.toPandas().to_dict(orient='records')
            current_sum = list(current_sum[0].values())[0]
            if current_sum is None: current_sum = 0
            current_sum = current_sum * conversion_rate

            # spark sql query to calculate the sum of taxableAmount in previous time frame
            previous_sum = 'SELECT SUM(accounting.taxableAmount) FROM storeOrder WHERE createdTimeStamp BETWEEN {} AND {} AND status.status == 7'.format(
                int(last_month_first), int(last_month_date))
            previous_sum = previous_sum + store_query + store_categories_query
            previous_sum = sqlContext.sql(previous_sum)
            previous_sum = previous_sum.toPandas().to_dict(orient='records')
            previous_sum = list(previous_sum[0].values())[0]
            if previous_sum is None: previous_sum = 0
            previous_sum = previous_sum * conversion_rate

            if (previous_sum == 0) and (current_sum == 0):
                response = {"message": "success", "percentage": 0,
                            "current_period": current_sum, "previous_period": previous_sum,
                            "data": "decrease from {} to {}".format(last_month_first_d.strftime('%d %b, %Y'),
                                                                    last_month_date_d.strftime('%d %b, %Y'))}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            elif previous_sum == 0:
                response = {"message": "success",
                            "percentage": 100,
                            "data": "decrease from {} to {}".format(last_month_first_d.strftime('%d %b, %Y'),
                                                                    last_month_date_d.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            elif previous_sum > current_sum:
                decrease = previous_sum - current_sum
                percent_decrease = (decrease / previous_sum) * 100
                response = {"message": "success",
                            "percentage": -round(percent_decrease, 2),
                            "data": "decrease from {} to {}".format(last_month_first_d.strftime('%d %b, %Y'),
                                                                    last_month_date_d.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                increase = current_sum - previous_sum
                percent_increase = (increase / previous_sum) * 100
                response = {"message": "success",
                            "percentage": round(percent_increase, 2),
                            "data": "increase from {} to {}".format(last_month_first_d.strftime('%d %b, %Y'),
                                                                    last_month_date_d.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": "Internal server error"}
            return JsonResponse(error, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DescriptiveSalesReport(APIView):
    def get(self, request):
        """
        descriptive sales report comparison with respect to previous month sales in monthly time frame
        :param request:
        :return:
        """
        try:
            # authorization key
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            # time zone parameter
            try:
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # store id  request and query
            try:
                store_id = str(request.GET.get("store_categories_id", ""))
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            # store categories parameter and spark sql query
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            # currency support
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

            # start_timestamp and end_timestamp parameter
            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "mandatory field 'start_timestamp/end_timestamp'  missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    start_timestamp = int(request.GET["start_timestamp"])
                    end_timestamp = int(request.GET["end_timestamp"])
                    if end_timestamp < start_timestamp:
                        response = {"message": "end timestamp must be greater than start timestamp"}
                        return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                except:
                    response = {"message": "Incorrect timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # comparison parameter if pass 0 comparison will be done by previous month and if 1 previous year
            compare_support = {0: "previous month", 1: "previous year"}
            try:
                compare_with = int(request.GET.get("compare_with", 0))
                if compare_with not in [0, 1]:
                    response = {"message": "unsupport'compare_with'", "support": compare_support}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "Incorrect 'compare_with'", "support": compare_support}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            start_date = datetime.datetime.fromtimestamp(start_timestamp, tz=pytz.utc)
            end_date = datetime.datetime.fromtimestamp(end_timestamp, tz=pytz.utc)
            date_difference = end_date - start_date
            subtract = relativedelta(years=1) if compare_with else relativedelta(months=1)
            # previous_start_date = start_date - subtract
            previous_end_date = end_date - subtract
            previous_start_date = previous_end_date - date_difference

            previous_start_timestamp = datetime.datetime.timestamp(previous_start_date)
            previous_end_timestamp = datetime.datetime.timestamp(previous_end_date)

            # today's starting date-time and end date-time
            today_date = datetime.datetime.now()

            today_start_date = datetime.datetime(day=today_date.day, month=today_date.month, year=today_date.year)
            today_end_date = today_start_date + datetime.timedelta(days=1)
            today_start_date = today_start_date.replace(tzinfo=time_zone).astimezone(UTC)
            today_end_date = today_end_date.replace(tzinfo=time_zone).astimezone(UTC)
            today_start_timestamp = datetime.datetime.timestamp(today_start_date)
            today_end_timestamp = datetime.datetime.timestamp(today_end_date)

            # spark sql query to calculate the sum of taxableAmount in today's time frame
            today_sum = 'SELECT SUM({}.taxableAmount) FROM storeOrder WHERE createdTimeStamp BETWEEN {} AND {} AND status.status == 7'.format(
                accounting, int(today_start_timestamp), int(today_end_timestamp))
            today_sum = today_sum + store_query + store_categories_query
            try:
                assert False
                today_sum = sqlContext.sql(today_sum)
                today_sum = today_sum.toPandas().to_dict(orient='records')
                today_sum = list(today_sum[0].values())[0]
                if today_sum is None: today_sum = 0
            except:
                match = {"status.status": 7,
                         "createdTimeStamp": {"$gte": int(start_timestamp), "$lte": int(end_timestamp)}
                         }
                if store_id and store_id not in [0, "0"]: match["storeId"] = store_id
                if store_categories_id and store_categories_id not in [0, "0"]: match[
                    "storeCategoryId"] = store_categories_id

                print("match --->", match)
                data = list(db.storeOrder.aggregate([
                    {"$match": match},
                    {
                        "$group": {
                            "_id": None,
                            "taxableAmount": {"$sum": "$accounting.taxableAmount"}
                        }}
                ]))
                print("today_sum data ---------------->", data)
                today_sum = data[0].get("taxableAmount", 0) if data else 0

            today_sum = today_sum * conversion_rate

            # spark sql query to calculate the sum of taxableAmount in current time frame
            try:
                assert False
                current_sum = 'SELECT SUM({}.taxableAmount) FROM storeOrder WHERE createdTimeStamp BETWEEN {} AND {} AND status.status == 7'.format(
                    accounting, int(start_timestamp), int(end_timestamp))
                current_sum = current_sum + store_query + store_categories_query
                current_sum = sqlContext.sql(current_sum)
                current_sum = current_sum.toPandas().to_dict(orient='records')
                current_sum = list(current_sum[0].values())[0]
            except:
                match = {"status.status": 7,
                         "createdTimeStamp": {"$gte": int(start_timestamp), "$lte": int(end_timestamp)}
                         }
                if store_id and store_id not in [0, "0"]: match["storeId"] = store_id
                if store_categories_id and store_categories_id not in [0, "0"]:
                    match["storeCategoryId"] = store_categories_id
                data = list(db.storeOrder.aggregate([
                    {"$match": match},
                    {
                        "$group": {
                            "_id": None,
                            "taxableAmount": {"$sum": "$accounting.taxableAmount"}
                        }}
                ]))
                print("current_sum data ---------------->", data)
                current_sum = data[0].get("taxableAmount", 0) if data else 0

            if current_sum is None: current_sum = 0
            current_sum = current_sum * conversion_rate

            # spark sql query to calculate the sum of taxableAmount in previous time frame
            try:
                assert False
                previous_sum = 'SELECT SUM({}.taxableAmount) FROM storeOrder WHERE createdTimeStamp BETWEEN {} AND {} AND status.status == 7'.format(
                    accounting, int(previous_start_timestamp), int(previous_end_timestamp))
                previous_sum = previous_sum + store_query + store_categories_query
                previous_sum = sqlContext.sql(previous_sum)
                previous_sum = previous_sum.toPandas().to_dict(orient='records')
                previous_sum = list(previous_sum[0].values())[0]
            except:
                match = {"status.status": 7,
                         "createdTimeStamp": {"$gte": int(start_timestamp),
                                              "$lte": int(end_timestamp)}
                         }
                if store_id and store_id not in [0, "0"]: match["storeId"] = store_id
                if store_categories_id and store_categories_id not in [0, "0"]: match[
                    "storeCategoryId"] = store_categories_id
                data = list(db.storeOrder.aggregate([
                    {"$match": match},
                    {
                        "$group": {
                            "_id": None,
                            "taxableAmount": {"$sum": "$accounting.taxableAmount"}
                        }}
                ]))
                previous_sum = data[0].get("taxableAmount", 0) if data else 0

            if previous_sum is None: previous_sum = 0
            previous_sum = previous_sum * conversion_rate

            # conditional response
            if (previous_sum == 0) and (current_sum == 0):
                response = {"message": "success", "percentage": 0,
                            "current_period": current_sum, "previous_period": previous_sum,
                            "data": "decrease from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y'))}
            elif previous_sum == 0:
                response = {"message": "success",
                            "percentage": 100,
                            "data": "decrease from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
            elif previous_sum > current_sum:
                decrease = previous_sum - current_sum
                percent_decrease = (decrease / previous_sum) * 100
                response = {"message": "success",
                            "percentage": -round(percent_decrease, 2),
                            "data": "decrease from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
            else:
                increase = current_sum - previous_sum
                percent_increase = (increase / previous_sum) * 100
                response = {"message": "success",
                            "percentage": round(percent_increase, 2),
                            "data": "increase from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
            response["today_total"] = today_sum
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()
                                            [-1].tb_lineno), type(ex).__name__, ex)
            error = {"message": "Internal server error"}
            return JsonResponse(error, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class Columns(APIView):

    def get(self, request):
        """
        API to get all the columns dynamically
        :param request: None
        :return: list of field names
        """
        ############################################################################################################
        # HARD CODED COLUMN VALUES
        if request.method != "GET":
            response = {"message": "Method not allowed"}
            return JsonResponse(response, safe=False, status=status.HTTP_405_METHOD_NOT_ALLOWED)

        columns = [
            {"Store": [
                {"index": 1, "value": "Store Name"},
                {"index": 2, "value": "Store Phone"},
                {"index": 3, "value": "Store Email"},
                {"index": 4, "value": "Store Type"}
            ]},
            {"Payment": [
                {"index": 5, "value": "Payment Method"},
                {"index": 6, "value": "Pay By Wallet"}
            ]},
            {"Order": [
                {"index": 7, "value": "Order Id"},
                {"index": 8, "value": "Order Type"},
                {"index": 9, "value": "Order Status"},
                {"index": 10, "value": "Order Completion"},
                {"index": 11, "value": "Order Dispatch Type"}
            ]},
            {"Customer Details": [
                {"index": 12, "value": "Customer Name"},
                {"index": 13, "value": "Customer Email"},
                {"index": 14, "value": "Customer Type"}
            ]},
            {"Accounting": [
                {"index": 15, "value": "Gross Order Value"},
                {"index": 16, "value": "Offer Discount Value"},
                {"index": 17, "value": "Promo Discount Value"},
                {"index": 18, "value": "Gross Value(After offer Discount)"},
                {"index": 19, "value": "Add On Amount"},
                # {"index": 20, "value": "Taxable Amount"},
                {"index": 21, "value": "Delivery Fee"},
                {"index": 22, "value": "Tax Value"},
                {"index": 23, "value": "Value(After tax and Discount)"},
                {"index": 25, "value": "Final Order Value"},
                {"index": 26, "value": "App Earning"},
                {"index": 27, "value": "App Earning With Tax"},
                {"index": 28, "value": "Store Earning"}
            ]},
            {"Pickup Address": [
                {"index": 29, "value": "Pickup Locality"},
                {"index": 30, "value": "Pickup City"},
                {"index": 31, "value": "Pickup Postal Code"},
                {"index": 32, "value": "Pickup Country"}
            ]},
            {"Shipping Address": [
                {"index": 33, "value": "shipping Locality"},
                {"index": 34, "value": "Shipping City"},
                {"index": 35, "value": "Shipping Postal Code"},
                {"index": 36, "value": "Shipping State"}
            ]},
            {"Billing Address": [
                {"index": 37, "value": "Billing Locality"},
                {"index": 38, "value": "Billing City"},
                {"index": 39, "value": "Billing State"}
                # {"index": 40, "value": "Billing Country"}
            ]}]
        return JsonResponse({"message": "Success", "columns": columns}, safe=False, status=status.HTTP_200_OK)
