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
from analytics.function import Process, BASE_CURRENCY, CURRENCY_API
from rest_framework.views import APIView
from analytics.settings import  db
from datetime import datetime, timedelta, date
import traceback


class TransactionData(APIView):
    def get(self, request):
        """
        GET API to show data of Sales performance in respective time period
        :param request: store_categories(string): Mongo ID of respective store categories
                        start_timestamp(integer): epoch time in seconds
                        end_timestamp(integer)  : epoch time in seconds
                        search(string)          : product name if not received blank string
                        skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
                        sort_by(string)         : column name by which table to be sorted
                        ascending(integer)      : does the respective sorting should be ascending or not
                        currency(string)        : currency name ex. INR, USD, etc.
                        group_by(integer)       : group_by id, country or states {1: "transaction_id", 2: "country", 3: "states"}
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

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            # Store Categories
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = store_categories_id if store_categories_id != "0" else ""

            # group by
            group_column = {1: "Customer Name", 2: "Country", 3: "State"}
            try:
                group_by = int(request.GET.get("group_by", 1))
                if group_by not in [1, 2, 3]:
                    response = {"message": "unsupported group by"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "invalid group_by"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            export = int(request.GET.get('export', 0))

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

            # Search parameters and skip limit parameters
            search = str(request.GET["search"]).strip() if "search" in request.GET else ""
            try:
                skip = int(request.GET['skip']) if "skip" in request.GET else 0
                limit = int(request.GET['limit']) if "limit" in request.GET else 10
            except:
                response = {"message": "skip and limit must be integer"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)


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
                        response = {'message': 'Internal server issue with exchange rate API',
                                    "error": type(ex).__name__}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Sort by (Default = "Revenue")
            # ascending (Default = False)
            sort_by = str(request.GET["sort_by"]) if 'sort_by' in request.GET else "Revenue"
            supported_sort_by = [group_column[group_by], "Revenue", "Unique Purchases", "Quantity", "Avg. Price",
                                 "Avg. QTY", "Refund Amount", "Tax", "Shipping"]
            if sort_by not in supported_sort_by:
                return JsonResponse({"message": "unsupported sort_by", "support": supported_sort_by}, safe=False,
                                    status=422)
            try:
                ascending = int(request.GET["ascending"]) if "ascending" in request.GET else 0
                if ascending not in [0, 1]:
                    return JsonResponse({"message": "unsupported ascending", "support": {1: True, 0: False}},
                                        safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                return JsonResponse({"message": "ascending must be integer", "support": {1: True, 0: False}},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            ascending = bool(ascending)


            # spark sql query
            query = "SELECT * from storeOrder WHERE createdTimeStamp BETWEEN {} AND {} ".format(
                start_timestamp, end_timestamp)
            if store_categories_id: query = query + "AND storeCategoryId == '{}'".format(store_categories_id)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
            query = query + store_query

            query = query.strip()
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id:query["storeCategoryId"] = store_categories_id
                if store_id:query["storeId"] = store_id
                result_data = pd.DataFrame(db.storeOrder.find(query))
            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["masterOrderId", "createdDate", "customerDetails", "billingAddress", "products",
                                      "paymentType"]]

            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if int(product['status']['status']) in [7, 3]:
                        user_name = " ".join([product_df.customerDetails.loc[index]["firstName"],
                                              product_df.customerDetails.loc[index]["lastName"]]).strip().capitalize()
                        if not search:
                            _dict = {
                                "customerId": product_df.customerDetails.loc[index]["id"],
                                "userName": user_name.capitalize(),
                                'masterOrderId': product_df.masterOrderId.loc[index],
                                'country': product_df.billingAddress.loc[index]["country"] if "country" in product_df.billingAddress.loc[index].keys()\
                                     and product_df.billingAddress.loc[index]["country"] else "",
                                'state': product_df.billingAddress.loc[index].get("state", ""),
                                'name': product['name'].capitalize(),
                                'productId': product['productId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                'appEarning':  conversion_rate * float(product[accounting]['appEarning']) if accounting == "accounting" else 0,
                                'taxAmount': float(product[accounting]['taxAmount']) * conversion_rate,
                                'deliveryFee': float(product[accounting]['deliveryFee']) * conversion_rate,
                                "paymentType": product_df.paymentType.loc[index],
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])
                            }
                            invoice_product_list.append(_dict)
                        else:
                            if group_by == 1:
                                _search = re.search(str(search), str(user_name), flags=re.IGNORECASE)
                            else:
                                searching_columns = {2: "country", 3: "state"}
                                _search = re.search(
                                    str(search),
                                    str(product_df.billingAddress.loc[index][searching_columns[group_by]]),
                                    flags=re.IGNORECASE)
                            if _search:
                                _dict = {
                                    "customerId": product_df.customerDetails.loc[index]["id"],
                                    "userName": user_name.capitalize(),
                                    'masterOrderId': product_df.masterOrderId.loc[index],
                                    'country': product_df.billingAddress.loc[index].get("country", ""),
                                    'state': product_df.billingAddress.loc[index].get("state", ""),
                                    'name': product['name'].capitalize(),
                                    'productId': product['productId'],
                                    'quantity': product['quantity']['value'],
                                    'status': int(product['status']['status']),
                                    'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                    'appEarning': conversion_rate * float(product[accounting]['appEarning']) if accounting == "accounting" else 0,
                                    'taxAmount': float(product[accounting]['taxAmount']) * conversion_rate,
                                    'deliveryFee': float(product[accounting]['deliveryFee']) * conversion_rate,
                                    "paymentType": product_df.paymentType.loc[index],
                                    "cancelled": int(product['timestamps']['cancelled']),
                                    "completed": int(product['timestamps']['completed'])
                                }
                                invoice_product_list.append(_dict)
            invoice_df = pd.DataFrame(invoice_product_list)

            if not invoice_df.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            user_df = invoice_df[["customerId", "userName"]].drop_duplicates(keep="last")

            # ============================= If product payment not received =============================
            invoice_df["quantity"][((invoice_df.status == 3) &
                                    ((invoice_df.paymentType == 2) &
                                     ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0
            invoice_df["taxableAmount"][((invoice_df.status == 3) &
                                         ((invoice_df.paymentType == 2) &
                                          ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0

            invoice_df = invoice_df.drop(["paymentType", "cancelled", "completed"], axis=1, errors="ignore")
            # ===========================================================================================

            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["deliveryFee"] = invoice_df[['status', 'deliveryFee']].apply(lambda x: x[1] if x[0] == 7 else 0,
                                                                                    axis=1)
            invoice_df["taxAmount"] = invoice_df[['status', 'taxAmount']].apply(lambda x: x[1] if x[0] == 7 else 0,
                                                                                axis=1)
            invoice_df["appEarning"] = invoice_df[['status', 'appEarning']].apply(lambda x: x[1] if x[0] == 7 else 0,
                                                                                  axis=1)
            invoice_df["taxableAmount"] = invoice_df["taxableAmount"] - invoice_df["appEarning"]
            invoice_df["unique_purchase"] = 1
            invoice_df["order_quantity"] = invoice_df[["status", "quantity"]].apply(lambda x: x[1] if x[0] == 7 else 0,axis=1)
            invoice_df["return_quantity"] = invoice_df[["status", "quantity"]].apply(lambda x: x[1] if x[0] == 3 else 0,axis=1)
            # product = invoice_df[["productId", "name"]].drop_duplicates()
            group_column = {1: "customerId", 2: "country", 3: "state"}
            grouped_df = invoice_df.groupby(group_column[group_by]).sum().reset_index()
            if group_by == 1:
                col = list(grouped_df.columns)
                col.remove("customerId")
                grouped_df = pd.merge(grouped_df, user_df, on="customerId", how="left")
                grouped_df = grouped_df[["userName"] + col]
            # calculating the average price and quantity
            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)

            columns = [group_column[group_by] if group_by != 1 else "userName",
                       "taxableAmount", "quantity", "refund_amount", "taxAmount", "deliveryFee"]
            grouped_df = grouped_df[columns]
            # Columns Rename
            rename_col = {"taxableAmount": "Revenue", "unique_purchase": "Unique Purchases",
                          "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                          "refund_amount": "Refund Amount", "return_quantity": "Return Quantity",
                          "masterOrderId": "Transaction ID", "country": "Country", "state": "State",
                          "taxAmount": "Tax", "deliveryFee": "Shipping", "userName": "Customer Name"}
            grouped_df = grouped_df.rename(columns=rename_col)
            grouped_df = grouped_df.sort_values(by=sort_by, ascending=ascending)
            summary = dict(grouped_df.sum(axis=0, numeric_only=True))

            total_count = grouped_df.shape[0]
            # Export functionality, to download the data in csv format
            if not export:
                for col in ["Revenue", "Quantity", "Refund Amount", "Tax", "Shipping"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(lambda x: {
                        "value": x, 'percent': round((x / summation) * 100, 2) if x else 0})

                grouped_df = grouped_df[skip * limit: skip * limit + limit]
                grouped_df = grouped_df.to_dict(orient="records")
                response = {"message": "success", "data": grouped_df, "summary": summary, "count": total_count}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                for col in ["Revenue", "Quantity", "Refund Amount", "Tax", "Shipping"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(
                        lambda x: "{}   ({}%)".format(round(x, 2), round((x / summation) * 100, 2) if x else 0))
                grouped_df = grouped_df.to_dict(orient="records")
                response = {"message": "success", "data": grouped_df}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TransactionPercentage(APIView):
    def get(self, request):
        """
        GET API to show data of sales performance transaction percentage data with pie chart in respective time period
        :param request: store_categories(string): Mongo ID of respective store categories
                        start_timestamp(integer): epoch time in seconds
                        end_timestamp(integer)  : epoch time in seconds
                        search(string)          : product name if not received blank string
                        skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received 10
                        sort_by(string)         : column name by which table to be sorted
                        ascending(integer)      : does the respective sorting should be ascending or not
                        currency(string)        : currency name ex. INR, USD, etc.
                        column(string)          : comparative column
                        pie_column(string)      : percent with respect to column with pie chart data
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

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            supported_columns = ["Revenue", "Unique Purchases", "Quantity", "Avg. Price", "Avg. QTY",
                                 "Refund Amount", "Tax", "Shipping"]
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = store_categories_id if store_categories_id != "0" else ""

            # group by
            group_column = {1: "Customer Name", 2: "Country", 3: "State"}
            try:
                group_by = int(request.GET.get("group_by", 1))
                if group_by not in [1, 2, 3]:
                    response = {"message": "unsupported group by"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "invalid group_by"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            export = int(request.GET.get('export', 0))

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


            search = str(request.GET["search"]).strip() if "search" in request.GET else ""
            try:
                skip = int(request.GET['skip']) if "skip" in request.GET else 0
                limit = int(request.GET['limit']) if "limit" in request.GET else 10
            except:
                response = {"message": "skip and limit must be integer"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)


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
                        response = {'message': 'Internal server issue with exchange rate API', "error": type(ex).__name__}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


            column = str(request.GET["column"]).strip() if 'column' in request.GET else "Revenue"
            sort_by = str(request.GET["sort_by"]).strip() if 'sort_by' in request.GET else column
            if sort_by not in [group_column[group_by], column]:
                response = {"message": "unsupported sort_by", "support": [group_column[group_by], column]}
                return JsonResponse(response, safe=False, status=422)
            if sort_by == column: sort_by = "column"
            pie_column = str(request.GET["pie_column"]).strip() if 'pie_column' in request.GET else "Revenue"
            error_param = []
            if column not in supported_columns: error_param.append("column")
            if pie_column not in supported_columns: error_param.append("pie_column")
            if error_param:
                response = {"message": "unsupported {}".format(" ".join(error_param)), "support": supported_columns}
                return JsonResponse(response, safe=False, status=422)
            try:
                ascending = int(request.GET["ascending"]) if "ascending" in request.GET else 0
                if ascending not in [0, 1]:
                    return JsonResponse({"message": "unsupported ascending", "support": {1: True, 0: False}},
                                        safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                return JsonResponse({"message": "ascending must be integer", "support": {1: True, 0: False}},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            ascending = bool(ascending)

            # spark sql query
            query = "SELECT * from storeOrder WHERE createdTimeStamp BETWEEN {} AND {} ".format(
                start_timestamp, end_timestamp)
            if store_categories_id: query = query + "AND storeCategoryId == '{}'".format(store_categories_id)
            query = query + store_query
            query = query.strip()
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id:query["storeCategoryId"] = store_categories_id
                if store_id:query["storeId"] = store_id
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["masterOrderId", "createdDate", "customerDetails", "billingAddress", "products",
                                      "paymentType"]]
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if int(product['status']['status']) in [7, 3]:
                        user_name = " ".join([product_df.customerDetails.loc[index]["firstName"],
                                              product_df.customerDetails.loc[index]["lastName"]]).strip().capitalize()
                        if not search:
                            _dict = {
                                "customerId": product_df.customerDetails.loc[index]["id"],
                                "userName": user_name.capitalize(),
                                'masterOrderId': product_df.masterOrderId.loc[index],
                                'country': product_df.billingAddress.loc[index].get("country", ""),
                                'state': product_df.billingAddress.loc[index].get("state", ""),
                                'name': product['name'].capitalize(),
                                'productId': product['productId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                'appEarning':  conversion_rate * float(product[accounting]['appEarning']) if accounting == "accounting" else 0,
                                'taxAmount': float(product[accounting]['taxAmount']) * conversion_rate,
                                'deliveryFee': float(product[accounting]['deliveryFee']) * conversion_rate,
                                "paymentType": product_df.paymentType.loc[index],
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])
                            }
                            invoice_product_list.append(_dict)
                        else:
                            if group_by == 1:
                                _search = re.search(str(search), str(user_name), flags=re.IGNORECASE)
                            else:
                                searching_columns = {2: "country", 3: "state"}
                                _search = re.search(
                                    str(search),
                                    str(product_df.billingAddress.loc[index][searching_columns[group_by]]),
                                    flags=re.IGNORECASE)
                            if _search:
                                _dict = {
                                    "customerId": product_df.customerDetails.loc[index]["id"],
                                    "userName": user_name.capitalize(),
                                    'masterOrderId': product_df.masterOrderId.loc[index],
                                    'country': product_df.billingAddress.loc[index].get("country", ""),
                                    'state': product_df.billingAddress.loc[index].get("state", ""),
                                    'name': product['name'].capitalize(),
                                    'productId': product['productId'],
                                    'quantity': product['quantity']['value'],
                                    'status': int(product['status']['status']),
                                    'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                    'appEarning': conversion_rate * float(product[accounting]['appEarning']) if accounting == "accounting" else 0,
                                    'taxAmount': float(product[accounting]['taxAmount']) * conversion_rate,
                                    'deliveryFee': float(product[accounting]['deliveryFee']) * conversion_rate,
                                    "paymentType": product_df.paymentType.loc[index],
                                    "cancelled": int(product['timestamps']['cancelled']),
                                    "completed": int(product['timestamps']['completed'])
                                }
                                invoice_product_list.append(_dict)
            invoice_df = pd.DataFrame(invoice_product_list)
            # invoice_df = invoice_df[invoice_df["status"].isin([7, 3])]
            if not invoice_df.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            user_df = invoice_df[["customerId", "userName"]].drop_duplicates(keep="last")

            # ============================= If product payment not received =============================
            invoice_df["quantity"][((invoice_df.status == 3) &
                                    ((invoice_df.paymentType == 2) &
                                     ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0
            invoice_df["taxableAmount"][((invoice_df.status == 3) &
                                         ((invoice_df.paymentType == 2) &
                                          ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))] = 0

            invoice_df = invoice_df.drop(["paymentType", "cancelled", "completed"], axis=1, errors="ignore")
            # ===========================================================================================

            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["deliveryFee"] = invoice_df[['status', 'deliveryFee']].apply(lambda x: x[1] if x[0] == 7 else 0,
                                                                                    axis=1)
            invoice_df["taxAmount"] = invoice_df[['status', 'taxAmount']].apply(lambda x: x[1] if x[0] == 7 else 0,
                                                                                axis=1)
            invoice_df["appEarning"] = invoice_df[['status', 'appEarning']].apply(lambda x: x[1] if x[0] == 7 else 0,
                                                                                  axis=1)
            invoice_df["taxAmount"] = invoice_df["taxAmount"] - invoice_df["appEarning"]
            invoice_df["unique_purchase"] = 1
            invoice_df["order_quantity"] = invoice_df[["status", "quantity"]].apply(lambda x: x[1] if x[0] == 7 else 0)
            invoice_df["return_quantity"] = invoice_df[["status", "quantity"]].apply(lambda x: x[1] if x[0] == 3 else 0)
            # product = invoice_df[["productId", "name"]].drop_duplicates()
            group_column = {1: "customerId", 2: "country", 3: "state"}
            grouped_df = invoice_df.groupby(group_column[group_by]).sum().reset_index()
            if group_by == 1:
                col = list(grouped_df.columns)
                col.remove("customerId")
                grouped_df = pd.merge(grouped_df, user_df, on="customerId", how="left")
                grouped_df = grouped_df[["userName"] + col]

            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)

            columns = [group_column[group_by] if group_by != 1 else "userName",
                       "taxableAmount", "quantity", "refund_amount", "taxAmount", "deliveryFee"]
            grouped_df = grouped_df[columns]

            rename_col = {"taxableAmount": "Revenue", "unique_purchase": "Unique Purchases",
                          "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                          "refund_amount": "Refund Amount", "return_quantity": "Return Quantity",
                          "masterOrderId": "Transaction ID", "country": "Country", "state": "State",
                          "taxAmount": "Tax", "deliveryFee": "Shipping", "userName": "Customer Name"}
            grouped_df = grouped_df.rename(columns=rename_col)
            group_column = {1: "Customer Name", 2: "Country", 3: "State"}
            df = grouped_df[[group_column[group_by], column]]
            df = df.rename(columns={column: "column"})
            df[pie_column] = grouped_df[pie_column]
            df = df.sort_values(by=sort_by, ascending=ascending)
            summation = df[pie_column].sum()
            df[pie_column] = df[pie_column].apply(lambda x: round((x / summation) * 100, 2) if x else 0)
            pie_series = list(df[pie_column][:10])
            pie_label = list(df[group_column[group_by]][:10])
            if not search:
                pie_label.append("other")
                pie_series.append(100 - sum(pie_series))
            df = df.rename(columns={pie_column: "pie_column"})
            summary = dict(df.sum(axis=0, numeric_only=True))
            summary["pie_column"] = float(summation)
            total_count = df.shape[0]
            if not export:
                df = df[skip * limit: skip * limit + limit].to_dict(orient="records")
                response = {"message": "success", "data": df, "summary": summary, "count": total_count,
                            "graph": {"series": pie_series, "label": pie_label}}
            else:
                df = df.to_dict(orient="records")
                response = {"message": "success", "data": df}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
