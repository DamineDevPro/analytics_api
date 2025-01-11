from django.shortcuts import render
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
import ast
# import datetime
from analytics.settings import db, BASE_CURRENCY, CURRENCY_API
import requests
import json
import pytz
import re
from analytics.function import Process
from bson import ObjectId


class TopProductData(APIView):
    def get(self, request):
        """
        GET API to show data of top products in respective time period
        :param request: store_categories(string): Mongo ID of respective store categories
                        start_timestamp(integer): epoch time in seconds
                        end_timestamp(integer)  : epoch time in seconds
                        search(string)          : product name if not received blank string
                        skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
                        sort_by(string)         : column name by which table to be sorted
                        ascending(integer)      : does the respective sorting should be ascending or not
                        currency(string)        : currency name ex. INR, USD, etc.
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

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

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
                        response = {'message': 'Internal server issue with exchange rate API',
                                    "error": type(ex).__name__}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Ascending or descending sorting
            sort_by = str(request.GET["sort_by"]) if 'sort_by' in request.GET else "Product Revenue"
            supported_sort_by = ["Product", "Product Revenue", "Unique Purchases", "Quantity", "Avg. Price", "Avg. QTY",
                                 "Product Refund Amount"]
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
            query = query + store_query
            query = query.strip()
            print("query ---------->", query)
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "paymentType", "products"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if not search:
                        _dict = {
                            'cartId': product_df.cartId.loc[index],
                            "paymentType": product_df.paymentType.loc[index],
                            'name': product['name'].capitalize(),
                            'centralProductId': product['centralProductId'],
                            'quantity': product['quantity']['value'],
                            'status': int(product['status']['status']),
                            'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                            "cancelled": int(product['timestamps']['cancelled']),
                            "completed": int(product['timestamps']['completed'])

                        }
                        invoice_product_list.append(_dict)
                    else:
                        if re.search(str(search), str(product['name']), flags=re.IGNORECASE):
                            _dict = {
                                'cartId': product_df.cartId.loc[index],
                                "paymentType": product_df.paymentType.loc[index],
                                'name': product['name'].capitalize(),
                                'centralProductId': product['centralProductId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])

                            }
                            invoice_product_list.append(_dict)
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
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
            # ===========================================================================================
            # invoice_df = invoice_df[invoice_df["status"].isin([7, 3])]
            # if not invoice_df.shape[0]:
            #     response = {"message": "No data found"}
            #     return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            product = invoice_df[["centralProductId", "name"]].drop_duplicates(subset=['centralProductId'], keep='last')
            grouped_df = invoice_df.groupby(['centralProductId']).sum().reset_index()
            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df = pd.merge(grouped_df, product, on='centralProductId', how='left')
            columns = ["name", "taxableAmount", "unique_purchase", "quantity", "avg_price", "avg_qty", "refund_amount"]
            grouped_df = grouped_df[columns]
            rename_col = {"name": "Product", "taxableAmount": "Product Revenue", "unique_purchase": "Unique Purchases",
                          "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                          "refund_amount": "Product Refund Amount"}
            grouped_df = grouped_df.rename(columns=rename_col)
            grouped_df = grouped_df.sort_values(by=sort_by, ascending=ascending)
            summary = dict(grouped_df.sum(axis=0, numeric_only=True))
            total_count = grouped_df.shape[0]
            if not export:
                for col in ["Product Revenue", "Unique Purchases", "Quantity", "Product Refund Amount"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(lambda x: {
                        "value": x, 'percent': round((x / summation) * 100, 2) if x else 0})
                grouped_df = grouped_df[skip * limit: skip * limit + limit]
                grouped_df = grouped_df.to_dict(orient="records")
                response = {"message": "success", "data": grouped_df, "summary": summary, "count": total_count}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
            else:
                for col in ["Product Revenue", "Unique Purchases", "Quantity", "Product Refund Amount"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(
                        lambda x: "{}   ({}%)".format(round(x, 2), round((x / summation) * 100, 2) if x else 0))
                grouped_df = grouped_df.to_dict(orient="records")
                response = {"message": "success", "data": grouped_df}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(ex).__name__, ex)
            response = {"message": message, "data": [],
                        "error": {"line": 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno),
                                  "type": str(type(ex).__name__), "Exception": str(ex)}}
            return JsonResponse(response, safe=False, status=500)


class TopBrandData(APIView):
    def get(self, request):
        """
        GET API to show data of top brand in respective time period
        :param request: store_categories(string): Mongo ID of respective store categories (optional)
                        start_timestamp(integer): epoch time in seconds (mandatory)
                        end_timestamp(integer)  : epoch time in seconds (mandatory)
                        search(string)          : brand name if not received blank string (optional)
                        skip(integer)           : page skip if not received 0 (optional, default: 0)
                        limit(integer)          : page limit if not received (optional, default:10)
                        sort_by(string)         : column name by which table to be sorted (optional, default: "Product Revenue")
                        ascending(integer)      : does the respective sorting should be ascending or not (optional, default: 0)
                        currency(string)        : currency name ex. INR, USD, etc. (optional)
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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # if "store_categories_id" not in request.GET:
            #     response = {'message': 'store_categories is missing'}
            #     return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

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
                        response = {'message': 'Internal server issue with exchange rate API',
                                    "error": type(ex).__name__}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            sort_by = str(request.GET["sort_by"]) if 'sort_by' in request.GET else "Product Revenue"
            supported_sort_by = ["Product Brand", "Product Revenue", "Unique Purchases", "Quantity", "Avg. Price",
                                 "Avg. QTY", "Product Refund Amount"]
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
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                if restaurant_store_categories:
                    if query.get("storeCategoryId"):
                        _and = [
                            {"storeCategoryId": {"$nin": restaurant_store_categories}},
                            {"storeCategoryId": store_categories_id}
                        ]
                        del query["storeCategoryId"]
                        query["$and"] = _and
                    else:
                        query["storeCategoryId"] = {"$nin": restaurant_store_categories}
                print(query)
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "paymentType", "products"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if not search:
                        _dict = {
                            'cartId': product_df.cartId.loc[index],
                            "paymentType": product_df.paymentType.loc[index],
                            "brandName": product['brandName'],
                            'name': product['name'].capitalize(),
                            'centralProductId': product['centralProductId'],
                            'quantity': product['quantity']['value'],
                            'status': int(product['status']['status']),
                            'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                            "cancelled": int(product['timestamps']['cancelled']),
                            "completed": int(product['timestamps']['completed'])

                        }
                        invoice_product_list.append(_dict)
                    else:
                        if re.search(str(search), str(product['name']), flags=re.IGNORECASE):
                            _dict = {
                                'cartId': product_df.cartId.loc[index],
                                "paymentType": product_df.paymentType.loc[index],
                                "brandName": product['brandName'],
                                'name': product['name'].capitalize(),
                                'centralProductId': product['centralProductId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])

                            }
                            invoice_product_list.append(_dict)
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
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
            # ===========================================================================================
            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            grouped_df = invoice_df.groupby(['brandName']).sum().reset_index()

            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            columns = ["brandName", "taxableAmount", "unique_purchase", "quantity", "avg_price", "avg_qty",
                       "refund_amount"]
            grouped_df = grouped_df[columns]
            rename_col = {"brandName": "Product Brand", "taxableAmount": "Product Revenue",
                          "unique_purchase": "Unique Purchases", "quantity": "Quantity", "avg_price": "Avg. Price",
                          "avg_qty": "Avg. QTY", "refund_amount": "Product Refund Amount"}
            grouped_df = grouped_df.rename(columns=rename_col)
            grouped_df = grouped_df.sort_values(by=sort_by, ascending=ascending)
            summary = dict(grouped_df.sum(axis=0, numeric_only=True))
            total_count = grouped_df.shape[0]
            if not export:
                for col in ["Product Revenue", "Unique Purchases", "Quantity", "Product Refund Amount"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(lambda x: {
                        "value": x, 'percent': round((x / summation) * 100, 2) if x else 0})
                grouped_df = grouped_df[skip * limit: skip * limit + limit].to_dict(orient="records")
                response = {"message": "success", "data": grouped_df, "summary": summary, "count": total_count}
            else:
                for col in ["Product Revenue", "Unique Purchases", "Quantity", "Product Refund Amount"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(
                        lambda x: "{}   ({}%)".format(round(x, 2), round((x / summation) * 100, 2) if x else 0))
                grouped_df = grouped_df.to_dict(orient="records")
                response = {"message": "success", "data": grouped_df.to_dict(orient="records")}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TopProductPercentage(APIView):
    def get(self, request):
        """
                GET API to show data of top products in respective time period
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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            supported_columns = ["Product Revenue", "Unique Purchases", "Quantity", "Avg. Price", "Avg. QTY",
                                 "Product Refund Amount"]

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

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
                        response = {'message': 'Internal server issue with exchange rate API',
                                    "error": type(ex).__name__}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            column = str(request.GET["column"]).strip() if 'column' in request.GET else "Product Revenue"
            sort_by = str(request.GET["sort_by"]).strip() if 'sort_by' in request.GET else column
            if sort_by not in ["Product", column]:
                response = {"message": "unsupported sort_by", "support": ["Product", column]}
                return JsonResponse(response, safe=False, status=422)
            if sort_by == column: sort_by = "column"
            pie_column = str(request.GET["pie_column"]).strip() if 'pie_column' in request.GET else "Product Revenue"
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
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "paymentType", "products"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if not search:
                        _dict = {
                            'cartId': product_df.cartId.loc[index],
                            "paymentType": product_df.paymentType.loc[index],
                            'name': product['name'].capitalize(),
                            'centralProductId': product['centralProductId'],
                            'quantity': product['quantity']['value'],
                            'status': int(product['status']['status']),
                            'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                            "cancelled": int(product['timestamps']['cancelled']),
                            "completed": int(product['timestamps']['completed'])

                        }
                        invoice_product_list.append(_dict)
                    else:
                        if re.search(str(search), str(product['name']), flags=re.IGNORECASE):
                            _dict = {
                                'cartId': product_df.cartId.loc[index],
                                "paymentType": product_df.paymentType.loc[index],
                                'name': product['name'].capitalize(),
                                'centralProductId': product['centralProductId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])

                            }
                            invoice_product_list.append(_dict)
            if not len(invoice_product_list):
                response = {"message": "No Data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)

            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
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
            # ===========================================================================================
            # invoice_df = invoice_df[invoice_df["status"].isin([7, 3])]
            # if not invoice_df.shape[0]:
            #     response = {"message": "No data found"}
            #     return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            product = invoice_df[["centralProductId", "name"]].drop_duplicates(subset=['centralProductId'], keep='last')
            grouped_df = invoice_df.groupby(['centralProductId']).sum().reset_index()

            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df = pd.merge(grouped_df, product, on='centralProductId', how='left')
            columns = ["name", "taxableAmount", "unique_purchase", "quantity", "avg_price", "avg_qty", "refund_amount"]
            grouped_df = grouped_df[columns]
            rename_col = {"name": "Product", "taxableAmount": "Product Revenue", "unique_purchase": "Unique Purchases",
                          "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                          "refund_amount": "Product Refund Amount"}
            grouped_df = grouped_df.rename(columns=rename_col)
            df = grouped_df[["Product", column]]
            df = df.rename(columns={column: "column"})
            df[pie_column] = grouped_df[pie_column]
            df = df.sort_values(by=sort_by, ascending=ascending)
            summation = df[pie_column].sum()
            df[pie_column] = df[pie_column].apply(lambda x: round((x / summation) * 100, 2) if x else 0)
            pie_series = list(df[pie_column][:10])
            pie_label = list(df["Product"][:10])
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
                df = df.rename(columns={"column": column, "pie_column": pie_column + '(%)'})
                df = df.to_dict(orient="records")
                response = {"message": "success", "data": df}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TopBrandPercentage(APIView):
    def get(self, request):
        """
                GET API to show data of top products in respective time period
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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            supported_columns = ["Product Revenue", "Unique Purchases", "Quantity", "Avg. Price", "Avg. QTY",
                                 "Product Refund Amount"]

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

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

            search = str(request.GET["search"]).strip() if "search" in request.GET else str("")
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

            column = str(request.GET["column"]).strip() if 'column' in request.GET else "Product Revenue"
            sort_by = str(request.GET["sort_by"]).strip() if 'sort_by' in request.GET else column
            if sort_by not in ["Product Brand", column]:
                response = {"message": "unsupported sort_by", "support": ["Product Brand", column]}
                return JsonResponse(response, safe=False, status=422)
            if sort_by == column: sort_by = "column"
            pie_column = str(request.GET["pie_column"]).strip() if 'pie_column' in request.GET else "Product Revenue"
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

            # list all store categories related to food
            restaurant_store_categories = tuple(Process.restaurant_store_categories(db=db))
            if restaurant_store_categories:
                query = query + " AND storeCategoryId NOT IN {}".format(restaurant_store_categories)

            query = query.strip()
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                if restaurant_store_categories:
                    if query.get("storeCategoryId"):
                        _and = [
                            {"storeCategoryId": {"$nin": restaurant_store_categories}},
                            {"storeCategoryId": store_categories_id}
                        ]
                        del query["storeCategoryId"]
                        query["$and"] = _and
                    else:
                        query["storeCategoryId"] = {"$nin": restaurant_store_categories}
                print(query)
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "paymentType", "products"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if not search:
                        _dict = {
                            'cartId': product_df.cartId.loc[index],
                            "brandName": product['brandName'],
                            "paymentType": product_df.paymentType.loc[index],
                            'name': product['name'].capitalize(),
                            'centralProductId': product['centralProductId'],
                            'quantity': product['quantity']['value'],
                            'status': int(product['status']['status']),
                            'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                            "cancelled": int(product['timestamps']['cancelled']),
                            "completed": int(product['timestamps']['completed'])

                        }
                        invoice_product_list.append(_dict)
                    else:
                        if re.search(str(search), str(product['name']), flags=re.IGNORECASE):
                            _dict = {
                                'cartId': product_df.cartId.loc[index],
                                "brandName": product['brandName'],
                                "paymentType": product_df.paymentType.loc[index],
                                'name': product['name'].capitalize(),
                                'centralProductId': product['centralProductId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])

                            }
                            invoice_product_list.append(_dict)
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)

            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
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
            # ===========================================================================================
            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            # product = invoice_df[["centralProductId", "brandName"]].drop_duplicates()
            grouped_df = invoice_df.groupby(['brandName']).sum().reset_index()

            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            # grouped_df = pd.merge(grouped_df, product, on='centralProductId', how='left')
            columns = ["brandName", "taxableAmount", "unique_purchase", "quantity", "avg_price", "avg_qty",
                       "refund_amount"]
            grouped_df = grouped_df[columns]
            rename_col = {"brandName": "Product Brand", "taxableAmount": "Product Revenue",
                          "unique_purchase": "Unique Purchases",
                          "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                          "refund_amount": "Product Refund Amount"}
            grouped_df = grouped_df.rename(columns=rename_col)
            df = grouped_df[["Product Brand", column]]
            df = df.rename(columns={column: "column"})
            df[pie_column] = grouped_df[pie_column]
            df = df.sort_values(by=sort_by, ascending=ascending)
            summation = df[pie_column].sum()
            df[pie_column] = df[pie_column].apply(lambda x: round((x / summation) * 100, 2) if x else 0)
            pie_series = list(df[pie_column][:10])
            pie_label = list(df["Product Brand"][:10])
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
                df = df.rename(columns={"column": column, "pie_column": pie_column + "(%)"})
                df = df.to_dict(orient="records")
                response = {"message": "success", "data": df}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TopCategoriesData(APIView):
    @staticmethod
    def categories(value):
        if isinstance(value, list):
            if len(value) > 0:
                value = value[0]
                if isinstance(value, dict):
                    if isinstance(value.get("parentCategory"), dict):
                        if isinstance(value.get("parentCategory").get("categoryName"), dict):
                            value = value.get("parentCategory").get("categoryName").get("en")
                            return value
                        else:
                            return None
                    else:
                        return None
                else:
                    return None
            else:
                return None
        else:
            return None

    def get(self, request):
        """
        GET API to show data of top products in respective time period
        :param request: store_categories(string): Mongo ID of respective store categories
                        start_timestamp(integer): epoch time in seconds
                        end_timestamp(integer)  : epoch time in seconds
                        search(string)          : product name if not received blank string
                        skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
                        sort_by(string)         : column name by which table to be sorted
                        ascending(integer)      : does the respective sorting should be ascending or not
                        currency(string)        : currency name ex. INR, USD, etc.
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

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

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
                        response = {'message': 'Internal server issue with exchange rate API',
                                    "error": type(ex).__name__}
                        return JsonResponse(response, safe=False, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            sort_by = str(request.GET["sort_by"]) if 'sort_by' in request.GET else "Product Revenue"
            supported_sort_by = ["Product Category", "Product Revenue", "Unique Purchases", "Quantity", "Avg. Price",
                                 "Avg. QTY", "Product Refund Amount"]
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
            query = query + store_query
            query = query.strip()
            try:
                assert False
                result_data = sqlContext.sql(query)
                result_data = result_data.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                result_data = pd.DataFrame(db.storeOrder.find(query))
                
            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "paymentType", "products"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if not search:
                        _dict = {
                            'cartId': product_df.cartId.loc[index],
                            "paymentType": product_df.paymentType.loc[index],
                            'name': product['name'].capitalize(),
                            'centralProductId': product['centralProductId'],
                            'quantity': product['quantity']['value'],
                            'status': int(product['status']['status']),
                            'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                            "cancelled": int(product['timestamps']['cancelled']),
                            "completed": int(product['timestamps']['completed'])

                        }
                        invoice_product_list.append(_dict)
                    else:
                        if re.search(str(search), str(product['name']), flags=re.IGNORECASE):
                            _dict = {
                                'cartId': product_df.cartId.loc[index],
                                "paymentType": product_df.paymentType.loc[index],
                                'name': product['name'].capitalize(),
                                'centralProductId': product['centralProductId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])

                            }
                            invoice_product_list.append(_dict)
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
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
            # ===========================================================================================
            # add categorical data
            product_ids = list(invoice_df["centralProductId"].unique())
            product_ids = list(map(lambda x: ObjectId(str(x)), product_ids))
            product_data = pd.DataFrame(
                db.products.find({"_id": {"$in": product_ids}}, {"firstCategoryName": 1, "_id": 1}))

            # product_data["category"] = product_data["categoryList"].apply(self.categories)
            product_data["category"] = product_data["firstCategoryName"].fillna("").astype(str)
            product_data["_id"] = product_data["_id"].apply(lambda x: str(x))
            product_data = product_data.drop("firstCategoryName", axis=1)
            product_data = product_data.rename(columns={"_id": "centralProductId"})
            invoice_df = pd.merge(invoice_df, product_data, on='centralProductId', how='left')

            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            invoice_df = invoice_df.drop(['centralProductId', 'name'], axis=1)
            grouped_df = invoice_df.groupby(['category']).sum().reset_index()

            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            columns = ["category", "taxableAmount", "unique_purchase", "quantity", "avg_price", "avg_qty",
                       "refund_amount"]
            grouped_df = grouped_df[columns]
            rename_col = {
                "category": "Product Category", "taxableAmount": "Product Revenue",
                "unique_purchase": "Unique Purchases",
                "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                "refund_amount": "Product Refund Amount"}
            grouped_df = grouped_df.rename(columns=rename_col)
            grouped_df = grouped_df.sort_values(by=sort_by, ascending=ascending)
            summary = dict(grouped_df.sum(axis=0, numeric_only=True))
            total_count = grouped_df.shape[0]
            if not export:
                for col in ["Product Revenue", "Unique Purchases", "Quantity", "Product Refund Amount"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(lambda x: {
                        "value": x, 'percent': round((x / summation) * 100, 2) if x else 0})
                grouped_df = grouped_df[skip * limit: skip * limit + limit].to_dict(orient="records")
                response = {"message": "success", "data": grouped_df, "count": total_count, "summary": summary}
            else:
                for col in ["Product Revenue", "Unique Purchases", "Quantity", "Product Refund Amount"]:
                    summation = grouped_df[col].sum()
                    grouped_df[col] = grouped_df[col].apply(
                        lambda x: "{}   ({}%)".format(round(x, 2), round((x / summation) * 100, 2) if x else 0))
                grouped_df = grouped_df.to_dict(orient="records")
                response = {"message": "success", "data": grouped_df}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class TopCategoriesPercentage(APIView):
    @staticmethod
    def categories(value):
        if isinstance(value, list):
            if len(value) > 0:
                value = value[0]
                if isinstance(value, dict):
                    if isinstance(value.get("parentCategory"), dict):
                        if isinstance(value.get("parentCategory").get("categoryName"), dict):
                            value = value.get("parentCategory").get("categoryName").get("en")
                            return value
                        else:
                            return None
                    else:
                        return None
                else:
                    return None
            else:
                return None
        else:
            return None

    def get(self, request):
        """
                GET API to show data of top products in respective time period
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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            supported_columns = ["Product Revenue", "Unique Purchases", "Quantity", "Avg. Price", "Avg. QTY",
                                 "Product Refund Amount"]

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

            export = int(request.GET.get("export", 0))

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

            search = str(request.GET["search"]).strip() if "search" in request.GET else str("")
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

            column = str(request.GET["column"]).strip() if 'column' in request.GET else "Product Revenue"
            sort_by = str(request.GET["sort_by"]).strip() if 'sort_by' in request.GET else column
            if sort_by not in ["Product Category", column]:
                response = {"message": "unsupported sort_by", "support": ["Product Category", column]}
                return JsonResponse(response, safe=False, status=422)
            if sort_by == column: sort_by = "column"
            pie_column = str(request.GET["pie_column"]).strip() if 'pie_column' in request.GET else "Product Revenue"
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
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            result_data['createdDate'] = pd.to_datetime(result_data['createdDate'])
            product_df = result_data[["cartId", 'createdDate', "paymentType", "products"]]
            del result_data
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    if not search:
                        _dict = {
                            'cartId': product_df.cartId.loc[index],
                            "paymentType": product_df.paymentType.loc[index],
                            'name': product['name'].capitalize(),
                            'centralProductId': product['centralProductId'],
                            'quantity': product['quantity']['value'],
                            'status': int(product['status']['status']),
                            'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                            "cancelled": int(product['timestamps']['cancelled']),
                            "completed": int(product['timestamps']['completed'])

                        }
                        invoice_product_list.append(_dict)
                    else:
                        if re.search(str(search), str(product['name']), flags=re.IGNORECASE):
                            _dict = {
                                'cartId': product_df.cartId.loc[index],
                                "paymentType": product_df.paymentType.loc[index],
                                'name': product['name'].capitalize(),
                                'centralProductId': product['centralProductId'],
                                'quantity': product['quantity']['value'],
                                'status': int(product['status']['status']),
                                'taxableAmount': float(product[accounting]['taxableAmount']) * conversion_rate,
                                "cancelled": int(product['timestamps']['cancelled']),
                                "completed": int(product['timestamps']['completed'])

                            }
                            invoice_product_list.append(_dict)
            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)

            # add categorical data
            product_ids = list(invoice_df["centralProductId"].unique())
            product_ids = list(map(lambda x: ObjectId(str(x)), product_ids))
            product_data = pd.DataFrame(db.products.find({"_id": {"$in": product_ids}}, {"firstCategoryName": 1,
                                                                                         "_id": 1}))  # query = "SELECT _id, categoryList from products"
            # product_data = sqlContext.sql(query)
            # product_data = product_data.toPandas()
            # product_data["category"] = product_data["categoryList"].apply(self.categories)
            product_data["category"] = product_data["firstCategoryName"].fillna("").astype(str)
            product_data["_id"] = product_data["_id"].apply(lambda x: str(x))
            product_data = product_data.drop("firstCategoryName", axis=1)
            product_data = product_data.rename(columns={"_id": "centralProductId"})
            invoice_df = pd.merge(invoice_df, product_data, on='centralProductId', how='left')
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
            # ===========================================================================================
            invoice_df["refund_amount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["taxableAmount"] = invoice_df[['status', 'taxableAmount']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            invoice_df = invoice_df.drop(['centralProductId', 'name'], axis=1)
            grouped_df = invoice_df.groupby(['category']).sum().reset_index()

            grouped_df["avg_price"] = grouped_df[["taxableAmount", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            columns = ["category", "taxableAmount", "unique_purchase", "quantity", "avg_price", "avg_qty",
                       "refund_amount"]
            grouped_df = grouped_df[columns]
            rename_col = {
                "category": "Product Category", "taxableAmount": "Product Revenue",
                "unique_purchase": "Unique Purchases",
                "quantity": "Quantity", "avg_price": "Avg. Price", "avg_qty": "Avg. QTY",
                "refund_amount": "Product Refund Amount"}

            grouped_df = grouped_df.rename(columns=rename_col)
            df = grouped_df[["Product Category", column]]
            df = df.rename(columns={column: "column"})
            df[pie_column] = grouped_df[pie_column]
            df = df.sort_values(by=sort_by, ascending=ascending)
            summation = df[pie_column].sum()
            df[pie_column] = df[pie_column].apply(lambda x: round((x / summation) * 100, 2) if x else 0)
            pie_series = list(df[pie_column][:10])
            pie_label = list(df["Product Category"][:10])
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
                df = df.rename(columns={"column": column, "pie_column": pie_column + "(%)"})
                df = df.to_dict(orient="records")
                response = {"message": "success", "data": df}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class ProductGraph(APIView):

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

            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

            # --------------------- Accounting integration ---------------------
            accounting = "sellerAccounting" if store_id else "accounting"
            print("accounting ------------------>", accounting)

            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'missing/incorrect time_zone', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""

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
                return JsonResponse({"message": "'group_by' must be integer"}, safe=False, status=400)

            #         start_timestamp and end_timestamp request
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "missing/Incorrect start_timestamp or end_timestamp, must be integer"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

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
                        response = {'message': 'Error while fetching currency rate', 'error': currency_response.content}
                        return JsonResponse(response, safe=False, status=currency_response.status_code)
                    currency_data = json.loads(currency_response.content.decode('utf-8'))
                    if currency_data.get("data").get('data'):
                        conversion_rate = float(currency_data.get("data").get('data')[0].get('exchange_rate'))
                    else:
                        response = {"message": "currency conversion not found"}
                        return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
                except Exception as e:
                    response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            y_axis = str(request.GET["y_axis"]).strip() if "y_axis" in request.GET else ""
            supported_y_axis = ["product_revenue", "unique_purchase", "quantity", "avg_price", "avg_qty",
                                "refund_amount"]
            if not y_axis or y_axis not in supported_y_axis:
                response = {"message": "unsupported 'y-axis'", "support": supported_y_axis}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # Query
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
                if store_id: query["storeId"] = store_id
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                result_data = pd.DataFrame(db.storeOrder.find(query))

            if (not result_data.shape[0]) and (not result_data.shape[1]):
                response = {'message': 'Error with respect to data frame, No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            product_df = result_data[["cartId", 'createdTimeStamp', "paymentType", "products"]]
            invoice_product_list = []
            for index in range(product_df.shape[0]):
                for product in product_df.products.loc[index]:
                    _dict = {
                        'cartId': product_df.cartId.loc[index],
                        'createdTimeStamp': product_df.createdTimeStamp.loc[index],
                        'centralProductId': product['centralProductId'],
                        'quantity': product['quantity']['value'],
                        'status': int(product['status']['status']),
                        'product_revenue': float(product[accounting]['taxableAmount']) * conversion_rate,
                        "paymentType": product_df.paymentType.loc[index],
                        "cancelled": int(product['timestamps']['cancelled']),
                        "completed": int(product['timestamps']['completed'])
                    }
                    invoice_product_list.append(_dict)

            if not len(invoice_product_list):
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            invoice_df = pd.DataFrame(invoice_product_list)
            invoice_df["status"] = invoice_df["status"].apply(lambda x: 7 if x != 3 else x)
            # ============================= If product payment not received =============================
            invoice_df = invoice_df[~((invoice_df.status == 3) &
                                      ((invoice_df.paymentType == 2) &
                                       ((invoice_df.cancelled != 0) & (invoice_df.completed == 0))))]
            invoice_df = invoice_df.drop(["paymentType", "cancelled", "completed"], axis=1, errors="ignore")
            # ===========================================================================================
            invoice_df["refund_amount"] = invoice_df[['status', 'product_revenue']].apply(
                lambda x: x[1] if x[0] == 3 else 0,
                axis=1)
            invoice_df["product_revenue"] = invoice_df[['status', 'product_revenue']].apply(
                lambda x: x[1] if x[0] == 7 else 0,
                axis=1)
            invoice_df["unique_purchase"] = 1
            invoice_df['dateTime'] = invoice_df['createdTimeStamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            invoice_df["dateTime"] = invoice_df.dateTime.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))

            invoice_df = Process.date_conversion(group_by=group_by, data_frame=invoice_df, date_col='dateTime')

            grouped_df = invoice_df.groupby(['dateTime']).sum().reset_index()

            grouped_df["avg_price"] = grouped_df[["product_revenue", "quantity"]].apply(lambda x: x[0] / x[1], axis=1)
            grouped_df["avg_qty"] = grouped_df[["quantity", "unique_purchase"]].apply(lambda x: x[0] / x[1], axis=1)
            columns = ["dateTime", "product_revenue", "unique_purchase", "quantity", "avg_price", "avg_qty",
                       "refund_amount"]
            grouped_df = grouped_df[columns]

            # Date Filler
            if group_by == 0 or group_by == 6:
                adder = timedelta(hours=1)
            else:
                adder = timedelta(days=1)
            min_time = datetime.fromtimestamp(start_timestamp, tz=time_zone)
            min_time = datetime(day=min_time.day, month=min_time.month, year=min_time.year, hour=min_time.hour)
            max_time = datetime.fromtimestamp(end_timestamp, tz=time_zone)
            max_time = datetime(day=max_time.day, month=max_time.month, year=max_time.year, hour=max_time.hour)
            if group_by:
                min_time = datetime(day=min_time.day, month=min_time.month, year=min_time.year)
                max_time = datetime(day=max_time.day, month=max_time.month, year=max_time.year)

            all_date = []
            date_time = min_time
            while date_time < max_time:
                all_date.append(date_time)
                date_time = date_time + adder
            all_date.append(max_time)
            all_date = set(all_date)
            data_frame_date = list(grouped_df["dateTime"])
            all_date.difference_update(data_frame_date)
            append_df = pd.DataFrame()
            append_df["dateTime"] = list(all_date)
            for col in grouped_df:
                if col != "dateTime":
                    append_df[col] = 0
            append_df["dateTime"] = append_df["dateTime"].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            append_df = Process.date_conversion(group_by=group_by, data_frame=append_df, date_col='dateTime')
            aggregate_data = grouped_df.append(append_df, ignore_index=True)
            aggregate_data = aggregate_data.groupby(['dateTime']).sum().reset_index()

            aggregate_data = aggregate_data.sort_values(by="dateTime", ascending=True)

            if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

            rename_col = {"product_revenue": "Product Revenue ({})".format(currency_symbol),
                          "unique_purchase": "Unique Purchases",
                          "quantity": "Quantity",
                          "avg_price": "Avg. Price ({})".format(currency_symbol),
                          "avg_qty": "Avg. QTY",
                          "refund_amount": "Product Refund Amount ({})".format(currency_symbol)}

            aggregate_data["quantity"] = aggregate_data["quantity"].astype(int)
            graph = {"xcat": list(aggregate_data["dateTime"]),
                     "series": [{"data": list(aggregate_data[y_axis]), "name": rename_col[y_axis]}]}
            response = {"message": "success",
                        "graph": graph}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
