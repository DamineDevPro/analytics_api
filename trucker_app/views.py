import os
import sys
import ast
from datetime import datetime, timedelta
import pandas as pd
import pytz
import requests
import json
import traceback
import numpy as np
from dateutil.relativedelta import relativedelta
from django.http import JsonResponse
from rest_framework import status
from rest_framework.views import APIView
from analytics.function import Process
from .trucker_db_helper import DbHelper
from .trucker_operations_helper import TruckerOperations
from .trucker_response_helper import TruckerResponses
from analytics.settings import UTC, db, BASE_CURRENCY, BOOKING_STATUS, DEVICE_SUPPORT, CURRENCY_API

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()
DbHelper = DbHelper()
TruckerResponses = TruckerResponses()
TruckerOperations = TruckerOperations()
device_support = DEVICE_SUPPORT


class DescriptiveSalesReport(APIView):
    def get(self, request):
        """
        descriptive sales report comparison with respect to previous month sales in monthly time frame
        :param request:
        :return:
        """
        try:
            # ------------- authorization key -------------
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            # ------------- device Type -------------
            try:
                device_type = int(request.GET.get("device_type", "0"))
                assert device_support[device_type]
            except:
                response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
                return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])

            # ------------- time zone parameter -------------
            try:
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # ------------- store id  request and query -------------
            try:
                store_id = str(request.GET.get("store_categories_id", ""))
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_query = "AND storeId == '{}'".format(store_id) if store_id else ""

            # ------------- store categories parameter and spark sql query -------------
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = "AND storeCategoryId == '{}'".format(store_categories_id)

            # -------------------- currency support --------------------
            currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
            conversion_rate = 1
            if currency != BASE_CURRENCY:
                _currency = Process.currency(currency)
                if _currency["error_flag"]:
                    return TruckerResponses.get_status(message=_currency["response_message"],
                                                    status=_currency["response_status"])
                conversion_rate = _currency["conversion_rate"]

            # -------------------- start_timestamp and end_timestamp parameter --------------------
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "Missing/Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # comparison parameter if pass 0 comparison will be done by previous month and if 1 previous year
            compare_support = {0: "previous month", 1: "previous year"}
            try:
                compare_with = int(request.GET.get("compare_with", 0))
                if compare_with not in [0, 1]:
                    response = {"message": "unsupported 'compare_with'", "support": compare_support}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "Incorrect 'compare_with'", "support": compare_support}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            country_id = str(request.GET.get("country_id", ""))
            city_id = str(request.GET.get("city_id", ""))
            zone_id = str(request.GET.get("zone_id", ""))

            start_date = datetime.fromtimestamp(start_timestamp, tz=pytz.utc)
            end_date = datetime.fromtimestamp(end_timestamp, tz=pytz.utc)
            date_difference = end_date - start_date
            subtract = relativedelta(years=1) if compare_with else relativedelta(months=1)
            # previous_start_date = start_date - subtract
            previous_end_date = end_date - subtract
            previous_start_date = previous_end_date - date_difference

            previous_start_timestamp = datetime.timestamp(previous_start_date)
            previous_end_timestamp = datetime.timestamp(previous_end_date)

            # -------------------- today's starting date-time and end date-time --------------------
            today_date = datetime.now()

            today_start_date = datetime(day=today_date.day, month=today_date.month, year=today_date.year)
            today_end_date = today_start_date + timedelta(days=1)
            today_start_date = today_start_date.replace(tzinfo=time_zone).astimezone(UTC)
            today_end_date = today_end_date.replace(tzinfo=time_zone).astimezone(UTC)
            today_start_timestamp = datetime.timestamp(today_start_date)
            today_end_timestamp = datetime.timestamp(today_end_date)

            # ------ spark sql query to calculate the sum of fare in today's time frame ------
            today_sum = DbHelper.fare_count(start_timestamp=today_start_timestamp,
                                            end_timestamp=today_end_timestamp,
                                            store_query=store_query,
                                            store_categories_query=store_categories_query,
                                            conversion_rate=conversion_rate,
                                            device_type=device_type,
                                            country_id=country_id,
                                            city_id=city_id,
                                            zone_id=zone_id
                                            )

            # ------ spark sql query to calculate the sum of fare in current time frame ------
            current_sum = DbHelper.fare_count(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              store_query=store_query,
                                              store_categories_query=store_categories_query,
                                              conversion_rate=conversion_rate,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id)

            # ------ spark sql query to calculate the sum of fare in previous time frame ------
            previous_sum = DbHelper.fare_count(start_timestamp=previous_start_timestamp,
                                               end_timestamp=previous_end_timestamp,
                                               store_query=store_query,
                                               store_categories_query=store_categories_query,
                                               conversion_rate=conversion_rate,
                                               device_type=device_type,
                                               country_id=country_id,
                                               city_id=city_id,
                                               zone_id=zone_id)

            return TruckerOperations.descriptive_stats(previous_sum=previous_sum,
                                                    current_sum=current_sum,
                                                    previous_start_date=previous_start_date,
                                                    previous_end_date=previous_end_date,
                                                    today_sum=today_sum)
        except Exception as ex:
            return TruckerResponses.get_status_500(ex=ex)

class DetailedTotalSales(APIView):
    def get(self, request):
        """
        descriptive sales report comparison with respect to previous month sales in monthly time frame
        :param request:
        :return:
        """
    # try:
        # ------------- authorization key -------------
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            response_data = {"message": "Unauthorized"}
            return JsonResponse(response_data, safe=False, status=401)

        # ------------- device Type -------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])

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

        column_key_dict = {
            'storeName': 1, 'storePhone': 1, 'storeEmail': 1, 'storeTypeMsg': 1, 'paymentTypeText': 1, 'createdDate': 1,
            'payByWallet': 1, '_id': 1, 'orderTypeMsg': 1, 'status.statusText': 1, 'status.partialStatus': 1, "status.status": 1,
            'autoDispatch': 1, 'customerDetails.firstName': 1, 'customerDetails.email': 1,
            'customerDetails.userTypeText': 1, 'accounting.unitPrice': 1, 'accounting.offerDiscount': 1,
            'accounting.promoDiscount': 1, 'accounting.finalUnitPrice': 1, 'accounting.addOnsAmount': 1,
            'accounting.taxAmount': 1, 'accounting.subTotal': 1,
            'accounting.finalTotal': 1, 'accounting.appEarning': 1, 'accounting.appEarningWithTax': 1,
            'accounting.storeEarning': 1, 'pickupAddress.locality': 1, 'pickupAddress.cityName': 1,
            'pickupAddress.postal_code': 1, 'pickupAddress.country': 1, 'deliveryAddress.locality': 1,
            'deliveryAddress.city': 1, 'deliveryAddress.pincode': 1, 'deliveryAddress.state': 1,
            'billingAddress.locality': 1, 'billingAddress.city': 1, 'billingAddress.state': 1}

        # ------------- time zone parameter -------------
        try:
            time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        # ------------- store id  request and query -------------
        try:
            store_id = str(request.GET.get("store_categories_id", ""))
            store_id = "" if store_id == "0" else store_id
        except:
            response = {"message": "mandatory field 'store_id' missing"}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        # ------------- store categories parameter and spark sql query -------------
        store_categories_id = str(request.GET.get("store_categories_id", ""))

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

        # -------------------- currency support --------------------
        currency_symbol = request.GET.get("currency_symbol", "₹")
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return TruckerResponses.get_status(message=_currency["response_message"],
                                                status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]

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
            if ascending not in [0, 1]:
                response = {"message": "ascending must be 0 or 1"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            ascending = bool(ascending)

        # -------------------- start_timestamp and end_timestamp parameter --------------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            response = {"message": "Missing/Incorrect timestamp"}
            return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

        data = DbHelper.total_sales(start_timestamp, end_timestamp, store_categories_id, store_id, column_key_dict)
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
        data["order_status"] = data["status"].apply(lambda x: x["status"])

        # date converter as per group by
        data = Process.date_conversion(group_by=group_by, data_frame=data, date_col='dateTime')
        data = data.groupby(['dateTime']).sum().reset_index()

        table_column = ["dateTime", "order_status", "taxableAmount"]
        # table_data = data[table_column]
        aggregate_data = data.groupby(['dateTime']).sum().reset_index()

        aggregate_data = Process.filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=aggregate_data,
                                        date_column="dateTime", group_by=group_by)

        aggregate_data = aggregate_data.sort_values(by="dateTime", ascending="dateTime")

        if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
        if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data, date_column="dateTime")
        if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

        aggregate_data.fillna('', inplace=True)
        
        graph = aggregate_data[["dateTime", "taxableAmount"]]
        graph['dateTime'] = graph['dateTime'].astype('str')
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

        aggregate_data = aggregate_data.sort_values(by=order_by, ascending=ascending)
        if order_by == "dateTime":
            if group_by == 3: aggregate_data = Process.month_sort(data_frame=aggregate_data, date_column="dateTime")
            if group_by == 4: aggregate_data = Process.quarter_sort(data_frame=aggregate_data,
                                                                    date_column="dateTime")
            if group_by == 7: aggregate_data = Process.day_sort(data_frame=aggregate_data, date_column="dateTime")

        # if "Cancelled" not in aggregate_data.columns:
        #     aggregate_data["Cancelled"] = 0
        # if "order" not in aggregate_data.columns:
        #     aggregate_data["order"] = 0
        aggregate_data.fillna(value=" ", inplace=True)
        new_summary = {}
        for col in aggregate_data.columns:
            if col not in list(summary.keys()):
                new_summary[col] = ' '
            else:
                new_summary[col] = summary[col]

        total_count = int(aggregate_data.shape[0])
        aggregate_data['dateTime'] = aggregate_data['dateTime'].astype('str')
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
            print("data-----> ", data)
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

    # except Exception as ex:
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #     finalResponse = {"message": message, "data": []}
    #     return JsonResponse(finalResponse, safe=False, status=500)

class DescriptiveOrderReport(APIView):
    def get(self, request):
        """
        descriptive sales report comparison with respect to previous month sales in monthly time frame
        :param request:
        :return:
        """
        try:
            # ------------- authorization key -------------
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            # ------------- device Type -------------
            try:
                device_type = int(request.GET.get("device_type", "0"))
                assert device_support[device_type]
            except:
                response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
                return TruckerResponses.get_status_400(message=response, support=device_support, params=["device_type"])

            # ------------- time zone parameter -------------
            try:
                time_zone = pytz.timezone(str(request.GET.get('time_zone', "Asia/Calcutta")))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # ------------- store id  request and query -------------
            try:
                store_id = str(request.GET.get("store_categories_id", ""))
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_query = "AND storeId == '{}'".format(store_id) if store_id else ""

            # ------------- store categories parameter and spark sql query -------------
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = "AND storeCategoryId == '{}'".format(store_categories_id)

            # -------------------- currency support --------------------
            currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
            conversion_rate = 1
            if currency != BASE_CURRENCY:
                _currency = Process.currency(currency)
                if _currency["error_flag"]:
                    return TruckerResponses.get_status(message=_currency["response_message"],
                                                    status=_currency["response_status"])
                conversion_rate = _currency["conversion_rate"]

            # -------------------- start_timestamp and end_timestamp parameter --------------------
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "Missing/Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            # comparison parameter if pass 0 comparison will be done by previous month and if 1 previous year
            compare_support = {0: "previous month", 1: "previous year"}
            try:
                compare_with = int(request.GET.get("compare_with", 0))
                if compare_with not in [0, 1]:
                    response = {"message": "unsupported 'compare_with'", "support": compare_support}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {"message": "Incorrect 'compare_with'", "support": compare_support}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            country_id = str(request.GET.get("country_id", ""))
            city_id = str(request.GET.get("city_id", ""))
            zone_id = str(request.GET.get("zone_id", ""))

            start_date = datetime.fromtimestamp(start_timestamp, tz=pytz.utc)
            end_date = datetime.fromtimestamp(end_timestamp, tz=pytz.utc)
            date_difference = end_date - start_date
            subtract = relativedelta(years=1) if compare_with else relativedelta(months=1)
            # previous_start_date = start_date - subtract
            previous_end_date = end_date - subtract
            previous_start_date = previous_end_date - date_difference

            previous_start_timestamp = datetime.timestamp(previous_start_date)
            previous_end_timestamp = datetime.timestamp(previous_end_date)

            # -------------------- today's starting date-time and end date-time --------------------
            today_date = datetime.now()

            today_start_date = datetime(day=today_date.day, month=today_date.month, year=today_date.year)
            today_end_date = today_start_date + timedelta(days=1)
            today_start_date = today_start_date.replace(tzinfo=time_zone).astimezone(UTC)
            today_end_date = today_end_date.replace(tzinfo=time_zone).astimezone(UTC)
            today_start_timestamp = datetime.timestamp(today_start_date)
            today_end_timestamp = datetime.timestamp(today_end_date)

            # ------ spark sql query to calculate the sum of fare in today's time frame ------
            today_sum = DbHelper.order_fare_count(start_timestamp=today_start_timestamp,
                                            end_timestamp=today_end_timestamp,
                                            store_query=store_query,
                                            store_categories_query=store_categories_query,
                                            conversion_rate=conversion_rate,
                                            device_type=device_type,
                                            country_id=country_id,
                                            city_id=city_id,
                                            zone_id=zone_id
                                            )

            # ------ spark sql query to calculate the sum of fare in current time frame ------
            current_sum = DbHelper.order_fare_count(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              store_query=store_query,
                                              store_categories_query=store_categories_query,
                                              conversion_rate=conversion_rate,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id)

            # ------ spark sql query to calculate the sum of fare in previous time frame ------
            previous_sum = DbHelper.order_fare_count(start_timestamp=previous_start_timestamp,
                                               end_timestamp=previous_end_timestamp,
                                               store_query=store_query,
                                               store_categories_query=store_categories_query,
                                               conversion_rate=conversion_rate,
                                               device_type=device_type,
                                               country_id=country_id,
                                               city_id=city_id,
                                               zone_id=zone_id)

            return TruckerOperations.descriptive_order_stats(previous_sum=previous_sum,
                                                    current_sum=current_sum,
                                                    previous_start_date=previous_start_date,
                                                    previous_end_date=previous_end_date,
                                                    today_sum=today_sum)
        except Exception as ex:
            return TruckerResponses.get_status_500(ex=ex)


