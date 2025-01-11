import traceback

from django.shortcuts import render
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
import ast
from analytics.settings import  db, CURRENCY_API, BASE_CURRENCY

from .promo_db_helper import DbHelper
from .promo_response_helper import Responses
from .promo_operation_helper import Operation
import requests
import json
import pytz
import re
from analytics.function import Process

DbHelper = DbHelper()
Responses = Responses()
Operation = Operation()


class PromoUsage(APIView):
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
            header_status = Operation.header_check(meta_data=request.META)
            if header_status == 401: return Responses.get_status_401()
            # store id parameter and query
            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            #
            # store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
            # # store category id parameter and query

            # start_timestamp and end_timestamp in epoch(seconds)
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                # date validation
                if end_timestamp < start_timestamp:
                    return Responses.get_status_422(message="end timestamp must be greater than start timestamp")
            except:
                return Responses.get_status_400(params=["start_timestamp", "end_timestamp"])
            # -------------------- service_type parameter --------------------
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            if store_categories_id == "0": store_categories_id = ""
            support_service_type = {0: 'All', 1: 'Delivery', 2: 'Ride', 3: 'Service'}
            try:
                service_type = int(request.GET.get('service_type', 0))
                check = support_service_type[service_type]
            except:
                return Responses.get_status_422(message="service_type must be integer", support=support_service_type)
            # skip and limit parameter
            try:
                skip = int(request.GET.get('skip', 0))
                limit = int(request.GET.get('limit', 10))
            except:
                return Responses.get_status_422(message="skip and limit must be integer")

            # currency support
            currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
            conversion_rate = 1
            if currency != BASE_CURRENCY:
                _currency = Process.currency(currency)
                if _currency["error_flag"]:
                    return Responses.get_status(
                        message=_currency["response_message"], status=_currency["response_status"])
                conversion_rate = _currency["conversion_rate"]

            result_data = DbHelper.promo_data(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                              service_type=service_type)
            # If no Data Found in respective query
            if not result_data.shape[0]:
                return Responses.get_status_404(message='No data found')

            return Operation.promo_process(result_data=result_data,
                                           conversion_rate=conversion_rate,
                                           skip=skip,
                                           limit=limit)
        except Exception as ex:
            traceback.print_exc()
            return Responses.get_status_500(ex)


class TopPromo(APIView):
    def get(self, request):
        """
        GET API: Top N Promo Code as per Discounted value and applied
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
        header_status = Operation.header_check(meta_data=request.META)
        if header_status == 401: return Responses.get_status_401()
        # store id parameter and query
        # try:
        #     store_id = str(request.GET['store_id'])
        #     store_id = "" if store_id == "0" else store_id
        # except:
        #     response = {"message": "mandatory field 'store_id' missing"}
        #     return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
        #
        # store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
        # # store category id parameter and query

        # -------------------- start_timestamp and end_timestamp in epoch(seconds) --------------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                return Responses.get_status_422(message="end timestamp must be greater than start timestamp")
        except:
            return Responses.get_status_400(params=["start_timestamp", "end_timestamp"])

        # -------------------- top N Promo Code (Default = 10) --------------------
        try:
            top = int(request.GET.get('top', 6))
        except:
            return Responses.get_status_422(message="top must be integer")

        # -------------------- currency support --------------------
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return Responses.get_status(
                    message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]

        print("conversion_rate  -->", conversion_rate)
        # -------------------- service_type parameter --------------------
        support_service_type = {0: 'All', 1: 'Delivery', 2: 'Ride', 3: 'Service',5: 'Towme'}
        try:
            service_type = int(request.GET.get('service_type', 0))
            check = support_service_type[service_type]
        except:
            return Responses.get_status_422(message="service_type must be integer", support=support_service_type)
        # -------------------- sort_by parameter --------------------
        sort_by_support = {1: "count", 2: "Discount Value", 3: "Gross Revenue"}
        try:
            sort_by = int(request.GET.get('sort_by', 1))
            check = sort_by_support[sort_by]
        except:
            return Responses.get_status_422(message="sort_by must be integer", support=sort_by_support)
        # ------------------ Promo Data ----------------------------
        result_data = DbHelper.promo_data(start_timestamp=start_timestamp,
                                          end_timestamp=end_timestamp,
                                          service_type=service_type)
        print("result_data: ----->", result_data.shape)
        # If no Data Found in respective query
        if not result_data.shape[0]:
            return Responses.get_status_404(message='No data found')
        # ----------------- Promo Data Operation --------------------
        return Operation.top_promo_process(result_data=result_data,
                                           conversion_rate=conversion_rate,
                                           sort_by=sort_by,
                                           ascending=False,
                                           top=top)
