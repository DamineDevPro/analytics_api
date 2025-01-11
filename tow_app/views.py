import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta
from django.http import JsonResponse
from rest_framework import status
from rest_framework.views import APIView
from analytics.function import Process
from .tow_db_helper import DbHelper
from .tow_operations_helper import RideOperations
from .tow_response_helper import RideResponses
from analytics.settings import UTC, BASE_CURRENCY, BOOKING_STATUS, DEVICE_SUPPORT

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()
DbHelper = DbHelper()
RideResponses = RideResponses()
RideOperations = RideOperations()
device_support = DEVICE_SUPPORT

class RideFare(APIView):

    @staticmethod
    def get(request):
        """
        GET API: 
        """
        print("GRAPH ---------------------------------->")
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return RideResponses.get_status_401()
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()
        # device Type
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status:
                status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])
        # skip and limit
        try:
            skip = int(request.GET.get("skip", 0))
            limit = int(request.GET.get("limit", 10))
        except:
            response = {"message": "skip and limit must be integer"}
            return RideResponses.get_status_400(response)
        # currency rate
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        currency_symbol = request.GET.get("currency_symbol", "₹")

        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return RideResponses.get_status(
                    message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]

        # timezone
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:

            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return RideResponses.get_status_400(response)
        # group_by
        group_by_value = {
            0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_check = group_by_value[group_by]
        except:
            response = {"message": "'group_by' must be integer", "support": group_by_value}
            return RideResponses.get_status_400(response)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        result_data = DbHelper.ride_trip_data(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              booking_status=booking_status,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id
                                              )
        if not result_data.shape[0]:
            return RideResponses.get_status_204()
        return RideOperations.ride_fare_graph(result_data=result_data,
                                              time_zone=time_zone,
                                              start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              group_by=group_by,
                                              conversion_rate=conversion_rate,
                                              currency_symbol=currency_symbol)


class RideCount(APIView):
    def get(self, request):
        """
        GET API:
        """
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()

        # device Type
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(response, params=["start_timestamp", "end_timestamp"])
        # skip and limit
        try:
            skip = int(request.GET.get("skip", 0))
            limit = int(request.GET.get("limit", 10))
        except:
            response = {"message": "skip and limit must be integer"}
            return RideResponses.get_status_400(response)
        # currency rate
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return RideResponses.get_status(
                    message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]

        # timezone
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:

            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return RideResponses.get_status_400(response)
        # group_by
        group_by_value = {
            0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_check = group_by_value[group_by]
        except:
            response = {"message": "'group_by' must be integer", "support": group_by_value}
            return RideResponses.get_status_400(response)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        result_data = DbHelper.ride_trip_data(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                              booking_status=booking_status, device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id)
        if not result_data.shape[0]:
            return RideResponses.get_status_204()
        return RideOperations.ride_count_graph(result_data, time_zone, start_timestamp, end_timestamp,
                                               group_by)


class Payment(APIView):
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
            return RideResponses.get_status_401()

        # device Type
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        if 'timezone' not in request.GET:
            response = {'message': 'Time Zone Missing'}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
            # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return RideResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                response = {'message': 'export only support 0 and 1'}

                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            message = 'export must be integer'
            return RideResponses.get_status_400(message=message, params=["export"], support={0: False, 1: True})

        vehicle_type_id = str(request.GET.get("vehicle_type_id", ""))
        vehicle_type_id = "" if vehicle_type_id == "0" else vehicle_type_id
        # vehicle_type_query = ""
        # if vehicle_type_id:
        #     vehicle_type_query = " AND vehicleType.typeId typeId == '{}'".format(vehicle_type_id)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        order = DbHelper.ride_payment(start_timestamp=start_timestamp,
                                      end_timestamp=end_timestamp,
                                      vehicle_type_id=vehicle_type_id,
                                      device_type=device_type,
                                      country_id=country_id,
                                      city_id=city_id,
                                      zone_id=zone_id
                                      )
        if not order.shape[0]:
            return RideResponses.get_status_204()
        return RideOperations.ride_payment(order, time_zone, start_timestamp, end_timestamp, group_by, export)


class VehicleType(APIView):
    def get(self, request):
        """
        Vehicle Type API
        :param request:
        :return:
        """

        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()

        data = pd.DataFrame(DbHelper.vehicle_type())

        if not data.shape[0]:
            return RideResponses.get_status_404(message="No data found")

        return RideOperations.vehicle_type(data=data)


class RideStatus(APIView):
    def get(self, request):
        """
        GET API to provide the graphical data with respect to the ride cancellation with respect to driver and customer
        :param request:
        :return:
        """
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()

        # device Type
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        if 'timezone' not in request.GET:
            response = {'message': 'Time Zone Missing'}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
            # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return RideResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                response = {'message': 'export only support 0 and 1'}

                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            message = 'export must be integer'
            return RideResponses.get_status_400(message=message, params=["export"], support={0: False, 1: True})

        vehicle_type_id = str(request.GET.get("vehicle_type_id", ""))
        vehicle_type_id = "" if vehicle_type_id == "0" else vehicle_type_id
        # vehicle_type_query = ""
        # if vehicle_type_id:
        #     vehicle_type_query = " AND vehicleType.typeId typeId == '{}'".format(vehicle_type_id)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        order = DbHelper.ride_status(start_timestamp=start_timestamp,
                                     end_timestamp=end_timestamp,
                                     vehicle_type_id=vehicle_type_id,
                                     device_type=device_type,
                                     country_id=country_id,
                                     city_id=city_id,
                                     zone_id=zone_id
                                     )
        if not order.shape[0]:
            return RideResponses.get_status_204()
        return RideOperations.ride_status(order, time_zone, start_timestamp, end_timestamp, group_by, export)


class TopLocation(APIView):

    def get(self, request):
        print("Top Location request hitted")
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()

        # device Type
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = "end timestamp must be greater than start timestamp"
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                response = {'message': 'export only support 0 and 1'}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            message = 'export must be integer'
            return RideResponses.get_status_400(message=message, params=["export"], support={0: False, 1: True})
        
        try:
            sort_value = {1: "count", 2: "fare"}
            sort = int(request.GET.get('sort', 1))
            sort = sort_value[sort]
        except:
            return RideResponses.get_status_422("sort must be integer, and only support 1 and 2")

        try:
            top = int(request.GET.get('top', 5))
        except:
            return RideResponses.get_status_422("top must be integer")

        # currency rate
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        print(BASE_CURRENCY)
        print("currency---->", currency)
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                print("error flag---> ", _currency)
                return RideResponses.get_status(
                    message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]
            print("conversion_rate----> ", conversion_rate)
            
        currency_symbol = request.GET.get("currency_symbol", "₹")

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        order = DbHelper.top_ride(start_timestamp=start_timestamp,
                                  end_timestamp=end_timestamp,
                                  device_type=device_type,
                                  country_id=country_id,
                                  city_id=city_id,
                                  zone_id=zone_id)
        if not order.shape[0]:
            return RideResponses.get_status_204()
        return RideOperations.top_ride(order=order,
                                       sort=sort,
                                       top=top,
                                       conversion_rate=conversion_rate,
                                       currency_symbol=currency_symbol)


class Map(APIView):
    def get(self, request):

        if 'HTTP_AUTHORIZATION' not in request.META:
            return RideResponses.get_status_401()
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()

        # device Type
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

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
        # zone_id = request.GET.get("zone_id", "").strip()
        # city_id = request.GET.get("city_id", "").strip()
        # country_id = request.GET.get("country_id", "").strip()
        # status_code = int(request.GET.get("status_code", 0))

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        location_status = {1: "pickup", 2: "drop"}
        try:
            location = int(request.GET.get("location", 1))
            location = location_status[location]
        except:
            response = {"message": "Incorrect location type selected", "support": location_status}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        result_data = DbHelper.ride_map_data(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                             device_type=device_type,
                                             country_id=country_id,
                                             city_id=city_id,
                                             zone_id=zone_id)
        if not result_data.shape[0]:
            response = {'message': 'No data found'}
            return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

        return RideOperations.ride_map_process(result_data=result_data, location=location)


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
                return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

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
                    return RideResponses.get_status(message=_currency["response_message"],
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

            return RideOperations.descriptive_stats(previous_sum=previous_sum,
                                                    current_sum=current_sum,
                                                    previous_start_date=previous_start_date,
                                                    previous_end_date=previous_end_date,
                                                    today_sum=today_sum)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)


class RideCountTable(APIView):
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
            return RideResponses.get_status_401()

        # ------------- device Type -------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        if 'timezone' not in request.GET:
            response = {'message': 'Time Zone Missing'}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
            # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return RideResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                response = {'message': 'export only support 0 and 1'}

                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            message = 'export must be integer'
            return RideResponses.get_status_400(message=message, params=["export"], support={0: False, 1: True})

        try:
            booking_status = int(request.GET.get("status", 0))
            assert BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        vehicle_type_id = str(request.GET.get("vehicle_type_id", ""))
        vehicle_type_id = "" if vehicle_type_id == "0" else vehicle_type_id
        # vehicle_type_query = ""
        # if vehicle_type_id:
        #     vehicle_type_query = " AND vehicleType.typeId typeId == '{}'".format(vehicle_type_id)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        order = DbHelper.ride_payment(start_timestamp=start_timestamp,
                                      end_timestamp=end_timestamp,
                                      vehicle_type_id=vehicle_type_id,
                                      device_type=device_type,
                                      country_id=country_id,
                                      city_id=city_id,
                                      zone_id=zone_id,
                                      booking_status=booking_status)
        if not order.shape[0]:
            return RideResponses.get_status_204()
        return RideOperations.ride_count(order, time_zone, start_timestamp, end_timestamp, group_by, export)


class RideFareTable(APIView):
    def get(self, request):
        """
        Order activity with respect to payment aggregated as per time period drill down to vehicle type

        :param request:
        :return:
        """
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()

        # ------------- device Type -------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])

        if 'timezone' not in request.GET:
            response = {'message': 'Time Zone Missing'}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
            # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return RideResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        # currency rate
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        currency_symbol = request.GET.get("currency_symbol", "₹")

        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return RideResponses.get_status(
                    message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]

        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                response = {'message': 'export only support 0 and 1'}

                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            message = 'export must be integer'
            return RideResponses.get_status_400(message=message, params=["export"], support={0: False, 1: True})

        vehicle_type_id = str(request.GET.get("vehicle_type_id", ""))
        vehicle_type_id = "" if vehicle_type_id == "0" else vehicle_type_id
        vehicle_type_query = ""
        vehicle_mongo_query = {}
        if vehicle_type_id:
            vehicle_type_query = " AND vehicleType.typeId typeId == '{}'".format(vehicle_type_id)
            vehicle_mongo_query["vehicleType.typeId"] = vehicle_type_id
        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        order = DbHelper.ride_fare(start_timestamp=start_timestamp,
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
            return RideResponses.get_status_204()
        return RideOperations.ride_fare(order=order,
                                        time_zone=time_zone,
                                        start_timestamp=start_timestamp,
                                        end_timestamp=end_timestamp,
                                        group_by=group_by,
                                        export=export,
                                        currency=currency,
                                        conversion_rate=conversion_rate,
                                        currency_symbol=currency_symbol,
                                        )


class SurgePrice(APIView):
    def get(self, request):
        # ------------- Authorization -------------
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()
        # ------------- device Type -------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ------------- timezone -------------
        if 'timezone' not in request.GET:
            response = {'message': 'Time Zone Missing'}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        # ------------- start_timestamp and end_timestamp in epoch(seconds) -------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # ------------- date validation -------------
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                RideResponses.get_status_422(response)
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])
        # ------------- group_by(int) -------------
        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            assert group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return RideResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)
        # ------------- currency rate -------------
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        currency_symbol = request.GET.get("currency_symbol", "₹")
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return RideResponses.get_status(
                    message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]
        # ------------- export -------------
        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                response = {'message': 'export only support 0 and 1'}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
        except:
            message = 'export must be integer'
            return RideResponses.get_status_400(message=message, params=["export"], support={0: False, 1: True})
        # ------------- Location Filter -------------
        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))
        # ------------- QUERY -------------
        order = DbHelper.surge_fare(start_timestamp=start_timestamp,
                                    end_timestamp=end_timestamp,
                                    device_type=device_type,
                                    country_id=country_id,
                                    city_id=city_id,
                                    zone_id=zone_id)
        if not order.shape[0]:
            response = {'message': 'No data found'}
            return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
        # ------------- DATA -------------
        return RideOperations.surge_fare(order=order,
                                         time_zone=time_zone,
                                         conversion_rate=conversion_rate,
                                         start_timestamp=start_timestamp,
                                         end_timestamp=end_timestamp,
                                         group_by=group_by,
                                         export=export,
                                         currency_symbol=currency_symbol)
