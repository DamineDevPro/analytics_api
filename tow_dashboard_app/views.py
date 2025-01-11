import os
import sys
import traceback

import pandas as pd
import pytz
from django.http import JsonResponse
from rest_framework import status
from rest_framework.views import APIView

from analytics.function import Process
from analytics.settings import db, BASE_CURRENCY, BOOKING_STATUS, DEVICE_SUPPORT
from .ride_dashboard_db_helper import DbHelper
from .ride_dashboard_operations_helper import RideOperations
from .ride_dashboard_response_helper import RideResponses
from django.core.wsgi import get_wsgi_application

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

application = get_wsgi_application()
DbHelper = DbHelper()
RideResponses = RideResponses()
RideOperations = RideOperations()
device_support = DEVICE_SUPPORT


class RideRevenue(APIView):
    def get(self, request):
        """
        GET API: 
        """
        # ---------------------------- Auth ----------------------------
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return RideResponses.get_status_401()
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()
        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status:
                status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        # ------------------ start_timestamp and end_timestamp in epoch(seconds) ----------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])

        # ------------------ currency rate ------------------
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return RideResponses.get_status(message=_currency["response_message"],
                                                status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]
        currency_symbol = request.GET.get("currency_symbol", "₹")

        # ------------------ timezone ------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return RideResponses.get_status_400(response)
        # ------------------ group_by ------------------
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

        # ------------------ DATA ------------------
        result_data = DbHelper.ride_trip_data(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              booking_status=booking_status,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id
                                              )

        if not result_data.shape[0]: return RideResponses.get_status_204()
        # ------------------ Operation ------------------
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
        # ---------------------------- Auth ----------------------------
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()
        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        # ------------------ start_timestamp and end_timestamp in epoch(seconds) ----------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(response, params=["start_timestamp", "end_timestamp"])
        # ------------------ timezone ------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:

            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return RideResponses.get_status_400(response)
        # ------------------ group_by ------------------
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

        # ------------------ DATA ------------------
        result_data = DbHelper.ride_trip_data(start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              booking_status=booking_status,
                                              device_type=device_type,
                                              country_id=country_id,
                                              city_id=city_id,
                                              zone_id=zone_id
                                              )

        if not result_data.shape[0]: return RideResponses.get_status_204()
        # ------------------ Operation ------------------
        return RideOperations.ride_count_graph(result_data=result_data,
                                               time_zone=time_zone,
                                               start_timestamp=start_timestamp,
                                               end_timestamp=end_timestamp,
                                               group_by=group_by)


class RideStatus(APIView):
    def get(self, request):
        """
        GET API to provide the graphical data with respect to the ride cancellation with respect to driver and customer
        :param request:
        :return:
        """
        # ---------------------------- Auth ----------------------------

        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            return RideResponses.get_status_401()
        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        # ---------------------------- time_zone ----------------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        # ----------------- start_timestamp and end_timestamp in epoch(seconds) -----------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
        except:
            response = {"message": "Mandatory/Incorrect parameter"}
            return RideResponses.get_status_400(message=response, params=["start_timestamp", "end_timestamp"])
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')
        # ---------------------------- group_by ----------------------------
        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                          4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_period = group_by_value[group_by]
        except:
            message = "'group_by' must be integer"
            return RideResponses.get_status_400(message=message, params=["group_by"], support=group_by_value)

        country_id = str(request.GET.get("country_id", ""))
        city_id = str(request.GET.get("city_id", ""))
        zone_id = str(request.GET.get("zone_id", ""))

        # ---------------------------- DATA ----------------------------
        order = DbHelper.ride_status(start_timestamp=start_timestamp,
                                     end_timestamp=end_timestamp,
                                     device_type=device_type,
                                     country_id=country_id,
                                     city_id=city_id,
                                     zone_id=zone_id,
                                     booking_status=booking_status)
        if not order.shape[0]: return RideResponses.get_status_204()
        # ------------------ Operation ------------------
        return RideOperations.ride_status(order=order,
                                          time_zone=time_zone,
                                          start_timestamp=start_timestamp,
                                          end_timestamp=end_timestamp,
                                          group_by=group_by)


class RidePayment(APIView):
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
        # ---------------------------- booking_status ----------------------------
        try:
            booking_status = int(request.GET.get("status", 0))
            if booking_status: status_check = BOOKING_STATUS[booking_status]
        except:
            return RideResponses.get_status_422('booking status issue')

        # ---------------------------- device Type ----------------------------
        try:
            device_type = int(request.GET.get("device_type", "0"))
            assert device_support[device_type]
        except:
            response = {"message": "Mandatory/Incorrect 'device_type' parameter"}
            return RideResponses.get_status_400(message=response, support=device_support, params=["device_type"])
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', "error": type(e).__name__}
            return RideResponses.get_status_400(message=response, params=["timezone"])
        # start_timestamp and end_timestamp in epoch(seconds)
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
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

        order = DbHelper.ride_payment(start_timestamp=start_timestamp,
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
        return RideOperations.ride_payment(order, time_zone, start_timestamp, end_timestamp, group_by)


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
            result_data = pd.DataFrame(db["operationZones_rides"].find(query, projection))
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
