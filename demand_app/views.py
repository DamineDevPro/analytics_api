from rest_framework.views import APIView
from .demand_response_helper import DemandResponses
from .demand_operations_helper import DemandOperations
from .demand_db_helper import DbHelper
from ast import literal_eval
from django.http import JsonResponse
from rest_framework import status
from analytics.settings import  UTC, db
import pandas as pd

DbHelper = DbHelper()
DemandOperations = DemandOperations()
DemandResponses = DemandResponses()


# Create your views here.


class DemandMeat(APIView):
    def get(self, request):
        """
        GET API : SLA api
        :param request:
        :return:
        """

        try:
            skip = int(request.GET.get('skip', 0))
            limit = int(request.GET.get('limit', 10))
        except:
            return DemandResponses.get_status_400(params=["skip", "limit"])

        dc_id = str(request.GET.get('dc_id', ""))
        dc_id = dc_id if dc_id != "0" else ""

        shift_id = str(request.GET.get('shift_id', "[]"))
        try:
            shift_id = literal_eval(shift_id)
            if not isinstance(shift_id, list):
                message = "issue with respect to shift_id, shift_id must be list"
                return DemandResponses.get_status_400(message=message, params=["shift_id"])
        except:
            message = "issue with respect to shift_id, shift_id must be list"
            return DemandResponses.get_status_400(message=message, params=["shift_id"])

        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            if end_timestamp < start_timestamp:
                message = "end timestamp must be greater than start timestamp"
                return DemandResponses.get_status_400(message=message, params=["skip", "limit"])
        except:
            message = "Missing/Incorrect timestamp, timestamp must be integer"
            return DemandResponses.get_status_400(message=message, params=["skip", "limit"])

        store_id = str(request.GET.get("storeId", "")).strip()

        driver_roaster = DbHelper.driver_roaster_data(
            start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        if not driver_roaster.count():
            message = "No Data Found, Roaster"
            return DemandResponses.get_status_400(message=message, params=[])

        try:
            export = int(request.GET.get("export", 0))
        except:
            return DemandResponses.get_status_422(message="'export' must be 0 or 1")

        driver_roaster_id = DemandOperations.driver_roaster_tranform(driver_roaster=driver_roaster)

        order_data = DbHelper.order_data(driver_roaster_id=driver_roaster_id, store_id=store_id)
        if not len(order_data):
            message = "No Data Found, Order"
            return DemandResponses.get_status_404(message=message)

        return DemandOperations.order_transform(order_data=order_data, skip=skip, limit=limit, dc_id=dc_id,
                                                shift_id=shift_id, export=export)


class ProductDemand(APIView):
    def get(self, request):
        """
        GET API : SLA api
        :param request:
        :return:
        """
        if "product_id" not in request.GET:
            return DemandResponses.get_status_400(params=["product_id"])
        product_id = str(request.GET.get('product_id', "")).strip()
        if not product_id: return DemandResponses.get_status_400(params=["product_id"])
        store_id = str(request.GET.get("storeId", "")).strip()
        dc_id = str(request.GET.get('dc_id', ""))
        dc_id = dc_id if dc_id != "0" else ""

        shift_id = str(request.GET.get('shift_id', "[]"))
        try:
            shift_id = literal_eval(shift_id)
            if not isinstance(shift_id, list):
                message = "issue with respect to shift_id, shift_id must be list"
                return DemandResponses.get_status_400(message=message, params=["shift_id"])
        except:
            message = "issue with respect to shift_id, shift_id must be list"
            return DemandResponses.get_status_400(message=message, params=["shift_id"])

        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            if end_timestamp < start_timestamp:
                message = "end timestamp must be greater than start timestamp"
                return DemandResponses.get_status_400(message=message, params=["skip", "limit"])
        except:
            message = "Missing/Incorrect timestamp, timestamp must be integer"
            return DemandResponses.get_status_400(message=message, params=["skip", "limit"])

        driver_roaster = DbHelper.driver_roaster_data(
            start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        if not driver_roaster.count():
            message = "No Data Found, Roaster"
            return DemandResponses.get_status_400(message=message, params=[])

        driver_roaster_id = DemandOperations.driver_roaster_tranform(driver_roaster=driver_roaster)
        product_data = DbHelper.product_data(driver_roaster_id=driver_roaster_id,
                                             product_id=product_id,
                                             dc_id=dc_id,
                                             shift_id=shift_id,
                                             store_id=store_id)

        if not len(product_data):
            message = "No Data Found, Order"
            return DemandResponses.get_status_204()

        return DemandOperations.product_transform(order_data=product_data,
                                                  dc_id=dc_id,
                                                  shift_id=shift_id)


class Store(APIView):
    def get(self, request):
        try:
            search = request.GET.get('search', "")
            if not search:
                response = {"message": "mandatory field 'search' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
        except:
            response = {"message": "mandatory field 'search' missing"}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        data = db.stores.find({"storeName.en": {"$regex": "^.*" + search + ".*$", "$options": "i"}}, {"storeName": 1})
        if data.count() == 0:
            response = {"message": "No data found"}
            return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
        data = pd.DataFrame(data)
        data["_id"] = data["_id"].apply(lambda x: str(x))
        data = data.to_dict(orient="records")
        response = {"message": "success", "data": data}
        return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
