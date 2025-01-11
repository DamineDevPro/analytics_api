from rest_framework.views import APIView
from django.http import JsonResponse
from rest_framework import status
from .export_db_helper import DbHelper
from .export_response_helper import ExportResponses
from .export_operations_helper import ExportOperations
from analytics.settings import  UTC, db, ECOMMERCE_SERVICE as SERVICE_SUPPORT, PLATFORM_SUPPORT
import pandas as pd
import traceback

DbHelper = DbHelper()
ExportOperations = ExportOperations()
ExportResponses = ExportResponses()

class Export(APIView):
    def get(self, request):
        try:
            # -------------------- type message --------------------
            try:
                _type = int(request.GET.get("type"))
                print("type----?",type)
                # type_msg = SERVICE_SUPPORT[_type]
            except:
                message = "mandatory field 'type' missing/Invalid"
                response = {"message": message, "support": SERVICE_SUPPORT}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            # try:
            #     store_id = request.GET["storeId"]
            # except:
            #     message = "mandatory field 'storeId' missing/Invalid"
            #     response = {"message": message}
            #     return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            # --------------- Start Time and End Time -----------------
            start_time = int(request.GET.get("start_time", 0))
            end_time = int(request.GET.get("end_time", 0))
            # --------------------- Skip and limit ---------------------
            # skip = int(request.GET.get("skip", 0))
            # limit = int(request.GET.get("limit", 10))
            # ----------------------- Platform -----------------------
            try:
                platform = int(request.GET.get("platform", 0))
                assert PLATFORM_SUPPORT[platform]
            except:
                message = "mandatory field 'platform' missing/Invalid"
                response = {"message": message, "support": PLATFORM_SUPPORT}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            store_category_id = ""
            # ----------------------- Mongo Query -----------------------
            export_data, count = DbHelper.data(
                start_time=start_time,
                end_time=end_time,
                _type=_type,
                store_category_id=store_category_id,
                platform=platform
            )
            # ------------------------------------------------------------
            if not export_data.count():
                response = {"message": "No Data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            response = {"message": "success", "data": list(export_data), "count": count}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def post(self, request):
        print("request hitted")
        print("requested data-------->", request.data)
        try:
            _type = int(request.data.get("type"))
            print("type------", _type)
            # assert SERVICE_SUPPORT[_type]
        except:
            message = "mandatory field 'type' missing/Invalid"
            response = {"message": message, "support": SERVICE_SUPPORT}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        # --------------------------- Request Checking ------------------------------------
        operation = {1: ExportOperations.hourly_fee_report,
                     2: ExportOperations.mileage_stop_off,
                     4: ExportOperations.per_stop_report,
                     }

        return operation[_type](request=request)

class Loads(APIView):

    def post(self, request):
        print("request hitted loads")
        print("requested data-------->", request.data)
        _type = 5

        # --------------------------- Request Checking ------------------------------------
        operation = {5: ExportOperations.loads
                     }

        return operation[_type](request=request)
