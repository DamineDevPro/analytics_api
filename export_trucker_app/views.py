from rest_framework.views import APIView
from django.http import JsonResponse
from rest_framework import status
from .export_ride_db_helper import DbHelper
from .export_ride_response_helper import ExportResponses
from .export_ride_operations_helper import ExportOperations
import traceback
from analytics.settings import RIDE_SERVICE_SUPPORT, PLATFORM_SUPPORT

DbHelper = DbHelper()
ExportOperations = ExportOperations()
ExportResponses = ExportResponses()

class RideExport(APIView):
    def get(self, request):
        # -------------------- type message --------------------
        try:
            _type = int(request.GET.get("type"))
            type_msg = RIDE_SERVICE_SUPPORT[_type]
        except:
            message = "mandatory field 'type' missing/Invalid"
            response = {"message": message, "support": RIDE_SERVICE_SUPPORT}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        # --------------- Start Time and End Time -----------------
        try:
            start_time = int(request.GET.get("start_time", 0))
            end_time = int(request.GET.get("end_time", 0))
        except:
            response = {"message": "Incorrect/Unsupported Timestamp, 'start_time' or 'end_time', must be integer"}
            return ExportResponses.get_status_422(response)
        # --------------------- Skip and limit ---------------------
        try:
            skip = int(request.GET.get("skip", 0))
            limit = int(request.GET.get("limit", 10))
        except:
            response = {"message": "Incorrect/Unsupported 'skip' or 'limit', must be integer"}
            return ExportResponses.get_status_422(response)
        # --------------------- File Status ---------------------
        try:
            file_status = int(request.GET.get("status", 0))
        except:
            response = {"message": "Incorrect/Unsupported 'status', must be integer"}
            return ExportResponses.get_status_422(response)
        # -------------------- Platform ---------------------------
        try:
            platform = int(request.GET["platform"])
            assert PLATFORM_SUPPORT[platform]
        except:
            message = "mandatory field 'platform' missing/Invalid"
            response = {"message": message, "support": PLATFORM_SUPPORT}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        city_id = request.GET.get("city_id", "")
        # ----------------------- Mongo Query -----------------------
        export_data, count = DbHelper.data(start_time=start_time, end_time=end_time, _type=_type,
                                           file_status=file_status, platform=platform, city_id=city_id)
        # ------------------------------------------------------------
        if not count:
            response = {"message": "No Data Found"}
            return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
        response = {"message": "success", "data": list(export_data.skip(skip).limit(limit)), "count": count}
        return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

    def post(self, request):
        try:
            _type = int(request.data.get("type"))
            type_msg = RIDE_SERVICE_SUPPORT[_type]
        except:
            message = "mandatory field 'type' missing/Invalid"
            response = {"message": message, "support": RIDE_SERVICE_SUPPORT}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
        # -------------------- Platform ---------------------------
        try:
            platform = int(request.data["platform"])
            assert PLATFORM_SUPPORT[platform]
        except:
            message = "mandatory field 'platform' missing/Invalid"
            response = {"message": message, "support": PLATFORM_SUPPORT}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
        # ------------------- Request Checking ------------------------------------
        operation = {6: ExportOperations.trip_invoice,
                     7: ExportOperations.fare_estimate,
                     8: ExportOperations.bookings,
                     9: ExportOperations.financial_logs,
                     10: ExportOperations.acceptance_rate
                     }
        try:
            return operation[_type](request=request)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)
