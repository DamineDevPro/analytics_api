from rest_framework.views import APIView
from .ride_response_helper import RideResponses
from .ride_operations_helper import RideOperations
from .ride_db_helper import DbHelper


class RideSla(APIView):
    def get(self, request):
        """
        GET API : SLA api
        :param request:
        :return:
        """
        # header_status = RideOperations.header_check(meta_data=request.META)
        # if header_status == 401: return RideResponses.get_status_401()
        try:
            try:
                skip = int(request.GET.get('skip', 0))
                limit = int(request.GET.get('limit', 10))
            except:
                return RideResponses.get_status_400(params=["skip", "limit"])

            booking_data = DbHelper.booking(skip=skip, limit=limit)
            if not booking_data.count(): return RideResponses.get_status_204()
            count = int(DbHelper.booking(skip=skip, limit=limit, count=True).count())
            return RideOperations.booking_data(booking_data=booking_data, count=count)
        except Exception as ex:
            RideResponses.get_status_500(ex)


class DriverPerformance(APIView):
    def get(self, request):
        """
        GET API: Driver Performance

        """
        # API Authorization
        # if 'HTTP_AUTHORIZATION' not in request.META:
        #     response = {'message': 'AUTHORIZATION is missing in header'}
        #     return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
        # token = request.META['HTTP_AUTHORIZATION']
        # if token == "":
        #     response_data = {
        #         "message": "Unauthorized",
        #     }
        #     return JsonResponse(response_data, safe=False, status=401)

        # Start time and End Time
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            if end_timestamp < start_timestamp:
                message = "end timestamp must be greater than start timestamp"
                return RideResponses.get_status_404(message)
        except:
            message = "Missing/Incorrect timestamp, timestamp must be integer"
            return RideResponses.get_status_400(message=message, params=["start_timestamp", "end_timestamp"])

        # Store Id param check - mandatory field
        try:
            store_id = str(request.GET['store_id'])
            store_id = "" if store_id == "0" else store_id
        except:
            return RideResponses.get_status_400(params=["store_id"])

        try:
            skip = int(request.GET.get("skip", 0))
            limit = int(request.GET.get("limit", 20))
        except:
            return RideResponses.get_status_400(params=["skip", "limit"])
        sort_by = "timestamps.new"
        data = DbHelper.driver_performance(store_id=store_id, skip=skip, limit=limit, sort_by=sort_by)
        if data.count() == 0:
            print("No Data Found")
            return RideResponses.get_status_204()

        return RideOperations.driver_data(data)
