import traceback
import pytz
from django.shortcuts import render
from rest_framework.views import APIView
from .operationHelper import OperationHelper
from .dbHelper import DbHelper
from .responseHelper import ResponsesHelper
from asgiref.sync import async_to_sync

opr = OperationHelper()
res = ResponsesHelper()
dbhelper = DbHelper()

class Utilisation(APIView):
    def get(self, request):
        # try:
        ################ Authorization #################
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return res.get_status_401(response)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            response_data = {
                "message": "Unauthorized",
            }
            return res.get_status_401(response_data)

        # -------------------- type message --------------------
        try:
            store_id = request.GET["storeId"]
        except:
            message = "mandatory field 'storeId' missing/Invalid"
            response = {"message": message}
            return res.get_status_400(response=response)
        
        # --------------- Start Time and End Time -----------------
        print("storeid------->", store_id)
        start_time = int(request.GET.get("start_time", 0))
        end_time = int(request.GET.get("end_time", 0))
        
        # --------------------- Skip and limit ---------------------
        skip = int(request.GET.get("skip", 0))
        limit = int(request.GET.get("limit", 10))
        store_category_id = ""

        if 'timezone' not in request.GET:
            response = {'message': 'timezone is missing'}
            return res.get_status_400(response)
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return res.get_status_400(response)

        ############ Group-By Check ###############
        group_by = opr.group_by_check(request)
        if isinstance(group_by, dict):
            return res.get_status_400(group_by)

        # -------------------- Get data from mongo --------------------
        data = dbhelper.utilization(start_time, end_time)

        # -------------------- Check if empty dataframe ----------------------
        if not data.shape[0]:
            return res.get_status_204()

        data, graph = opr.utilization(data, time_zone, group_by)
        response = {
            "graph": graph,
            "table": data.to_dict(orient="records"),
            "Mean VHU Volume": data["volumne_ratio"].mean(),
            "VHU Weight": data["weight_ratio"].mean()
        }
        return res.get_status_200(response)

        # except Exception as ex:
        #         traceback.print_exc()
        #         return res.get_status_500(ex)

class Dispatch(APIView):
    def get(self, request):
        try:
            # authorization from header check
            # if 'HTTP_AUTHORIZATION' not in request.META:
            #     response = {'message': 'AUTHORIZATION is missing in header'}
            #     return res.get_status_401(response)
            # token = eval(request.META['HTTP_AUTHORIZATION'])
            # if token['userType'] == 'manager':
            #     if 'storeId' not in token["metaData"].keys():
            #         return res.get_status_401(response)
            #     store_id = token["metaData"]["storeId"]
            # else:
            #     store_id = request.GET.get("storeId", "")
            #     store_id = "" if store_id=="0" else store_id


            #---------------- driver-check -------------------
            # 0 - Company Driver
            # 1 - Independent Driver
            driver_type = opr.driver_check(request)
            if isinstance(driver_type, dict):
                return res.get_status_400(driver_type)

            # -------------------- type message --------------------
            try:
                store_id = request.GET["storeId"]
            except:
                message = "mandatory field 'storeId' missing/Invalid"
                response = {"message": message}
                return res.get_status_400(response=response)

            # --------------- Start Time and End Time -----------------
            print("storeid------->", store_id)
            start_time = int(request.GET.get("start_time", 0))
            end_time = int(request.GET.get("end_time", 0))

            # --------------------- Skip and limit ---------------------
            skip = int(request.GET.get("skip", 0))
            limit = int(request.GET.get("limit", 10))
            store_category_id = ""

            # -------------------- Get data from mongo --------------------
            data = dbhelper.dispatch(start_time, end_time, driver_type)

            # -------------------- Check if empty dataframe ----------------------
            if not data.shape[0]:
                return res.get_status_204()

            data = opr.dispatch(data=data)

            

            return opr.utilization(data)

        except Exception as ex:
                traceback.print_exc()
                return res.get_status_500(ex)

class Delivery(APIView):
    def get(self, request):
        # try:
        ################ Authorization #################
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return res.get_status_401(response)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            response_data = {
                "message": "Unauthorized",
            }
            return res.get_status_401(response_data)

        # --------------- Start Time and End Time -----------------
        start_time = int(request.GET.get("start_time", 0))
        end_time = int(request.GET.get("end_time", 0))
        
        # --------------------- Skip and limit ---------------------
        skip = int(request.GET.get("skip", 0))
        limit = int(request.GET.get("limit", 10))

        # ---------------------- TimeZone Check ----------------------
        if 'timezone' not in request.GET:
            response = {'message': 'timezone is missing'}
            return res.get_status_400(response)
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return res.get_status_400(response)
        
        ############ Group-By Check ###############
        group_by = opr.group_by_check(request)
        if isinstance(group_by, dict):
            return res.get_status_400(group_by)

        #---------------- driver-check -------------------
        # 0 - Company Driver
        # 1 - Independent Driver
        driver_type = opr.driver_check(request)
        if isinstance(driver_type, dict):
            return res.get_status_400(driver_type)

        # -------------------- Get data from mongo --------------------
        data = dbhelper.acceptance(start_time, end_time, driver_type)

        # -------------------- Check if empty dataframe ----------------------
        if not data.shape[0]:
            return res.get_status_204()

        data, bar_graph, pie_chart = async_to_sync(opr.acceptance)(data, time_zone)
        response = {
            "bar_graph": bar_graph,
            "pie_chart": pie_chart,
            "table": data.to_dict(orient="records")
        }
        return res.get_status_200(response)

        # except Exception as ex:
        #         traceback.print_exc()
        #         return res.get_status_500(ex)

class Acceptance(APIView):
    def get(self, request):
        # try:
        ################ Authorization #################
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return res.get_status_401(response)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            response_data = {
                "message": "Unauthorized",
            }
            return res.get_status_401(response_data)

        # --------------- Start Time and End Time -----------------
        start_time = int(request.GET.get("start_time", 0))
        end_time = int(request.GET.get("end_time", 0))
        
        # --------------------- Skip and limit ---------------------
        skip = int(request.GET.get("skip", 0))
        limit = int(request.GET.get("limit", 10))

        # ---------------------- TimeZone Check ----------------------
        if 'timezone' not in request.GET:
            response = {'message': 'timezone is missing'}
            return res.get_status_400(response)
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return res.get_status_400(response)
        
        #---------------- driver-check -------------------
        # 0 - Company Driver
        # 1 - Independent Driver
        driver_type = opr.driver_check(request)
        if isinstance(driver_type, dict):
            return res.get_status_400(group_by)

        ############ Group-By Check ###############
        group_by = opr.group_by_check(request)
        if isinstance(group_by, dict):
            return res.get_status_400(group_by)

        # -------------------- Get data from mongo --------------------
        data = dbhelper.acceptance(start_time, end_time, driver_type)

        # -------------------- Check if empty dataframe ----------------------
        if not data.shape[0]:
            return res.get_status_204()

        data, bar_graph, pie_chart = async_to_sync(opr.acceptance)(data, time_zone)
        response = {
            "bar_graph": bar_graph,
            "pie_chart": pie_chart,
            "table": data.to_dict(orient="records")
        }
        return res.get_status_200(response)

        # except Exception as ex:
        #         traceback.print_exc()
        #         return res.get_status_500(ex)

class Performance(APIView):
    def get(self, request):
        # try:
        ################ Authorization #################
        if 'HTTP_AUTHORIZATION' not in request.META:
            response = {'message': 'AUTHORIZATION is missing in header'}
            return res.get_status_401(response)
        token = request.META['HTTP_AUTHORIZATION']
        if token == "":
            response_data = {
                "message": "Unauthorized",
            }
            return res.get_status_401(response_data)

        # --------------- Start Time and End Time -----------------
        start_time = int(request.GET.get("start_time", 0))
        end_time = int(request.GET.get("end_time", 0))
        
        # --------------------- Skip and limit ---------------------
        skip = int(request.GET.get("skip", 0))
        limit = int(request.GET.get("limit", 10))

        # ---------------------- TimeZone Check ----------------------
        if 'timezone' not in request.GET:
            response = {'message': 'timezone is missing'}
            return res.get_status_400(response)
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return res.get_status_400(response)
        
        #---------------- driver-check -------------------
        # 0 - Company Driver
        # 1 - Independent Driver
        driver_type = opr.driver_check(request)
        if isinstance(driver_type, dict):
            return res.get_status_400(group_by)

        ############ Group-By Check ###############
        group_by = opr.group_by_check(request)
        if isinstance(group_by, dict):
            return res.get_status_400(group_by)

        # -------------------- Get data from mongo --------------------
        data = dbhelper.performance(start_time, end_time, driver_type)

        # -------------------- Check if empty dataframe ----------------------
        if not data.shape[0]:
            return res.get_status_204()

        data, bar_graph, pie_chart = async_to_sync(opr.performance)(data, time_zone)
        response = {
            "bar_graph": bar_graph,
            "pie_chart": pie_chart,
            "table": data.to_dict(orient="records")
        }
        return res.get_status_200(response)

        # except Exception as ex:
        #         traceback.print_exc()
        #         return res.get_status_500(ex)

