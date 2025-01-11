import pandas as pd
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
from analytics.settings import db
import traceback
from .operation_helper import Helper
from .responseHelper import ResponseHelper

res = ResponseHelper()
Helper = Helper()

class SellerStats(APIView):
    def get(self, request):
        """
        GET API to provide Seller Stats
        :param request: skip(integer)           
        : page skip if not received 0
        : limit(integer)          
        : page limit if not received
        :return: json data with 200 response status
        """
        try:
            shop_stats = db.sellerStats.find_one({},{"_id": 0})

            response = {"message": "success", "data": shop_stats}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)

        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return res.get_status_500(finalResponse)

class CatalogStats(APIView):
    def get(self, request):
        """
        GET API to provide Seller Stats
        :param request: skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
        :return: json data with 200 response status
        """
        try:
            # authorization from header check
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = eval(request.META['HTTP_AUTHORIZATION'])
            if token['userType'] == 'manager':
                if 'storeId' not in token["metaData"].keys():
                    return JsonResponse({"message": 'unauthorized'}, safe=False, status=401)
                store_id = token["metaData"]["storeId"]
            else:
                store_id = request.GET.get("storeId", "")
                store_id = "" if store_id=="0" else store_id
        # ---------------------- Skip Limit ----------------------
            try:
                skip = int(request.GET.get('skip',0))
                limit = int(request.GET.get('limit',100))
            except Exception as e:
                response = {'message': 'page and limit must be integer values'}
                return res.get_status_400(response)

            store_category_id = request.GET.get("storeCategoryId", "")
            category_text = request.GET.get("search", "")
            response = Helper.process_leaf_category_api(store_id, store_category_id, category_text, int(skip),
                                                                 int(limit))
            if not response:
                return res.get_status_204()

            return res.get_status_200(response)

        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return res.get_status_500(finalResponse)

class Stores(APIView):
    def get(self, request):
        """
        GET API to provide Seller Stats
        :param request: skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
        :return: json data with 200 response status
        """
        try:
            stores = Helper.stores()
            return res.get_status_200(stores)

        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return res.get_status_500(finalResponse)

class LeafCategory(APIView):
    def get(self, request):
        """
        GET API to provide Seller Stats
        :param request: skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
        :return: json data with 200 response status
        """
        try:
            categories = Helper.get_leaf_category()
            return res.get_status_200(categories)

        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return res.get_status_500(finalResponse)

class BrandStats(APIView):
    def get(self, request):
        """
        GET API to provide Seller Stats
        :param request: skip(integer)           : page skip if not received 0
                        limit(integer)          : page limit if not received
        :return: json data with 200 response status
        """
        try:
            # authorization from header check
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = eval(request.META['HTTP_AUTHORIZATION'])
            if token['userType'] == 'manager':
                if 'storeId' not in token["metaData"].keys():
                    return JsonResponse({"message": 'unauthorized'}, safe=False, status=401)
                store_id = token["metaData"]["storeId"]
            else:
                store_id = request.GET.get("storeId", "")
                store_id = "" if store_id=="0" else store_id

        # ---------------------- Skip Limit ----------------------
            try:
                skip = int(request.GET.get('skip',0))
                limit = int(request.GET.get('limit',100))
            except Exception as e:
                response = {'message': 'page and limit must be integer values'}
                return res.get_status_400(response)

            category_id = str(request.GET.get("category_id",''))
            category_id = "" if category_id=="0" else category_id
            response = Helper.get_brands_data(store_id, int(skip), int(limit), category_id)

            if not response:
                return res.get_status_204()

            return res.get_status_200(response)

        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return res.get_status_500(finalResponse)
