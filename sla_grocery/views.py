from .grocery_db_helper import DbHelper
from rest_framework.views import APIView
from .grocery_response_helper import GroceryResponses
from .grocery_operations_helper import GroceryOperations


##### creating objects of helper classes #####
CartResponses = GroceryResponses()
DbHelper = DbHelper()
GroceryOperations = GroceryOperations()


class GroceryMeat(APIView):
    def get(self, request):
        """
        GET API : SLA api
        :param request:
        :return:
        """
        try:
            header_status = GroceryOperations.header_check(meta_data=request.META)
            if header_status == 401: return GroceryResponses.get_status_401()

            missing_params = []
            store_id = request.GET.get('storeId')
            if not store_id: missing_params.append('storeId')
            store_id = "" if store_id == "0" else store_id
            if missing_params: return GroceryResponses.get_status_400(params=missing_params)
            try:
                skip = int(request.GET.get('skip', 0))
                limit = int(request.GET.get('limit', 10))
            except:
                return GroceryResponses.get_status_400(params=["skip", "limit"])

            data = DbHelper.grocery_store_order(store_id, skip, limit)
            if not data.count():
                return GroceryResponses.get_status_204()
            count = int(DbHelper.grocery_store_order(store_id, skip, limit, count=True).count())
            return GroceryOperations.get_grocery(data, count)
        except Exception as ex:

            GroceryResponses.get_status_500(ex)

class DcDemand(APIView):
    def get(self, request):
        """
        GET API : SLA api
        :param request:
        :return:
        """
        try:
            header_status = GroceryOperations.header_check(meta_data=request.META)
            if header_status == 401: 
                return GroceryResponses.get_status_401()

            return GroceryOperations.get_demand(request)

        except Exception as ex:

            GroceryResponses.get_status_500(ex)

class FCStores(APIView):
    def get(self, request):
        """
        GET API : SLA api
        :param request:
        :return:
        """
        try:
            header_status = GroceryOperations.header_check(meta_data=request.META)
            if header_status == 401: 
                return GroceryResponses.get_status_401()

            return GroceryOperations.all_fc_stores(request)

        except Exception as ex:

            GroceryResponses.get_status_500(ex)
