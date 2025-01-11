from rest_framework.views import APIView
from analytics.settings import BASE_CURRENCY
from .promo_db_helper import DbHelper
from .promo_response_helper import Responses
from .promo_operation_helper import Operation
from analytics.function import Process
import pytz
import traceback

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
            # try:
            #     store_id = str(request.GET['store_id'])
            #     store_id = "" if store_id == "0" else store_id
            # except:
            #     response = {"message": "mandatory field 'store_id' missing"}
            #     return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            #
            # store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
            # # store category id parameter and query

            # ----------- start_timestamp and end_timestamp in epoch(seconds) -----------
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                # date validation
                if end_timestamp < start_timestamp:
                    return Responses.get_status_422(message="end timestamp must be greater than start timestamp")
            except:
                return Responses.get_status_400(params=["start_timestamp", "end_timestamp"])

            # ----------- skip and limit parameter -----------
            try:
                skip = int(request.GET.get('skip', 0))
                limit = int(request.GET.get('limit', 10))
            except:
                return Responses.get_status_422(message="skip and limit must be integer")

            # ----------- service_type parameter -----------
            support_service_type = {0: 'All', 1: 'Delivery', 2: 'Ride', 5: 'Tow'}
            try:
                service_type = int(request.GET.get('service_type', 0))
                check = support_service_type[service_type]
            except:
                return Responses.get_status_422(message="service_type must be integer")

            # ----------- currency support -----------
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
            # ----------- Export the data to excel/csv -----------
            try:
                export = int(request.GET.get('export', 0))
                if export not in [0, 1]:
                    return Responses.get_status_422(message="export must be integer", support=[0, 1])
            except:
                return Responses.get_status_422(message="export must be integer", support=[0, 1])

            # If no Data Found in respective query
            if not result_data.shape[0]:
                return Responses.get_status_404(message='No data found')
            return Operation.promo_process(result_data, conversion_rate, skip, limit, export)
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

        # ----------- skip and limit parameter -----------
        try:
            skip = int(request.GET.get('skip', 0))
            limit = int(request.GET.get('limit', 10))
        except:
            return Responses.get_status_422(message="skip and limit must be integer")
        # -------------------- Sort by (Default = 1 [Count]) --------------------
        support_sort_by = {1: "Count", 2: "Discount"}
        try:
            sort_by = int(request.GET.get('sort_by', 1))
            assert support_sort_by[sort_by]
        except:
            return Responses.get_status_422(message="sort_by must be integer", support=support_sort_by)

        # -------------------- Sort by (Default = 1 [Count]) --------------------
        support_ascending_by = {1: "True", 0: "False"}
        try:
            ascending = int(request.GET.get('ascending', 0))
            assert support_ascending_by[ascending]
        except:
            return Responses.get_status_422(message="ascending must be integer", support=support_ascending_by)
        ascending = bool(ascending)

        # -------------------- currency support --------------------
        currency = str(request.GET.get("currency", BASE_CURRENCY)).upper()
        conversion_rate = 1
        if currency != BASE_CURRENCY:
            _currency = Process.currency(currency)
            if _currency["error_flag"]:
                return Responses.get_status(message=_currency["response_message"], status=_currency["response_status"])
            conversion_rate = _currency["conversion_rate"]
        # ----------- service_type parameter -----------
        support_service_type = {0: 'All', 1: 'Delivery', 2: 'Ride', 5: 'Tow'}
        try:
            service_type = int(request.GET.get('service_type', 0))
            assert support_service_type[service_type]
        except:
            return Responses.get_status_422(message="service_type must be integer")
        # ----------- Export the data to excel/csv -----------
        try:
            export = int(request.GET.get('export', 0))
            if export not in [0, 1]:
                return Responses.get_status_422(message="export must be integer", support=[0, 1])
        except:
            return Responses.get_status_422(message="export must be integer", support=[0, 1])
        # ------------------------------------------------------------
        result_data = DbHelper.promo_data(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                          service_type=service_type
                                          )
        # If no Data Found in respective query
        if not result_data.shape[0]:
            return Responses.get_status_404(message='No data found')

        return Operation.top_promo_process(result_data=result_data,
                                           conversion_rate=conversion_rate,
                                           sort_by=sort_by,
                                           ascending=ascending,
                                           skip=skip,
                                           limit=limit,
                                           export=export,)

class PromoCountAnalysis(APIView):
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
        # -------------------- start_timestamp and end_timestamp in epoch(seconds) --------------------
        try:
            start_timestamp = int(request.GET["start_timestamp"])
            end_timestamp = int(request.GET["end_timestamp"])
            # date validation
            if end_timestamp < start_timestamp:
                return Responses.get_status_422(message="end timestamp must be greater than start timestamp")
        except:
            return Responses.get_status_400(params=["start_timestamp", "end_timestamp"])
        # ----------- service_type parameter -----------
        support_service_type = {0: 'All', 1: 'Delivery', 2: 'Ride', 5: 'Tow'}
        try:
            service_type = int(request.GET.get('service_type', 0))
            assert support_service_type[service_type]
        except:
            return Responses.get_status_422(message="service_type must be integer")
        # -------------------- Store Id param check - mandatory field --------------------
        try:
            store_id = str(request.GET['store_id'])
            store_id = "" if store_id == "0" else store_id
        except:
            message = "mandatory field 'store_id' missing"
            return Responses.get_status_400(params=["start_timestamp", "end_timestamp"], message=message)
        store_query = " AND storeId == '{}'".format(store_id) if store_id else ""

        # -------------------- Store Categories Id --------------------
        store_categories_id = str(request.GET.get("store_categories_id", ""))
        if store_categories_id == "0": store_categories_id = ""
        store_categories_query = " AND storeCategoryId == '{}'".format(store_id) if store_id else ""

        # -------------------- timezone --------------------
        try:
            time_zone = pytz.timezone(str(request.GET['timezone']))
        except Exception as e:
            response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
            return Responses.get_status(message=response, status=400)

        # -------------------- group_by --------------------
        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year',
                          6: "Hour of Day", 7: "Day of Week"}
        try:
            group_by = int(request.GET.get("group_by", 0))
            group_by_check = group_by_value[group_by]
        except:
            return Responses.get_status_422(message="'group_by' must be integer", support=group_by_value)

        return Operation.promo_count_analysis(store_id=store_id,
                                              store_categories_id=store_categories_id,
                                              start_timestamp=start_timestamp,
                                              end_timestamp=end_timestamp,
                                              time_zone=time_zone,
                                              group_by=group_by,
                                              service_type=service_type)
