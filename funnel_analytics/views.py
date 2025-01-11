import pandas as pd
from rest_framework import status
from django.http import JsonResponse
from rest_framework.views import APIView
from analytics.settings import db
import traceback

"""
class FunnelPlatform(APIView):
    def get(self, request):
        '''
        HTTP_AUTHORIZATION
        store_categories_id
        start_timestamp
        end_timestamp
        param request:
        return:
        '''
        try:
            # authorization from header check
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            # store category parameter and query add on for spark sql
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = store_categories_id if store_categories_id != "0" else ""
            store_categories_query = ""
            if store_categories_id:
                store_categories_query = "AND storeCategoryId == '{}' ".format(store_categories_id)

            store_id = str(request.GET.get("store_id", ""))
            store_id = store_id if store_id != "0" else ""
            store_query = ""
            if store_id != "0" and store_id:
                store_query = "AND storeId == '{}' ".format(store_id)

            # start_timestamp and end_timestamp in epoch(seconds)
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
            except:
                response = {"message": "Incorrect/Missing 'start_timestamp' and 'end_timestamp'"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # Spark SQL query for session logs
            query = "SELECT _id, device, make FROM sessionLogs WHERE sessionStart BETWEEN {} AND {}".format(
                start_timestamp, end_timestamp)
            query = query.strip()
            try:
                assert False
                session_logs = sqlContext.sql(query)
                session_logs = session_logs.toPandas()
                if session_logs.shape[0]:
                    session_logs["_id"] = session_logs["_id"].apply(lambda x: x.oid)
            except:
                query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp}}
                print("session_logs -->", query)
                session_logs = pd.DataFrame(db.sessionLogs.find(query, {"_id": 1, "device": 1, "make": 1}))
                if session_logs.shape[0]:
                    session_logs["_id"] = session_logs["_id"].astype(str)
            if not session_logs.shape[0]:
                response = {"message": "No data Found, No Session Data"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            # Spark SQL query for cart
            query = "SELECT _id, sessionId, sellers.fullFillMentCenterId fullFillMentCenterId FROM cart "
            date_range_query = "WHERE cartLogs.timestamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
            query = query + date_range_query + store_categories_query
            try:
                assert False
                query = query.strip()
                cart_logs = sqlContext.sql(query)
                cart_logs = cart_logs.toPandas()
                if cart_logs.shape[0]:
                    cart_logs["_id"] = cart_logs["_id"].apply(lambda x: x.oid)
            except:
                query = {"cartLogs.timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                cart_logs = pd.DataFrame(
                    db.cart.find(query, {"_id": 1, "sessionId": 1, "sellers.fullFillMentCenterId": 1}))
                if cart_logs.shape[0]:
                    cart_logs["fullFillMentCenterId"] = cart_logs["sellers"].apply(
                        lambda x: x.get("fullFillMentCenterId", "") if isinstance(x, dict) else x)
                    cart_logs = cart_logs.drop("sellers", axis=1, errors="ignore")
                    cart_logs["_id"] = cart_logs["_id"].astype(str)

            # if no cart data available bad request
            if not cart_logs.shape[0]:
                response = {"message": "No data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # cart_logs["type"] = cart_logs["sessionId"].apply(lambda x: type(x))  # type casting
            # cart_logs = cart_logs[cart_logs["type"].astype(str) != "<class 'dict'>"].drop("type", axis=1)
            cart_logs = cart_logs[cart_logs["sessionId"].astype(str) != ""]
            print("Cart_logs---->", cart_logs)
            print("store_id---->", store_id)

            # ------------------------ session filter as per store in cart ------------------------------------
            if store_id:
                cart_logs["check"] = cart_logs["fullFillMentCenterId"].apply(lambda x: 1 if store_id in x else 0)
                cart_logs = cart_logs[cart_logs["check"] == 1]
            cart_logs = cart_logs.drop(["fullFillMentCenterId", "check"], axis=1, errors="ignore")
            # ------------------------- session filter as per store in sessionLogs ----------------------------
            cart_session_ids = list(cart_logs["sessionId"])
            # print(cart_session_ids)

            session_logs = session_logs[session_logs["_id"].isin(cart_session_ids)]
            print("session_logs--->",session_logs)
            # -------------------------------------------------------------------------------------------------
            
            cart_logs = cart_logs.rename(columns={"_id": "cartId"})
            print(cart_logs)
            merge_log = pd.merge(cart_logs, session_logs, left_on="sessionId", right_on="_id", how="inner")
            print("merge_log----->",merge_log)
            # try:
            #     merge_log["cartId"] = merge_log["cartId"].apply(lambda x: x.oid if not isinstance(x, float) else x)
            # except:
            #     merge_log["cartId"] = merge_log["cartId"].apply(lambda x: str(x) if not isinstance(x, float) else x)

            merge_log = merge_log.drop("sessionId", axis=1)

            # Spark SQL query for storeOrder
            try:
                assert False
                query = "SELECT _id, cartId, status FROM storeOrder "
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
                query = query + date_range_query + store_categories_query + store_query
                query = query.strip()
                store_order_log = sqlContext.sql(query)
                store_order_log = store_order_log.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                store_order_log = pd.DataFrame(db.storeOrder.find(query, {"_id": 1, "cartId": 1, "status": 1}))
                if not store_order_log.shape[0]:
                    response = {"message": "No data found"}
                    return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

            store_order_log["status"] = store_order_log["status"].apply(lambda x: x["status"])
            store_order_log = store_order_log.rename(columns={"_id": "OrderId"})
            new_merge = pd.merge(store_order_log, merge_log, on="cartId", how="outer")
            web = new_merge[new_merge.device == "web"]
            # funnel data frame integration
            funnel = []
            for make in web.make.unique():
                make_df = web[web["make"] == make]
                funnel.append({
                    "make": make,
                    "session": make_df._id.count(),
                    "cartId": make_df.cartId.count(),
                    "OrderId": make_df.OrderId.count(),
                    "payment": make_df[make_df.status == 7].status.count(),
                })
            if not len(funnel):
                response = {"message": "No data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            funnel_df = pd.DataFrame(funnel)
            # sneaky graph conversion
            sneaky = []
            for index in range(funnel_df.shape[0]):
                sneaky.append(
                    {"from": str(funnel_df.make.loc[index]), "to": "Cart", "weight": int(funnel_df.cartId.loc[index])})

            sneaky.append({"from": "Cart", "to": "Order", "weight": int(funnel_df.OrderId.sum())})
            sneaky.append({"from": "Order", "to": "Payment", "weight": int(funnel_df.payment.sum())})
            sneaky.append({"from": "Cart", "to": None, "weight": int(funnel_df.cartId.sum() - funnel_df.OrderId.sum())})
            sneaky.append({"from": "Order", "to": None, "weight": int(funnel_df.OrderId.sum() - funnel_df.payment.sum())})

            funnel_df["step1"] = funnel_df[["session", "cartId"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df["step2"] = funnel_df[["session", "OrderId"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df["step3"] = funnel_df[["session", "payment"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)

            funnel_df = funnel_df.to_dict(orient="records")
            response = {"message": "success", "data": funnel_df, "graph": sneaky}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
"""


class Device(APIView):
    def get(self, request):
        """
        HTTP_AUTHORIZATION
        store_categories_id
        :param request:
        :return:
        """
        # TODO: Store Id integration pending in cart collection
        try:
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            # start_timestamp and end_timestamp in epoch(seconds)
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
            except:
                response = {"message": "Incorrect/Missing 'start_timestamp' and 'end_timestamp'"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = store_categories_id if store_categories_id != "0" else ""
            store_categories_query = ""
            if store_categories_id != "0" and store_categories_id:
                store_categories_query = " WHERE storeCategoryId == '{}'".format(store_categories_id)

            store_id = str(request.GET.get("store_id", ""))
            store_id = store_id if store_id != "0" else ""
            store_query = ""
            if store_id != "0" and store_id:
                store_query = "AND storeId == '{}' ".format(store_id)

            query = "SELECT _id, device, make FROM sessionLogs WHERE sessionStart BETWEEN {} AND {}".format(
                start_timestamp, end_timestamp)
            query = query.strip()
            try:
                assert False
                session_logs = sqlContext.sql(query)
                session_logs = session_logs.toPandas()
                if session_logs.shape[0]:
                    session_logs["_id"] = session_logs["_id"].apply(lambda x: x.oid)
            except:
                query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp}}
                print("sessionLogs ----->", query)
                session_logs = pd.DataFrame(db.sessionLogs.find(query, {"_id": 1, "device": 1, "make": 1}))
                if session_logs.shape[0]:
                    session_logs["_id"] = session_logs["_id"].astype(str)
            if not session_logs.shape[0]:
                response = {"message": "No data Found, No Session Log Data"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            try:
                assert False
                query = "SELECT _id, sessionId, sellers.fullFillMentCenterId fullFillMentCenterId FROM cart "
                date_range_query = "WHERE cartLogs.timestamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
                query = query + date_range_query + store_categories_query
                query = query.strip()
                cart_logs = sqlContext.sql(query)
                cart_logs = cart_logs.toPandas()
                if cart_logs.shape[0]:
                    cart_logs["_id"] = cart_logs["_id"].apply(lambda x: x.oid)
            except:
                query = {"cartLogs.timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                cart_logs = pd.DataFrame(
                    db.cart.find(query, {"_id": 1, "sessionId": 1, "sellers.fullFillMentCenterId": 1}))
                if cart_logs.shape[0]:
                    cart_logs["fullFillMentCenterId"] = cart_logs["sellers"].apply(
                        lambda x: x.get("fullFillMentCenterId", "") if isinstance(x, dict) else x)
                    cart_logs = cart_logs.drop("sellers", axis=1, errors="ignore")
                    cart_logs["_id"] = cart_logs["_id"].apply(lambda x: str(x))

            cart_logs["type"] = cart_logs["sessionId"].apply(lambda x: type(x))
            # cart_logs = cart_logs[cart_logs["type"].astype(str) != "<class 'dict'>"].drop("type", axis=1)
            # cart_logs = cart_logs[cart_logs["sessionId"].astype(str) != ""]
            cart_logs = cart_logs.rename(columns={"_id": "cartId"})

            # ------------------------ session filter as per store in cart ------------------------------------
            if store_id:
                cart_logs["check"] = cart_logs["fullFillMentCenterId"].apply(lambda x: 1 if store_id in x else 0)
                cart_logs = cart_logs[cart_logs["check"] == 1]
            cart_logs = cart_logs.drop(["fullFillMentCenterId", "check"], axis=1, errors="ignore")
            # ------------------------- session filter as per store in sessionLogs ----------------------------
            cart_session_ids = list(cart_logs["sessionId"])
            session_logs = session_logs[session_logs["_id"].isin(cart_session_ids)]
            # -------------------------------------------------------------------------------------------------

            merge_log = pd.merge(cart_logs, session_logs, left_on="sessionId", right_on="_id", how="outer")
            # merge_log["cartId"] = merge_log["cartId"].apply(lambda x: x.oid if not isinstance(x, float) else x)
            merge_log = merge_log.drop("sessionId", axis=1)
            try:
                assert False
                query = "SELECT _id, cartId, status FROM storeOrder "
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
                query = query + date_range_query + store_categories_query + store_query
                query = query.strip()
                store_order_log = sqlContext.sql(query)
                store_order_log = store_order_log.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id != "0" and store_categories_id:
                    query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id:
                    query["storeId"] = store_id
                store_order_log = pd.DataFrame(db.storeOrder.find(query, {"_id": 1, "cartId": 1, "status": 1}))

            store_order_log["status"] = store_order_log["status"].apply(lambda x: x["status"])
            store_order_log = store_order_log.rename(columns={"_id": "OrderId"})
            new_merge = pd.merge(store_order_log, merge_log, on="cartId", how="outer")
            new_merge = new_merge.dropna(subset=["device"], axis=0)
            funnel = []
            for device in new_merge.device.unique():
                device_df = new_merge[new_merge["device"] == device]
                funnel.append({
                    "device": device,
                    "session": device_df._id.count(),
                    "cart": device_df.cartId.count(),
                    "order": device_df.OrderId.count(),
                    "payment": device_df[device_df.status == 7].status.count(),
                })
            if not len(funnel):
                response = {"message": "No data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            funnel_df = pd.DataFrame(funnel)

            sneaky = []
            for index in range(funnel_df.shape[0]):
                sneaky.append(
                    {"from": str(funnel_df.device.loc[index]), "to": "Cart", "weight": int(funnel_df.cart.loc[index])})

            sneaky.append({"from": "Cart", "to": "Order", "weight": int(funnel_df.order.sum())})
            sneaky.append({"from": "Order", "to": "Payment", "weight": int(funnel_df.payment.sum())})
            sneaky.append({"from": "Cart", "to": None, "weight": int(funnel_df.cart.sum() - funnel_df.order.sum())})
            sneaky.append({"from": "Order", "to": None, "weight": int(funnel_df.order.sum() - funnel_df.payment.sum())})
            funnel_df["step1"] = funnel_df[["session", "cart"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df["step2"] = funnel_df[["session", "order"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df["step3"] = funnel_df[["session", "payment"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df = funnel_df.to_dict(orient="records")
            response = {"message": "success", "data": funnel_df, "graph": sneaky}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)



class FunnelPlatform(APIView):
    def get(self, request):
        """
        HTTP_AUTHORIZATION
        store_categories_id
        start_timestamp
        end_timestamp
        :param request:
        :return:
        """
        print("Inside funnel platform --------->>> ")
        try:
            # authorization from header check
            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            # store category parameter and query add on for spark sql
            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = store_categories_id if store_categories_id != "0" else ""
            store_categories_query = ""
            if store_categories_id:
                store_categories_query = "AND storeCategoryId == '{}' ".format(store_categories_id)

            store_id = str(request.GET.get("store_id", ""))
            store_id = store_id if store_id != "0" else ""
            store_query = ""
            if store_id != "0" and store_id:
                store_query = "AND storeId == '{}' ".format(store_id)

            # start_timestamp and end_timestamp in epoch(seconds)
            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
            except:
                response = {"message": "Incorrect/Missing 'start_timestamp' and 'end_timestamp'"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # Spark SQL query for session logs
            query = "SELECT _id, device, make FROM sessionLogs WHERE sessionStart BETWEEN {} AND {}".format(
                start_timestamp, end_timestamp)
            query = query.strip()
            try:
                assert False
                session_logs = sqlContext.sql(query)
                session_logs = session_logs.toPandas()
                if session_logs.shape[0]:
                    session_logs["_id"] = session_logs["_id"].apply(lambda x: x.oid)
            except:
                query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp}}
                print("session_logs -->", query)
                session_logs = pd.DataFrame(db.sessionLogs.find(query, {"_id": 1, "device": 1, "make": 1}))
                if session_logs.shape[0]:
                    session_logs["_id"] = session_logs["_id"].astype(str)
            if not session_logs.shape[0]:
                response = {"message": "No data Found, No Session Data"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)

            # Spark SQL query for cart
            query = "SELECT _id, sessionId, sellers.fullFillMentCenterId fullFillMentCenterId FROM cart "
            date_range_query = "WHERE cartLogs.timestamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
            query = query + date_range_query + store_categories_query
            try:
                assert False
                query = query.strip()
                cart_logs = sqlContext.sql(query)
                cart_logs = cart_logs.toPandas()
                if cart_logs.shape[0]:
                    cart_logs["_id"] = cart_logs["_id"].apply(lambda x: x.oid)
            except:
                query = {"cartLogs.timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                cart_logs = pd.DataFrame(
                    db.cart.find(query, {"_id": 1, "sessionId": 1, "sellers.fullFillMentCenterId": 1}))
                if cart_logs.shape[0]:
                    cart_logs["fullFillMentCenterId"] = cart_logs["sellers"].apply(
                        lambda x: x.get("fullFillMentCenterId", "") if isinstance(x, dict) else x)
                    cart_logs = cart_logs.drop("sellers", axis=1, errors="ignore")
                    cart_logs["_id"] = cart_logs["_id"].astype(str)

            # if no cart data available bad request
            if not cart_logs.shape[0]:
                response = {"message": "No data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # cart_logs["type"] = cart_logs["sessionId"].apply(lambda x: type(x))  # type casting
            # cart_logs = cart_logs[cart_logs["type"].astype(str) != "<class 'dict'>"].drop("type", axis=1)
            cart_logs = cart_logs[cart_logs["sessionId"].astype(str) != ""]
            print("Cart_logs---->", cart_logs)
            print("store_id---->", store_id)

            # ------------------------ session filter as per store in cart ------------------------------------
            if store_id:
                cart_logs["check"] = cart_logs["fullFillMentCenterId"].apply(lambda x: 1 if store_id in x else 0)
                cart_logs = cart_logs[cart_logs["check"] == 1]
            cart_logs = cart_logs.drop(["fullFillMentCenterId", "check"], axis=1, errors="ignore")
            # ------------------------- session filter as per store in sessionLogs ----------------------------
            cart_session_ids = list(cart_logs["sessionId"])
            # print(cart_session_ids)

            session_logs = session_logs[session_logs["_id"].isin(cart_session_ids)]
            print("session_logs--->",session_logs)
            # -------------------------------------------------------------------------------------------------
            
            cart_logs = cart_logs.rename(columns={"_id": "cartId"})
            print(cart_logs)
            merge_log = pd.merge(cart_logs, session_logs, left_on="sessionId", right_on="_id", how="inner")
            print("merge_log----->",merge_log)
            # try:
            #     merge_log["cartId"] = merge_log["cartId"].apply(lambda x: x.oid if not isinstance(x, float) else x)
            # except:
            #     merge_log["cartId"] = merge_log["cartId"].apply(lambda x: str(x) if not isinstance(x, float) else x)

            merge_log = merge_log.drop("sessionId", axis=1)

            # Spark SQL query for storeOrder
            try:
                assert False
                query = "SELECT _id, cartId, status FROM storeOrder "
                date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
                query = query + date_range_query + store_categories_query + store_query
                query = query.strip()
                store_order_log = sqlContext.sql(query)
                store_order_log = store_order_log.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id != "0" and store_id: query["storeId"] = store_id
                print("query --->>> ", query)
                store_order_log = pd.DataFrame(db.storeOrder.find(query, {"_id": 1, "cartId": 1, "status": 1}))
                print("store_order_log ---->>> ", store_order_log)
                if not store_order_log.shape[0]:
                    response = {"message": "No data found"}
                    return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)

            store_order_log["status"] = store_order_log["status"].apply(lambda x: x["status"])
            store_order_log = store_order_log.rename(columns={"_id": "OrderId"})
            new_merge = pd.merge(store_order_log, merge_log, on="cartId", how="outer")
            print("Column names in new_merge:", new_merge.columns)
            print("new_merge device----->>> ", list(new_merge["device"]))
            print("new_merge make ----->>> ", list(new_merge["make"]))
            # Before filtering
            print("Before filtering - Column names in new_merge:", new_merge.columns)
            # Filter out rows with null make values
            new_merge_filtered = new_merge.dropna(subset=['make'])

            # Get unique make values
            unique_make_values = new_merge_filtered['make'].unique()
            # web = new_merge[new_merge.device == "web"]
            # print("After filtering - Column names in web:", web.columns)
            # print("web ------>>> ", web.make.unique())
            # funnel data frame integration
            funnel = []
            for make in unique_make_values:
                make_df = new_merge_filtered[new_merge_filtered['make'] == make]
                funnel.append({
                    "make": make,
                    "session": make_df._id.count(),
                    "cartId": make_df.cartId.count(),
                    "OrderId": make_df.OrderId.count(),
                    "payment": make_df[make_df.status == 7].status.count(),
                })
            if not len(funnel):
                response = {"message": "No data Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            funnel_df = pd.DataFrame(funnel)
            # sneaky graph conversion
            sneaky = []
            for index in range(funnel_df.shape[0]):
                sneaky.append(
                    {"from": str(funnel_df.make.loc[index]), "to": "Cart", "weight": int(funnel_df.cartId.loc[index])})

            sneaky.append({"from": "Cart", "to": "Order", "weight": int(funnel_df.OrderId.sum())})
            sneaky.append({"from": "Order", "to": "Payment", "weight": int(funnel_df.payment.sum())})
            sneaky.append({"from": "Cart", "to": None, "weight": int(funnel_df.cartId.sum() - funnel_df.OrderId.sum())})
            sneaky.append({"from": "Order", "to": None, "weight": int(funnel_df.OrderId.sum() - funnel_df.payment.sum())})

            funnel_df["step1"] = funnel_df[["session", "cartId"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df["step2"] = funnel_df[["session", "OrderId"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)
            funnel_df["step3"] = funnel_df[["session", "payment"]].apply(
                lambda x: {"value": int(x[1]), "percent": float((x[1] / x[0]) * 100) if x[0] else 0}, axis=1)

            funnel_df = funnel_df.to_dict(orient="records")
            response = {"message": "success", "data": funnel_df, "graph": sneaky}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)

