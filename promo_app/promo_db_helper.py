from rest_framework import status
from django.http import JsonResponse
from analytics.settings import  db
import pandas as pd


class DbHelper:

    def promo_data(self, start_timestamp, end_timestamp, service_type=0):
        # query = "SELECT T1.promo_id, T1.promo_name, T1.promo_code, " \
        #         "T1.accounting, T1.user_id, T1.promo_applied_on, " \
        #         "T2.promo_activation_status " \
        #         "FROM promo_consumption_history T1 LEFT JOIN promo_details T2 ON T1.promo_id = T2._id " \
        #         "WHERE T1.time_stamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
        try:
            assert False
            query = "SELECT promo_id, promo_name, promo_code, " \
                    "accounting, user_id, promo_applied_on " \
                    "FROM promo_consumption_history " \
                    "WHERE time_stamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
            if service_type:
                query = query + "AND service_type == {}".format(service_type)
            result_data = sqlContext.sql(query)
            return result_data.toPandas()
        except:
            query = {"time_stamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
            if service_type:
                query["service_type"] = service_type
            projection = {"promo_id": 1, "promo_name": 1, "promo_code": 1,
                          "accounting": 1, "user_id": 1, "promo_applied_on": 1}
            print("query--->", query)
            result_data = pd.DataFrame(db.promo_consumption_history.find(query, projection))
            return result_data

    def promo_active_data(self, promo_ids: tuple):
        # query = "SELECT _id, promo_activation_status FROM promo_details WHERE _id IN {}".format(promo_ids)
        # result_data = sqlContext.sql(query)
        query = {"_id": {"$in": promo_ids}}
        result_data = db.promo_details.find(query, {"promo_activation_status": 1})
        return result_data

    def percent_count(self, store_id: str, store_categories_id: str, service_type=0):
        """
        Percent of order with coupon (all time)
                :param store_id: add on query with respect to store id (string)
                :param store_categories_id:  add on query with respect to store categories id (string)
                :param service_type:
        """
        try:
            assert False
            query = "SELECT (COUNT(*)* 100 / (SELECT COUNT(*) FROM storeOrder)) as count FROM storeOrder " \
                    "WHERE promocodeData.isPromoCodeApplied == True"
            query = query + store_id + store_categories_id
            if service_type:   query = query + "AND service_type == {}".format(service_type)
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            try:
                count = result_data.loc[0, "count"]
            except:
                count = 0
            return count
        except:
            query = {"promocodeData.isPromoCodeApplied": True}
            if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
            if store_id != "0" and store_id: query["storeId"] = store_id
            promo_applied = db.storeOrder.find(query).count()
            all_count = db.storeOrder.find(query).count()
            count = 0
            if all_count:
                count = (promo_applied * 100) / all_count
            return count

    def percent_revenue_count(self, store_id: str, store_categories_id: str, service_type=0):
        """
        Percent net revenue from coupons (all time)
                :param store_id: add on query with respect to store id (string)
                :param store_categories_id:  add on query with respect to store categories id (string)
                :param service_type:
        """
        try:
            assert False
            query = "SELECT (SUM(accounting.finalUnitPrice)* 100 / (SELECT SUM(accounting.finalUnitPrice) FROM storeOrder)) as revenue FROM storeOrder " \
                    "WHERE promocodeData.isPromoCodeApplied == True"
            query = query + store_id + store_categories_id
            if service_type:   query = query + "AND service_type == {}".format(service_type)
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            try:
                revenue = result_data.loc[0, "revenue"]
            except:
                revenue = 0
            return revenue
        except:
            match = {"promocodeData.isPromoCodeApplied": True}
            if store_categories_id != "0" and store_categories_id: match["storeCategoryId"] = store_categories_id
            if store_id != "0" and store_id: match["storeId"] = store_id
            price_promo = list(db.storeOrder.aggregate([
                {"$match": match},
                {"$group": {"_id": None, "sum": {"$sum": "$accounting.finalUnitPrice"}}}

            ]))
            total_promo = list(db.storeOrder.aggregate([
                {"$group": {"_id": None, "sum": {"$sum": "$accounting.finalUnitPrice"}}}
            ]))
            if len(price_promo) != 0 and len(total_promo) != 0:
                price_promo = price_promo[0].get("sum", 0)
                total_promo = total_promo[0].get("sum", 0)
                return (price_promo * 100) / total_promo
            return 0

    def promo_count(self, store_id: str, store_categories_id: str, service_type=0):
        """
        Orders with coupons (all time)
                :param store_id: add on query with respect to store id (string)
                :param store_categories_id:  add on query with respect to store categories id (string)
                :param service_type:
        """
        try:
            assert False
            query = "SELECT Count(*) as count From storeOrder " \
                    "WHERE promocodeData.isPromoCodeApplied == True"
            query = query + store_id + store_categories_id
            if service_type:   query = query + "AND service_type == {}".format(service_type)
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            try:
                count = result_data.loc[0, "count"]
            except:
                count = 0
            return count
        except:
            query = {"promocodeData.isPromoCodeApplied": True}
            if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
            if store_id != "0" and store_id: query["storeId"] = store_id
            promo_applied = db.storeOrder.find(query).count()
            return promo_applied

    def promo_revenue_count(self, store_id: str, store_categories_id: str, service_type=0):
        """
        Net revenue from coupon orders (all time)
                :param store_id: add on query with respect to store id (string)
                :param store_categories_id:  add on query with respect to store categories id (string)
                :param service_type:
        """
        try:
            assert False
            query = "SELECT SUM(accounting.finalUnitPrice) as revenue From storeOrder " \
                    "WHERE promocodeData.isPromoCodeApplied == True"
            query = query + store_id + store_categories_id
            if service_type:   query = query + "AND service_type == {}".format(service_type)
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            try:
                revenue = result_data.loc[0, "revenue"]
            except:
                revenue = 0
            return revenue
        except:
            match = {"promocodeData.isPromoCodeApplied": True}
            if store_categories_id != "0" and store_categories_id: match["storeCategoryId"] = store_categories_id
            if store_id != "0" and store_id: match["storeId"] = store_id
            price_promo = list(db.storeOrder.aggregate([
                {"$match": match},
                {"$group": {"_id": None, "sum": {"$sum": "$accounting.finalUnitPrice"}}}
            ]))
            if len(price_promo)!=0:
                return price_promo[0].get("sum", 0)
            return 0


    def promo_analytics(self, store_id: str, store_categories_id: str, start_timestamp: int, end_timestamp: int,
                        service_type=0):
        """
        Order with and with out coupons in respective time delta (all time)
                :param store_id: add on query with respect to store id (string)
                :param store_categories_id:  add on query with respect to store categories id (string)
                :param start_timestamp:  epoch time stamp in GMT(integer)
                :param end_timestamp:  epoch time stamp in GMT(integer)
                :param service_type:
        """
        try:
            assert False
            query = "SELECT createdTimeStamp, promocodeData.isPromoCodeApplied promoStatus From storeOrder " \
                    "WHERE createdTimeStamp BETWEEN {} AND {} ".format(start_timestamp, end_timestamp)
            query = query + store_id + store_categories_id
            if service_type:   query = query + "AND service_type == {}".format(service_type)
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            print("########################### DATA ########################################")
            print(result_data)
            return result_data
        except:
            query = {"time_stamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
            if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
            if store_id != "0" and store_id: query["storeId"] = store_id
            if service_type:
                query["service_type"] = service_type

            projection = {"createdTimeStamp": 1, "promocodeData.isPromoCodeApplied": 1}

            print("query---------->", query)
            result_data = pd.DataFrame(db.storeOrder.find(query, projection))
            return result_data
