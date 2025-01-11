import pandas as pd
from .promo_response_helper import Responses
from .promo_db_helper import DbHelper
from analytics.settings import  UTC, db, CURRENCY_API, BASE_CURRENCY
from analytics.function import Process
from datetime import datetime, timedelta, date
from bson import ObjectId

DbHelper = DbHelper()
Responses = Responses()


class Operation:
    def header_check(self, meta_data):
        # authorization from header check
        token = meta_data.get('HTTP_AUTHORIZATION')
        response = 201 if token else 401
        return response

    def promo_process(self, result_data, conversion_rate, skip=0, limit=10, export=0):
        try:
            result_data["promo_id"] = result_data["promo_id"].apply(lambda x: str(x.oid))
        except:
            result_data["promo_id"] = result_data["promo_id"].astype(str)
        promo_ids = tuple([ObjectId(id["promo_id"]) for id in result_data[["promo_id"]].to_dict(orient="records")])
        promo_status = pd.DataFrame(DbHelper.promo_active_data(promo_ids=promo_ids))
        promo_status["_id"] = promo_status["_id"].apply(lambda x: str(x))
        # result_data = result_data.merge(promo_data, how="left", right_on="_id", left_on="promo_id")
        result_data["sellerPrice"] = result_data['accounting'].apply(
            lambda x: float(x.get("sellerPrice", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["unitPrice"] = result_data['accounting'].apply(
            lambda x: float(x.get("unitPrice", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["unitPriceWithTax"] = result_data['accounting'].apply(
            lambda x: float(x.get("unitPriceWithTax", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["offerDiscount"] = result_data['accounting'].apply(
            lambda x: float(x.get("offerDiscount", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["finalUnitPrice"] = result_data['accounting'].apply(
            lambda x: float(x.get("finalUnitPrice", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["promoDiscount"] = result_data['accounting'].apply(
            lambda x: float(x.get("promoDiscount", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["deliveryFee"] = result_data['accounting'].apply(
            lambda x: float(x.get("deliveryFee", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["finalTotal"] = result_data['accounting'].apply(
            lambda x: float(x.get("finalTotal", 0)) * conversion_rate if isinstance(x, dict) else 0)

        # Promo Code id, Name and Code data frame
        result_data["count"] = 1
        promo_col = ["promo_id", "promo_name", "promo_code", "promo_applied_on"]
        print(result_data.head())
        promo_data = result_data[promo_col].drop_duplicates(subset="promo_id")
        promo_data = promo_data.merge(promo_status, how="left", right_on="_id", left_on="promo_id")
        promo_data["applied_on"] = promo_data["promo_applied_on"].apply(lambda x: x["applied_on_status"])
        promo_data = promo_data[~promo_data.promo_activation_status.isna()]
        promo_data["activation_status"] = promo_data["promo_activation_status"].apply(
            lambda x: str(x.get("activation_description")).capitalize())
        promo_data["applied_on"] = promo_data["applied_on"].replace({1: "Fixed Value", 2: "Percentage"})
        promo_data = promo_data.drop(["promo_applied_on", "promo_activation_status"], axis=1, errors="ignore")
        _data = result_data.drop(["accounting", "promo_name", "promo_code", "tax", "user_id"],
                                 axis=1,
                                 errors="ignore")
        promo_group_sum = _data.groupby(['promo_id']).sum().reset_index()
        sum_rename = {}
        for col in list(promo_group_sum.columns):
            if col != "promo_id": sum_rename[col] = "sum_" + col
        promo_group_sum = promo_group_sum.rename(columns=sum_rename)
        promo_group_sum = pd.merge(promo_data, promo_group_sum, on='promo_id', how='left')

        promo_group_avg = _data.drop("count", axis=1, errors="ignore").groupby(['promo_id']).mean().reset_index()
        avg_rename = {}
        for col in list(promo_group_avg.columns):
            if col != "promo_id": avg_rename[col] = "avg_" + col
        promo_group_avg = promo_group_avg.rename(columns=avg_rename)
        promo_group_sum = pd.merge(promo_group_sum, promo_group_avg, on='promo_id', how='left')
        unique_count = result_data[["promo_id", "user_id"]].groupby('promo_id').user_id.nunique().reset_index()
        unique_count = unique_count.rename(columns={"user_id": "unique_count"})
        promo_group_sum = pd.merge(promo_group_sum, unique_count, on='promo_id', how='left')
        promo_group_sum["percent_discount"] = (promo_group_sum["sum_promoDiscount"] / promo_group_sum[
            "sum_finalTotal"]) * 100
        promo_group_sum = promo_group_sum.fillna(0)
        # promo_group_sum["promo_id"] = promo_group_sum["promo_id"].apply(lambda x: str(x[0]))
        if export:
            return Responses.get_status_200(data=promo_group_sum.to_dict(orient="records"))
        promo_group_sum = promo_group_sum.to_dict(orient="records")[skip * limit: skip * limit + limit]
        return Responses.get_status_200(data=promo_group_sum)

    def top_promo_process(self, result_data, conversion_rate, sort_by, ascending, skip=0, limit=10, export=0):
        result_data["promoDiscount"] = result_data['accounting'].apply(
            lambda x: float(x["promoDiscount"]) * conversion_rate if x["promoDiscount"] else 0)
        result_data["finalTotal"] = result_data['accounting'].apply(
            lambda x: float(x["finalTotal"]) * conversion_rate if x["finalTotal"] else 0)
        # Promo Code id, Name and Code data frame
        result_data["count"] = 1
        promo_data = result_data[["promo_id", "promo_name", "promo_code"]].drop_duplicates()
        _data = result_data.drop(["accounting", "promo_name", "promo_code", "tax", "user_id"],
                                 axis=1,
                                 errors="ignore")
        promo_group_sum = _data.groupby(['promo_id']).sum().reset_index()
        promo_group_sum = pd.merge(promo_data, promo_group_sum, on='promo_id', how='left')
        promo_group_sum = promo_group_sum.fillna(0)
        try:
            promo_group_sum["promo_id"] = promo_group_sum["promo_id"].apply(lambda x: str(x[0]))
        except:
            promo_group_sum["promo_id"] = promo_group_sum["promo_id"].astype(str)

        sort = {1: "count", 2: "promoDiscount"}
        promo_group_sum = promo_group_sum.sort_values(by=sort[sort_by], ascending=ascending)
        promo_discount_total = float(promo_group_sum["promoDiscount"].sum())
        final_total = float(promo_group_sum["finalTotal"].sum())
        total_count = int(promo_group_sum["count"].sum())
        total_unique_promo = int(promo_data.shape[0])

        promo_group_sum = promo_group_sum.to_dict(orient="records")
        if not export:
            promo_group_sum = promo_group_sum[skip * limit: skip * limit + limit]
        data = {"table": promo_group_sum,
                "promoDiscountTotal": promo_discount_total,
                "totalCount": total_count,
                "totalUniquePromo": total_unique_promo,
                "finalTotalSum": final_total
                }
        return Responses.get_status_200(data=data)

    def promo_count_analysis(self, store_id, store_categories_id, start_timestamp, end_timestamp, time_zone,
                             group_by, service_type=0):
        """
        Order with and with out coupons in respective time delta (all time)
                :param store_id: add on query with respect to store id (string)
                :param store_categories_id:  add on query with respect to store categories id (string)
                :param start_timestamp:  epoch time stamp in GMT(integer)
                :param end_timestamp:  epoch time stamp in GMT(integer)
                :param time_zone:  time zone (string)
                :param group_by:  group by value (integer)
                :param service_type:
        """
        percent_count = DbHelper.percent_count(store_id=store_id,
                                               store_categories_id=store_categories_id,
                                               service_type=service_type)
        percent_revenue_count = DbHelper.percent_revenue_count(store_id=store_id,
                                                               store_categories_id=store_categories_id,
                                                               service_type=service_type)
        promo_count = DbHelper.promo_count(store_id=store_id,
                                           store_categories_id=store_categories_id,
                                           service_type=service_type)
        promo_revenue_count = DbHelper.promo_revenue_count(store_id=store_id,
                                                           store_categories_id=store_categories_id,
                                                           service_type=service_type)

        result_data = DbHelper.promo_analytics(store_id=store_id,
                                               store_categories_id=store_categories_id,
                                               start_timestamp=start_timestamp,
                                               end_timestamp=end_timestamp,
                                               service_type=service_type)

        if not result_data.shape[0]:
            return Responses.get_status_204()
            
        result_data = pd.concat([result_data.drop("promoStatus", axis=1), pd.get_dummies(result_data.promoStatus)],
                                axis=1)
        for col in [False, True]:
            if col not in list(result_data.columns):
                result_data[col] = 0
        result_data["createdTimeStamp"] = pd.to_datetime(result_data["createdTimeStamp"], unit='s')
        result_data['createdTimeStamp'] = result_data['createdTimeStamp'].apply(
            lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))
        result_data["dateTime"] = result_data.createdTimeStamp.apply(
            lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
        result_data = result_data.drop("createdTimeStamp", axis=1, errors="ignore")
        # date filler (add missing date time in an result_data frame)

        result_data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                          time_zone=time_zone, data_frame=result_data, date_column="dateTime",
                                          group_by=group_by)

        # date converter as per group by
        result_data = Process.date_conversion(group_by=group_by, data_frame=result_data, date_col='dateTime')
        result_data = result_data.groupby(['dateTime']).sum().reset_index()

        # result_data frame sorting
        result_data = result_data.sort_values(by='dateTime', ascending=True)
        if group_by == 3: result_data = Process.month_sort(data_frame=result_data, date_column="dateTime")
        if group_by == 4: result_data = Process.quarter_sort(data_frame=result_data, date_column="dateTime")
        if group_by == 7: result_data = Process.day_sort(data_frame=result_data, date_column="dateTime")

        # result_data = result_data.to_dict(orient="records")
        group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year',
                          6: "Hour of Day", 7: "Day of Week"}
        graph = {
            "series": [
                {"name": "Coupon", "data": list(result_data[True].astype(int))},
                {"name": "No coupon", "data": list(result_data[False].astype(int))},
            ],
            "title": {"text": "Orders with and without coupons"},
            "xaxis": {"categories": list(result_data["dateTime"]), "title": {"text": group_by_value[group_by]}},
            "yaxis": {"title": {"text": 'Order'}}
        }
        data = {"graph": graph,
                "percent_count": int(percent_count),
                "percent_revenue_count": float(percent_revenue_count) if percent_revenue_count else 0,
                "promo_count": int(promo_count),
                "promo_revenue_count": float(promo_revenue_count)}
        return Responses.get_status_200(data=data)
