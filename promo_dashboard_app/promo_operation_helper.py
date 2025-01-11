import pandas as pd
from .promo_response_helper import Responses

Responses = Responses()


class Operation:
    def header_check(self, meta_data):
        # authorization from header check
        token = meta_data.get('HTTP_AUTHORIZATION')
        response = 201 if token else 401
        return response

    def promo_process(self, result_data, conversion_rate, skip=0, limit=10):
        try:
            result_data["promo_id"] = result_data["promo_id"].apply(lambda x: x.oid)
        except:
            result_data["promo_id"] = result_data["promo_id"].astype(str)
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
        promo_data = result_data[["promo_id", "promo_name", "promo_code"]].drop_duplicates()
        _data = result_data.drop(["accounting", "promo_name", "promo_code", "tax", "user_id"],
                                 axis=1,
                                 errors="ignore")
        promo_group_sum = _data.groupby(['promo_id']).sum().reset_index()
        sum_rename = {}
        for col in list(promo_group_sum.columns):
            if col != "promo_id": sum_rename[col] = "sum_" + col
        promo_group_sum = promo_group_sum.rename(columns=sum_rename)
        promo_group_sum = pd.merge(promo_data, promo_group_sum, on='promo_id', how='left')

        unique_count = result_data[["promo_id", "user_id"]].groupby('promo_id').user_id.nunique().reset_index()
        unique_count = unique_count.rename(columns={"user_id": "unique_count"})
        promo_group_sum = pd.merge(promo_group_sum, unique_count, on='promo_id', how='left')
        promo_group_sum["percent_discount"] = (promo_group_sum["sum_promoDiscount"] / promo_group_sum[
            "sum_finalTotal"]) * 100
        promo_group_sum = promo_group_sum.fillna(0)
        promo_group_sum["promo_id"] = promo_group_sum["promo_id"].apply(lambda x: str(x[0]))
        promo_group_sum = promo_group_sum.to_dict(orient="records")
        promo_group_sum = promo_group_sum[skip * limit: (skip * limit) + limit]
        return Responses.get_status_200(data=promo_group_sum)

    def top_promo_process(self, result_data, conversion_rate, sort_by, ascending, top):
        try:
            result_data["promo_id"] = result_data["promo_id"].apply(lambda x: x.oid)
        except:
            result_data["promo_id"] = result_data["promo_id"].astype(str)

        result_data["promoDiscount"] = result_data['accounting'].apply(
            lambda x: float(x.get("promoDiscount", 0)) * conversion_rate if isinstance(x, dict) else 0)
        result_data["finalTotal"] = result_data['accounting'].apply(
            lambda x: float(x.get("finalTotal", 0)) * conversion_rate if isinstance(x, dict) else 0)
        # Promo Code id, Name and Code data frame
        result_data["count"] = 1
        promo_data = result_data[["promo_id", "promo_name", "promo_code"]].drop_duplicates()
        _data = result_data.drop(["accounting", "promo_name", "promo_code", "tax", "user_id"],
                                 axis=1,
                                 errors="ignore")
        promo_group_sum = _data.groupby(['promo_id']).sum().reset_index()
        print(promo_group_sum[['finalTotal','promoDiscount']])
        print(promo_data)
        promo_group_sum = pd.merge(promo_data, promo_group_sum, on='promo_id', how='left')
        print(promo_group_sum)
        promo_group_sum = promo_group_sum.fillna(0)
        # promo_group_sum["promo_id"] = promo_group_sum["promo_id"].apply(lambda x: str(x[0]))
        sort = {1: "count", 2: "promoDiscount", 3: "finalTotal"}
        promo_group_sum = promo_group_sum.sort_values(by=sort[sort_by], ascending=ascending)
        promo_group_sum = promo_group_sum.head(top).to_dict(orient="records")
        data = {"table": promo_group_sum}
        return Responses.get_status_200(data=data)
