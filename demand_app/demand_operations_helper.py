import pandas as pd
from bson import ObjectId
from .demand_db_helper import DbHelper
from .demand_response_helper import DemandResponses
from analytics.settings import BUFFER_TIME
from datetime import datetime

DbHelper = DbHelper()
DemandResponses = DemandResponses()


class DemandOperations:
    def driver_roaster_tranform(self, driver_roaster):
        driver_roaster = pd.DataFrame(driver_roaster)
        driver_roaster["_id"] = driver_roaster["_id"].astype(str)
        driver_roaster_id = list(set(driver_roaster["_id"]))
        return driver_roaster_id

    def variant_spec(self, _list):
        variant = ""
        try:
            _list = _list[-1]
            variant_data = list(filter(lambda x: x["attrname"]["en"] == "Net Weight", _list))

            if len(variant_data) > 0:
                variant_data = variant_data[-1]
                variant = " ".join([str(variant_data["value"]["en"]), str(variant_data["measurementUnitName"])])
            return variant
        except: return ""

    def order_transform(self, order_data, skip, limit, shift_id, dc_id, export=0):
        try:
            order_data = pd.DataFrame(order_data)
            if "sku" not in order_data.columns: order_data["sku"] = ""
            child_product_list = [ObjectId(x) for x in list(order_data.productId.unique()) if x not in [0, "0"]]
            print("Child Product Fetch ...")
            child_product = DbHelper.child_product(child_product_list=child_product_list)
            child_product_df = pd.DataFrame(child_product)
            # child_product_df["variant_spec"] = child_product_df[["avgWeight", "avgweightunitName"]].apply(
            #     lambda x: " ".join([str(x[0]), str(x[1])]), axis=1)
            child_product_df["variant_spec"] = child_product_df["attrlist"].apply(self.variant_spec)

            child_product_df = child_product_df.drop(["_id", "avgWeight", "avgweightunitName", "attrlist"],
                                                     axis=1,
                                                     errors="ignore")


            demand_df = order_data[["productId", "pickupSlotId", "dcId", "quantity", "date"]]

            demand_df = demand_df.groupby(["productId", "pickupSlotId", "dcId", "date"]).sum().reset_index()
            product_df = order_data[["productId", "name", "sku", "unit"]].drop_duplicates()
            product_df = product_df.drop_duplicates(subset=['productId'], keep='last')
            dc_df = order_data[["dcId", "dcName"]].drop_duplicates()
            pickup_df = order_data[["pickupSlotId", "shiftName", "date", "startTime", "endTime", "startDateTime",
                                    "endDateTime"]].drop_duplicates()
            if export:
                child_product_df = child_product_df.fillna("")
                pickup_df["shiftName"] = pickup_df[["shiftName", "startTime", "endTime"]].apply(
                    lambda x: "{} ({}-{})".format(
                        x[0], str(x[1].strip()[: x[1].rfind(":")]),
                        str(x[2].strip()[: x[2].rfind(":")])), axis=1)
                pickup = pickup_df[["pickupSlotId", "shiftName", "startDateTime"]]
                if dc_id and shift_id: demand_df = demand_df[
                    (demand_df.dcId == dc_id) & (demand_df.pickupSlotId.isin(shift_id))]
                if dc_id: demand_df = demand_df[demand_df.dcId == dc_id]
                product_demand = demand_df.groupby(["productId", "date", "dcId", "pickupSlotId"]).sum().reset_index()
                product_demand = product_demand.merge(product_df, how="left", on="productId")
                product_demand = product_demand.merge(pickup, how="left", on="pickupSlotId")
                product_demand = product_demand.merge(dc_df, how="left", on="dcId")
                product_demand = product_demand.merge(child_product_df, how="left", on="productId")
                product_demand["startDateTime"] = product_demand["startDateTime"].astype(int)
                product_demand = product_demand.sort_values(by="startDateTime", ascending=False)
                product_demand["startDateTime"] = product_demand["startDateTime"] - int(BUFFER_TIME)
                cols = ["name", "sku", "variant_spec", "date", "startDateTime", "shiftName", "quantity", "unit"]
                date_format = '%d-%m-%Y %H:%M %p'
                product_demand["startDateTime"] = product_demand["startDateTime"].apply(
                    lambda x: datetime.fromtimestamp(x).strftime(date_format))
                pd.to_datetime(product_demand["startDateTime"], infer_datetime_format=False)
                renaming_cols = {"name": "Product Name",
                                 "sku": "SKU",
                                 "variant_spec": "Variant Specification",
                                 "date": "Date",
                                 "startDateTime": "Cut-off time/ Outbound time",
                                 "shiftName": "Shifts",
                                 "unit": "Units",
                                 "quantity": "Total"
                                 }
                product_demand = product_demand[cols].rename(columns=renaming_cols).fillna("")
                response_data = {"table": product_demand.to_dict(orient="records")}
                return DemandResponses.get_status_200(data=response_data)

            product_demand = demand_df[["productId", "quantity"]].groupby(
                ["productId"]).sum().reset_index()
            product_demand = product_demand.rename(columns={"quantity": "total"})
            # ----------------------------------------------------------------------------------------------------
            if dc_id and shift_id:
                product_dc_demand = demand_df.loc[(demand_df.dcId == dc_id) &
                                                  (demand_df.pickupSlotId.isin(shift_id)),
                                                  ["productId", "quantity"]]. \
                    groupby(["productId"]). \
                    sum(). \
                    rename(columns={"quantity": "sub_total"}).reset_index()
            elif dc_id:
                product_dc_demand = demand_df.loc[demand_df.dcId == dc_id, ["productId", "quantity"]]. \
                    groupby(["productId"]). \
                    sum(). \
                    rename(columns={"quantity": "sub_total"}).reset_index()
            else:
                product_dc_demand = demand_df[["productId", "quantity"]].groupby(["productId"]). \
                    sum().rename(columns={"quantity": "sub_total"}).reset_index()

            product_demand = product_demand.merge(product_dc_demand, on=["productId"], how="left")
            product_demand["sub_total"] = product_demand["sub_total"].fillna(0)
            product_demand = product_demand.fillna("")
            # ----------------------------------------------------------------------------------------------------

            demand_cols = demand_df[["dcId", "pickupSlotId"]]

            # --------------------------------------- DC Quantity Count ------------------------------------------
            dc_count = demand_df[["dcId", "quantity"]].groupby(["dcId"]).sum().reset_index()
            demand_cols = demand_cols.merge(dc_count, on=["dcId"], how="left")
            total_dc_count = int(demand_df["quantity"].sum())
            # ----------------------------------------------------------------------------------------------------
            demand_cols = demand_cols.merge(dc_df, how="left", on="dcId")
            pickup_df["Name"] = pickup_df[["shiftName", "startTime", "endTime"]].apply(
                lambda x: "{} ({}-{})".format(
                    x[0], str(x[1].strip()[: x[1].rfind(":")]),
                    str(x[2].strip()[: x[2].rfind(":")])), axis=1)

            pickup = pickup_df[["pickupSlotId", "Name"]]
            demand_cols = demand_cols.merge(pickup, how="left", on="pickupSlotId")
            dc_columns = []
            for dc in demand_cols[['dcId', 'dcName', 'quantity']].drop_duplicates().sort_values(by='quantity',
                                                                                                ascending=False).to_dict(
                orient="records"):
                _dict = {
                    "id": dc["dcId"],
                    "columneName": dc["dcName"],
                    "quantity": dc["quantity"],
                    "shiftdata": demand_cols.loc[
                        demand_cols.dcId == dc["dcId"], ["pickupSlotId", "Name"]].drop_duplicates().rename(
                        columns={"pickupSlotId": "id", "Name": "shift"}).to_dict(orient="records")}
                dc_columns.append(_dict)
            child_product_df = child_product_df.fillna("")
            product_demand = product_demand.merge(product_df, how="left", on="productId")
            product_demand = product_demand.fillna("")
            product_demand = product_demand.merge(child_product_df, how="left", on="productId")
            product_demand = product_demand.fillna("")
            product_demand = product_demand.sort_values(by='total', ascending=False)
            product_demand = product_demand[product_demand["sub_total"] != 0]
            count = int(product_demand.shape[0])
            response_data = {"table": product_demand.to_dict(orient="records")[skip: skip * limit + limit],
                             "DcColumd": dc_columns, "penCount": count, "totalQuantity": total_dc_count}
            return DemandResponses.get_status_200(data=response_data)
        except Exception as ex:
            DemandResponses.get_status_500(ex)

    def product_transform(self, order_data, shift_id, dc_id):
        try:
            order_data = pd.DataFrame(order_data)
            if "sku" not in order_data.columns: order_data["sku"] = ""
            demand_df = order_data[["productId", "pickupSlotId", "dcId", "date", "quantity"]]

            demand_df = demand_df.groupby(["productId", "pickupSlotId", "dcId", "date"]).sum().reset_index()
            product_df = order_data[["productId", "name", "unit"]].drop_duplicates()
            product_df = product_df.drop_duplicates(subset=['productId'], keep='last')
            dc_df = order_data[["dcId", "dcName"]].drop_duplicates()
            pickup_columns = ["pickupSlotId", "shiftName", "date", "startTime", "endTime", "startDateTime"]
            pickup_df = order_data[pickup_columns].drop_duplicates()

            product_demand = demand_df[["productId", "date", "dcId", "pickupSlotId", "quantity"]].groupby(
                ["productId", "date", "dcId", "pickupSlotId"]).sum().reset_index()
            product_demand = product_demand.rename(columns={"quantity": "total"})

            demand_cols = demand_df[["dcId", "pickupSlotId"]]
            demand_cols = demand_cols.merge(dc_df, how="left", on="dcId")
            pickup_df["Name"] = pickup_df[["shiftName", "startTime", "endTime"]].apply(
                lambda x: "{} ({}-{})".format(
                    x[0], str(x[1].strip()[: x[1].rfind(":")]),
                    str(x[2].strip()[: x[2].rfind(":")])), axis=1)

            pickup = pickup_df[["pickupSlotId", "Name", "startDateTime"]]
            demand_cols = demand_cols.merge(pickup, how="left", on="pickupSlotId")
            dc_columns = []
            for dc in demand_cols[['dcId', 'dcName']].drop_duplicates().to_dict(orient="records"):
                _dict = {
                    "id": dc["dcId"],
                    "columneName": dc["dcName"],
                    "shiftdata": demand_cols.loc[
                        demand_cols.dcId == dc["dcId"], ["pickupSlotId", "Name"]].drop_duplicates().rename(
                        columns={"pickupSlotId": "id", "Name": "shift"}).to_dict(orient="records")}
                dc_columns.append(_dict)
            product_demand = product_demand.merge(product_df, how="left", on="productId")
            product_demand = product_demand.merge(pickup, how="left", on="pickupSlotId")
            product_demand = product_demand.merge(dc_df, how="left", on="dcId")
            product_demand["startDateTime"] = product_demand["startDateTime"].astype(int)
            product_demand = product_demand.sort_values(by="startDateTime", ascending=False)
            product_demand["startDateTime"] = product_demand["startDateTime"] - int(BUFFER_TIME)
            count = int(product_demand.shape[0])
            response_data = {"table": product_demand.to_dict(orient="records"),
                             "DcColumd": dc_columns, "penCount": count}
            return DemandResponses.get_status_200(data=response_data)
        except Exception as ex:
            DemandResponses.get_status_500(ex)
