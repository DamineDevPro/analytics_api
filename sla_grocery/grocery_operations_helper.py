import pandas as pd
from .grocery_response_helper import GroceryResponses
from .grocery_db_helper import DbHelper
from datetime import datetime
from django.http import JsonResponse
from rest_framework import status
from analytics.settings import UTC
import pytz

date_format = '%m-%d-%Y'
date_format_1 = '%d %b, %Y %I:%M %p'

class GroceryOperations:

    def header_check(self, meta_data):
        self.meta_data = meta_data
        # authorization from header check
        token = self.meta_data.get('HTTP_AUTHORIZATION')
        response = 201 if token else 401
        return response

    def parameter_check(requested_data):
        # store category parameter and query add on for spark sql
        # Store Id param check - mandatory field
        try:
            store_id = str(requested_data['store_id'])
        except:
            response = {"message": "mandatory field 'store_id' missing"}
            return {"status": 400, "response": response}

        store_categories_id = str(requested_data.get("store_categories_id", ""))
        response = {
            "store_id": store_id,
            "store_categories_id": store_categories_id
        }
        return {"status": 201, "response": response}

    def get_grocery(self, data, count=0):
        self.data = data
        try:
            data = pd.DataFrame(self.data)
            data = pd.concat([data, data["timestamps"].apply(pd.Series)], axis=1)
            data['orderAccepted'] = data[["created", "accepted"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data["pickerAssigned"] = data[["accepted", "picking"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data["pickingCompleted"] = data[["picking", "picked"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data["checkoutNPickingCompleted"] = data[["picked", "packed"]].apply(lambda x: x[1] - x[0] if x[1] else x[1],
                                                                                 axis=1)
            data = data[['masterOrderId', 'parentOrderId', 'storeOrderId', 'orderAccepted', 'pickerAssigned',
                         'pickingCompleted', 'checkoutNPickingCompleted', "pickupAddress"]]
            store_order_id = list(data["storeOrderId"])

            driver_data = DbHelper.grocery_driver(store_order_id)
            if not driver_data.count():
                return GroceryResponses.get_status_204()

            driver_job = pd.DataFrame(driver_data)
            driver_job = driver_job.rename(columns={"timestamps": "ts"})
            driver_job = pd.concat([driver_job, driver_job["ts"].apply(pd.Series)], axis=1)
            driver_job = driver_job.drop("ts", axis=1, errors="ignore")
            data = driver_job.merge(data, how="left", on="storeOrderId")
            data = data.fillna("")
            data["DriverAssigned"] = data[["new", "assigned"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data["OrderPickedUp"] = data[["assigned", "picked"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data["AtDropLocation"] = data[["picked", "atDrop"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data["DeliveryComplete"] = data[["atDrop", "completed"]].apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            data = data.fillna("")
            data = data.to_dict(orient="records")
            return GroceryResponses.get_status_200(data, pen_count=int(count))
        except Exception as ex:
            GroceryResponses.get_status_500(ex)

    @staticmethod
    def var_specs(row):
        specs = []
        if row['colorName']:
            specs.append(f"Color : {row['colorName']}")
        if len(row['unitSizeGroupValue']) and 'en' in row['unitSizeGroupValue'].keys():
            specs.append(f"Size : {row['unitSizeGroupValue']['en']}")

        for obj in row['productAttributes']:
            for subObj in obj["attrlist"]:
                if subObj['linkedtounit']:
                    specs.append(f"Quantity : {subObj['value']['en']}")

        return ', '.join(specs)

    def get_demand(self, request):

        # ---------------------- time stamp ----------------------
        try:
            start_time = int(request.GET["start_time"])
            end_time = int(request.GET["end_time"])
        except:
            response = {"message": "Missing/Incorrect Time stamp, 'start_time' or 'end_time' must be integer"}
            return GroceryResponses.get_status_400(response)

        try:
            time_zone = pytz.timezone(str(request.GET.get('timezone','Asia/Kolkata')))
        except Exception as e:
            response = {'message': 'key error', 'issue': '"timezone" key is missing'}
            return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

        try:
            skip = int(request.GET.get('skip', 0))
            limit = int(request.GET.get('limit', 10))
        except:
            return GroceryResponses.get_status_400(params=["skip", "limit"])
        
        if 'storeId' not in request.GET:
            return GroceryResponses.get_status_400(["storeId"])

        storeId = request.GET["storeId"]

        fcStoreId = request.GET.get('fcStoreId','')

        dc_list = DbHelper.get_dc_list()
        print(dc_list)

        if storeId not in dc_list:
            return GroceryResponses.get_status_404()

        data = DbHelper.dc_demand(start_time, end_time, storeId, fcStoreId)
        if not data.shape[0]:
            return GroceryResponses.get_status_204()

        export_df = pd.DataFrame()
        export_df['Product Name'] = data['productName']
        export_df['SKU'] = data['productSKU'].astype('str')
        # export_df['Variant Specification'] = data['productAttributes'].apply(lambda x:\
        #     x[0]['attrlist'][0]['value']['en'] if x else '')
        export_df['Variant Specification'] = data[['productAttributes','colorName','unitSizeGroupValue']].apply(GroceryOperations.var_specs, axis=1)
        export_df['Total Demand'] = data['quantity']
        export_df['From Store'] = data['fromStoreName']
        export_df['Order Date'] = data['demandGeneratedOnTimestamp'].astype('int').apply(lambda x: datetime.\
                                        fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).\
                                            strftime(date_format_1))
        export_df.sort_values(by='Order Date', ascending=False, inplace=True)
        export_df.insert(loc=0, column='S. No.', value=(export_df.reset_index().index + 1))
        # export_df['Shift'] = data['productSKU']
        data = export_df.to_dict(orient="records")

        return GroceryResponses.get_status_200(data)

    def all_fc_stores(self, request):

        store_df = DbHelper.fc_stores()
        if store_df.shape[0] == 0:
            return GroceryResponses.get_status_204()
            
        export_df = pd.DataFrame()
        export_df['storeId'] = store_df['_id'].astype('str')
        export_df['storeName'] = store_df['storeName'].apply(lambda x:x['en'] if x else '')
    
        del store_df
        data = export_df.to_dict(orient='records')

        return GroceryResponses.get_status_200(data)
