from rest_framework import status
from django.http import JsonResponse
from analytics.settings import db
import pandas as pd

class DbHelper:
    def grocery_store_order(self, store_id, skip, limit, count=False):
        query = {}
        if store_id:
            query["storeId"] = store_id
        projection = {"_id": 0, "masterOrderId": 1, "parentOrderId": 1, "storeOrderId": 1, "cartId": 1, "sessionId": 1,
                      "storeCategoryId": 1, "storeCategory": 1, "sellerType": 1, "sellerTypeMsg": 1, "storeType": 1,
                      "storeTypeMsg": 1, "orderType": 1, "orderTypeMsg": 1, "customerPaymentType": 1,
                      "customerPaymentTypeMsg": 1, "deliveryFeePaidBy": 1, "deliveryFeePaidByText": 1,
                      "freeDeliveryLimitPerStore": 1, "driverTypeId": 1, "driverType": 1, "shopPickerAndPackerBy": 1,
                      "shopPickerAndPackerByText": 1, "autoDispatch": 1, "autoAcceptOrders": 1, "pickupAddress": 1,
                      "storeId": 1, "storeName": 1, "storeLogo": 1, "storePhone": 1, "storeTaxId": 1, "storeEmail": 1,
                      "paymentType": 1, "paymentTypeText": 1, "payByWallet": 1, "createdTimeStamp": 1, "createdDate": 1,
                      "bookingType": 1, "bookingTypeText": 1, "requestedForPickup": 1, "requestedForPickupTimeStamp": 1,
                      "requestedFor": 1, "requestedForTimeStamp": 1, "pickupSlotId": 1, "pickupSlotDetails": 1,
                      "deliverySlotId": 1, "deliverySlotDetails": 1, "deliveryAddress": 1, "status": 1, "timestamps": 1,
                      "pickerDetails": 1, "customerId": 1, "customerDetails": 1}
        if count:
            data = db.storeOrder.find(query, projection).sort("createdTimeStamp", -1)
        else:
            data = db.storeOrder.find(query, projection).sort("createdTimeStamp", -1).skip(skip).limit(limit)
        return data

    def grocery_driver(store_order_id):
        query = {"storeOrderId": {"$in": store_order_id}}
        projection = {"_id": 0, "storeOrderId": 1, "timestamps": 1, "customerId": 1, "customerDetails": 1,
                      "storeType": 1, "storeTypeMsg": 1, "storeId": 1, "storeName": 1, "storeLogo": 1,
                      "requestedFor": 1, "requestedForTimeStamp": 1, "slotId": 1, "slotDetails": 1,
                      "shippingLabel": 1,
                      "bags": 1, "vehicleDetails": 1, "inDispatch": 1, "driverId": 1, "customerSignature": 1,
                      "isRating": 1}
        return db.driverJobs.find(query, projection)

    def dc_demand(start_time, end_time, storeId, fcStoreId):
        query = {"status": 3, "toStoreId": storeId,\
            "demandGeneratedOnTimestamp": {"$gte": start_time, "$lte": end_time}}
        
        if fcStoreId:
            query['fromStoreId'] = fcStoreId
        projection = {"_id": 0, "productName": 1, "productSKU": 1, "productAttributes": 1, "quantity": 1,
                    "fromStoreName": 1, "demandGeneratedOnTimestamp": 1, "colorName": 1, "unitSizeGroupValue": 1}
        print("query----->", query)
        return pd.DataFrame(db.productOrderDemand.find(query,projection))

    def get_dc_list():
        return db.stores.distinct("storeId",{"storeFrontTypeId": 5})

    def fc_stores():
        query = {"storeFrontTypeId": 4}
        projection = {"storeName": 1}
        print("query---->", query)
        return pd.DataFrame(db['stores'].find(query, projection).sort([("_id",-1)]))
        