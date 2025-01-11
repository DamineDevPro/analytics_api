from analytics.settings import db


class DbHelper:

    def driver_roaster_data(self, start_timestamp, end_timestamp):
        query = {"startDateTime": {"$gte": start_timestamp}, "endDateTime": {"$lte": end_timestamp}, "status": 1}
        driver_roaster = db['driverRoasterDaily'].find(query)
        return driver_roaster

    def order_data(self, driver_roaster_id, store_id):
        query = [
            {"$match": {
                "pickupSlotId": {"$in": driver_roaster_id},
                # "storeId": store_id
            }},
            {"$unwind": "$products"},
            {"$match": {
                "products.storeId": store_id,
                "$expr": {"$in": ["$products.productId", "$pickupSlotDetails.productIds"]}
                # "products.productId": {"$in": "pickupSlotDetails.productIds"}
            }},
            {"$project":
                {
                    "_id": {"$toString": "$_id"},
                    "dcId": {"$toString": "$DCDetails.id"},
                    "dcName": "$DCDetails.name",
                    "productOrderId": {"$toString": "$products.productOrderId"},
                    "productId": "$products.productId",
                    "centralProductId": {"$toString": "$products.centralProductId"},
                    "unitId": {"$toString": "$products.unitId"},
                    "name": "$products.name",
                    "quantity": "$products.quantity.value",
                    "unit": "$products.quantity.unit",
                    "pickupSlotId": {"$toString": "$pickupSlotId"},
                    "shiftName": "$pickupSlotDetails.shiftName",
                    "date": "$pickupSlotDetails.date",
                    "startTime": "$pickupSlotDetails.startTime",
                    "endTime": "$pickupSlotDetails.endTime",
                    "startDateTime": "$pickupSlotDetails.startDateTime",
                    "endDateTime": "$pickupSlotDetails.endDateTime",
                    "sku": "$products.sku"
                }
            }
        ]
        print("Order Query-------------------->", query)
        order_data = list(db['storeOrder'].aggregate(query))
        return order_data

    def child_product(self, child_product_list):
        query = [
            {"$match": {
                "_id": {"$in": child_product_list}
            }},
            {"$unwind": "$units"},
            {"$project":
                {
                    "productId": {"$toString": "$_id"},
                    "avgWeight": "$units.avgWeight",
                    # "avgWeightunit": "$units.avgWeightunit",
                    "avgweightunitName": "$units.avgweightunitName",
                    # "attrname": "$units.attributes.attrlist.attrname",
                    "attrlist": "$units.attributes.attrlist",
                    # "value": "$units.attributes.attrlist.value",
                    # "measurementUnitName": "$units.attributes.attrlist.measurementUnitName"
                }
            }
        ]
        child_product = db['childProducts'].aggregate(query)
        return child_product

    def product_data(self, driver_roaster_id, product_id, dc_id, shift_id, store_id=""):
        match_query_addon = {
            "pickupSlotId": {"$in": driver_roaster_id},
        }
        if dc_id:
            match_query_addon["DCDetails.id"] = dc_id
        if shift_id:
            match_query_addon["pickupSlotId"] = {"$in": shift_id}
        # if store_id:
        #     match_query_addon["storeId"] = store_id
        match_query_addon = {"$match": match_query_addon}
        query = [match_query_addon, {"$unwind": "$products"}]
        sub_match_query = {"products.productId": {"$eq": product_id}}
        if store_id: sub_match_query["products.storeId"] = store_id
        query.append({"$match": sub_match_query})
        if store_id:
            query.extend([
                # {"$match": {"products.productId": {"$eq": product_id}}},
                {"$project":
                    {
                        "_id": {"$toString": "$_id"},
                        "dcId": {"$toString": "$DCDetails.id"},
                        "dcName": "$DCDetails.name",
                        # "productOrderId": {"$toString": "$products.productOrderId"},
                        "productId": "$products.productId",
                        # "centralProductId": {"$toString": "$products.centralProductId"},
                        # "unitId": {"$toString": "$products.unitId"},
                        "name": "$products.name",
                        "quantity": "$products.quantity.value",
                        "unit": "$products.quantity.unit",
                        "pickupSlotId": {"$toString": "$pickupSlotId"},
                        "shiftName": "$pickupSlotDetails.shiftName",
                        "date": "$pickupSlotDetails.date",
                        "startTime": "$pickupSlotDetails.startTime",
                        "endTime": "$pickupSlotDetails.endTime",
                        "startDateTime": "$pickupSlotDetails.startDateTime",
                    }
                }])
        order_data = list(db['storeOrder'].aggregate(query))
        return order_data
