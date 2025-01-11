from analytics.settings import db
from bson.objectid import ObjectId
import pandas as pd


class DbHelper:

    def utilization(self, start_time, end_time) -> pd.DataFrame:

        query = {"createdTimeStamp": {"$gte": start_time,
                                      "$lte": end_time}, "vehicleDetails.typeId": {"$ne": ""}}
        projection = {"createdTimeStamp": 1, "vehicleDetails._id": 1, "_id": 0, "packageId": 1,
                      "vehicleDetails.typeId": 1, "packageBox": 1, "parentOrderId": 1}

        return pd.DataFrame(db.deliveryOrder.find(query, projection))

    def vehicle_info(self, vehicle_type) -> pd.DataFrame:

        query = {"_id": {"$in": vehicle_type}, "isDeleted": False, "$expr": {
            "$gte": [{"$strLenCP": "$vehicle_width_metric"}, 20]}}

        projection = {
                    "_id": {"$toString": "$_id"},
                    "vehicleLength": "$vehicleLength.en", "vehicleWidth": "$vehicleWidth.en",
                    "vehicleHeight": "$vehicleHeight.en", "vehicle_pweight": "$vehicleCapacity.en",
                    "vehicle_height_metric": 1, "vehicle_width_metric": 1,
                    "vehicle_length_metric": 1, "vehicleCapacityMetrics": 1}

        vehicle_data = pd.DataFrame(db.vehicleTypes.aggregate([
            {"$match": query},
            {"$project": projection},
            {"$sort": {"_id": -1}}
        ]))

        return vehicle_data

    def acceptance(self, start_time, end_time, driver_type) -> pd.DataFrame:

        query = {"storeType" : 23, "timestamps.new": {"$gte": start_time, "$lte": end_time}, "dispatched": {"$exists": True},\
            "dispatched": {"$nin": [[],""]}}
        
        if driver_type:
            query["driverDetails.storeId"] = {"$exists": False}
        else:
            query["driverDetails.storeId"] = {"$exists": True}

        projection = {"dispatched.firstName": 1, "dispatched.lastName": 1, "dispatched.email": 1, "dispatched.status": 1,\
            "dispatched.driverId": 1, "_id": 0}
        data = pd.DataFrame(db.driverJobs.find(query, projection))

        return data
    
    def dispatch(self, start_time, end_time, driver_type) -> pd.DataFrame:

        query = {"timestamps.new": {"$gte": start_time, "$lte": end_time}, \
            "storeType" : 23, "status.status": {"$in": [4,5,6,7,8]}, "estimatedPickupTimestamp": {"$exists": True}}
        projection = {"timestamps.atPickup": 1, "estimatedPickupTimestamp": 1, "dispatched.firstName": 1, "dispatched.lastName": 1,\
            "dispatched.email": 1, "dispatched.status": 1, "dispatched.driverId": 1, "_id": 0}
        data = pd.DataFrame(db.driverJobs.find(query, projection))

        return data

    def delivery(self, start_time, end_time, driver_type) -> pd.DataFrame:

        query = {"timestamps.new": {"$gte": start_time, "$lte": end_time}, \
            "storeType" : 23, "status.status": 8, "estimatedDeliveryTimestamp": {"$exists": True}}
        projection = {"timestamps": 1, "estimatedDeliveryTimestamp": 1}
        data = pd.DataFrame(db.driverJobs.find(query, projection))

        return data

    def performance(self, start_time, end_time, driver_type) -> pd.DataFrame:

        query = {"storeType" : 23, "timestamps.new": {"$gte": start_time, "$lte": end_time}, "dispatched": {"$exists": True},\
            "dispatched": {"$nin": [[],""]}}
        
        if driver_type:
            query["driverDetails.storeId"] = {"$exists": False}
        else:
            query["driverDetails.storeId"] = {"$exists": True}

        projection = {"dispatched.firstName": 1, "dispatched.lastName": 1, "dispatched.email": 1, "dispatched.status": 1,\
            "dispatched.driverId": 1, "_id": 0}
        data = pd.DataFrame(db.driverJobs.find(query, projection))

        return data
