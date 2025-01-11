import os, sys
from analytics.settings import db
from analytics.settings import  UTC, db, CURRENCY_API, BASE_CURRENCY
from bson import ObjectId
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application


class DbHelper:

    def trucker_trip_data(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id,
                       booking_status=0):

        # query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}, "storeType" : 23}
        # if booking_status: query["status.status"] = booking_status
        # if device_type: query["createdBy.deviceType"] = device_type
        # if country_id: query["deliveryAddress.countryId"] = {"$in": [country_id, ObjectId(country_id)]}
        # if city_id: query["deliveryAddress.cityId"] = {"$in": [city_id, ObjectId(city_id)]}
        # # if zone_id: query["$or"] = [{"pickupOperationZoneId": zone_id},
        # #                             {"dropOperationZoneId": zone_id},
        #                             # ]
        # projection = {"createdTimeStamp": 1,
        #               "accounting.finalTotal": 1,
        #               "vehicleType.typeId": 1,
        #               "vehicleType.typeName": 1,
        #               }

        # result_data = pd.DataFrame(db.storeOrder.find(query, projection))
        # if result_data.shape[0]:
        #     result_data["estimate_fare"] = result_data["invoice"].apply(lambda x: x.get("total", 0))
        #     result_data["typeId"] = result_data["vehicleType"].apply(lambda x: x.get("typeId", ""))
        #     result_data["typeName"] = result_data["vehicleType"].apply(lambda x: x.get("typeName", ""))
        #     result_data = result_data.rename(columns={"bookingDateTimestamp": "createdDate"})
        # return result_data

        query = {"timestamps.new": {"$gte": start_timestamp, "$lte": end_timestamp}, "storeType" : 23}
        if booking_status: query["status.status"] = booking_status
        if device_type: query["createdBy.deviceType"] = device_type
        if country_id: query["deliveryAddress.countryId"] = {"$in": [country_id, ObjectId(country_id)]}
        if city_id: query["deliveryAddress.cityId"] = {"$in": [city_id, ObjectId(city_id)]}
        projection = {"timestamps.new": 1, "accounting.finalTotal": 1, "vehicleTypeId": 1, "vehicleTypeName": 1}

        print("query-------->", query)
        result_data = pd.DataFrame(db.driverJobs.find(query, projection))
        if result_data.shape[0]:
            
            result_data['timestamps'] = result_data['timestamps'].apply(lambda x: x['new'])
            result_data = result_data.rename(columns={"timestamps": "createdDate"})
            result_data["estimate_fare"] = result_data["accounting"].apply(
                lambda x: x.get("finalTotal", 0) if isinstance(x, dict) else 0)
            result_data["typeId"] = result_data["vehicleTypeId"]
            result_data["typeName"] = result_data["vehicleTypeName"]
            result_data = result_data.drop(columns=["vehicleTypeId", "vehicleTypeName","accounting"])
        return result_data

    def trucker_status(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id, booking_status):
        """

        """
        try:
            assert False
            date_range_query = " WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            #  "4": "Customer Cancelled", "5": "Driver Cancelled" for filter
            vehicle_type_query = " AND bookingStatus IN {}".format((4, 5))
            query = "SELECT bookingDateTimestamp, vehicleType.typeId typeId, vehicleType.typeName typeName, bookingStatus from bookings_truckers" \
                    + date_range_query + vehicle_type_query
            device_query = " AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            query = query + device_query
            if booking_status: query = query + " AND bookingStatus == {}".format(booking_status)
            if country_id: query = query + " AND countryId == '{}'".format(country_id)
            if city_id: query = query + " AND cityId.oid == '{}'".format(city_id)
            if zone_id: query = query + " AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id)
            query = query.replace("  ", " ")
            print("query----------->", query)
            assert False
            query = query.strip()
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:

            query = {"timestamps.new": {"$gte": start_timestamp, "$lte": end_timestamp}, "storeType" : 23}
            if booking_status: query["status.status"] = booking_status
            if device_type: query["createdBy.deviceType"] = device_type
            if country_id: query["deliveryAddress.countryId"] = {"$in": [country_id, ObjectId(country_id)]}
            if city_id: query["deliveryAddress.cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            # if zone_id: query["$or"] = [
            #     {"pickupOperationZoneId": zone_id},
            #     {"dropOperationZoneId": zone_id}
            # ]
            projection = {"timestamps.new": 1, "accounting.finalTotal": 1, "vehicleTypeId": 1, "vehicleTypeName": 1, "status.status": 1}
            # result_data = pd.DataFrame(db.driverJobs.find(query, projection))
            # if result_data.shape[0]:
            #     result_data["typeId"] = result_data["vehicleType"].apply(lambda x: x.get("typeId", ""))
            #     result_data["typeName"] = result_data["vehicleType"].apply(lambda x: x.get("typeName", ""))
            # return result_data
            result_data = pd.DataFrame(db.driverJobs.find(query, projection))
            if result_data.shape[0]:
                
                result_data['bookingDateTimestamp'] = result_data['timestamps'].apply(lambda x: x['new'])
                # result_data = result_data.rename(columns={"timestamps": "bookingDateTimestamp"})
                result_data["estimate_fare"] = result_data["accounting"].apply(
                    lambda x: x.get("finalTotal", 0) if isinstance(x, dict) else 0)
                result_data["typeId"] = result_data["vehicleTypeId"]
                result_data["typeName"] = result_data["vehicleTypeName"]
                result_data = result_data.drop(columns=["vehicleTypeId", "vehicleTypeName","accounting"])
            return result_data

    def trucker_payment(self, start_timestamp, end_timestamp, vehicle_type_query, device_type, country_id, city_id,
                     zone_id, booking_status, vehicle_mongo_query):
        """

        """
        try:
            assert False
            date_range_query = " WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT bookingDateTimestamp, vehicleType.typeId typeId, vehicleType.typeName typeName, paymentType from bookings_truckers" \
                    + date_range_query + vehicle_type_query
            device_query = " AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            if booking_status: query = query + " AND bookingStatus == {}".format(booking_status)
            if country_id: query = query + " AND countryId == '{}'".format(country_id)
            if city_id: query = query + " AND cityId.oid == '{}'".format(city_id)
            if zone_id: query = query + " AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id)
            query = query + device_query
            query = query.replace("  ", " ")
            print("query----------->", query)
            query = query.strip()
            assert False
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:
            query = {"timestamps.new": {"$gte": start_timestamp, "$lte": end_timestamp}, "storeType" : 23}
            if booking_status: query["status.status"] = booking_status
            if device_type: query["createdBy.deviceType"] = device_type
            if country_id: query["countryId"] = country_id
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if booking_status: query["bookingStatus"] = booking_status
            # if zone_id: query["$or"] = [
            #     {"pickupOperationZoneId": zone_id},
            #     {"dropOperationZoneId": zone_id}
            # ]
            projection = {"timestamps.new": 1, "accounting.finalTotal": 1, "vehicleTypeId": 1, "vehicleTypeName": 1, "accounting.payBy": 1}

            result_data = pd.DataFrame(db.driverJobs.find(query, projection))
            if result_data.shape[0]:
            
                result_data['timestamps'] = result_data['timestamps'].apply(lambda x: x['new'])
                result_data = result_data.rename(columns={"timestamps": "createdDate"})
                result_data["estimate_fare"] = result_data["accounting"].apply(
                    lambda x: x.get("finalTotal", 0) if isinstance(x, dict) else 0)
                result_data["typeId"] = result_data["vehicleTypeId"]
                result_data["typeName"] = result_data["vehicleTypeName"]
                result_data['paymentType'] = result_data['accounting'].apply(lambda x: x['payBy'])
                def paymentTypeM(x):
                    if x['card']:
                        return 1
                    elif x['cash']:
                        return 2
                    else:
                        return 3
                result_data['paymentType'] = result_data['paymentType'].apply(paymentTypeM)
                result_data = result_data.drop(columns=["vehicleTypeId", "vehicleTypeName","accounting"])
            return result_data
