import os, sys
from analytics.settings import  UTC, db, CURRENCY_API, BASE_CURRENCY
from bson import ObjectId
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

class DbHelper:

    def fare_count(self, start_timestamp, end_timestamp, store_query, store_categories_query, conversion_rate,
                   device_type, country_id, city_id, zone_id):
        
        match = {
            "createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp},
            "status.status": 7, "storeType" : 23
        }
        if device_type: match["createdBy.deviceType"] = device_type
        if country_id: match["deliveryAddress.countryId"] = country_id
        if city_id: match["deliveryAddress.cityId"] = {"$in": [city_id, ObjectId(city_id)]}

        query = [
            {"$match": match},
            {"$group": {"_id": None, "sum": {"$sum": "$accounting.finalTotal"}}}
        ]
        try:
            count_data = list(db.storeOrder.aggregate(query))[0].get("sum", 0)
        except:
            count_data = 0
        count_data = count_data * conversion_rate
        return count_data

    def total_sales(self, start_timestamp, end_timestamp, store_categories_id, store_id, column_key_dict):
        
        query = {
            "createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp},
            "status.status": 7, "storeType" : 23
        }
        if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
        if store_id != "0" and store_id: query["storeId"] = store_id
        # projection = {}
        data = pd.DataFrame(db.storeOrder.find(query,column_key_dict))

        return data

    def order_fare_count(self, start_timestamp, end_timestamp, store_query, store_categories_query, conversion_rate,
                   device_type, country_id, city_id, zone_id):
        
        match = {
            "createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp},
            "status.status": {"$ne": 3}, "storeType" : 23
        }
        if device_type: match["createdBy.deviceType"] = device_type
        if country_id: match["deliveryAddress.countryId"] = country_id
        if city_id: match["deliveryAddress.cityId"] = {"$in": [city_id, ObjectId(city_id)]}

        query = [
            {"$match": match},
            {"$group": {"_id": None, "sum": {"$sum": "$accounting.finalTotal"}}}
        ]
        try:
            count_data = list(db.storeOrder.aggregate(query))[0].get("sum", 0)
        except:
            count_data = 0
        count_data = count_data * conversion_rate
        return count_data


    def trucker_trip_data(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id,
                       booking_status=0):
        try:
            assert False
            query = "SELECT bookingDateTimestamp createdDate, invoice.total estimate_fare, vehicleType.typeId typeId, vehicleType.typeName typeName FROM bookings_truckers " \
                    "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            booking_type_query = "AND bookingStatus == {}".format(booking_status) if booking_status else ""
            device_query = "AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            # -------------------------------
            country_query = "AND countryId == '{}'".format(country_id) if country_id else ""
            city_query = "AND cityId.oid == '{}'".format(city_id) if city_id else ""
            zone_query = "AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id) if zone_id else ""
            # -------------------------------

            query = " ".join([query, booking_type_query, device_query, country_query, city_query, zone_query])
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            return result_data
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, 'serviceType': 5}
            if booking_status: query["bookingStatus"] = booking_status
            if device_type: query["slaveDetails.deviceType"] = device_type
            if country_id: query["countryId"] = country_id
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if booking_status: query["bookingStatus"] = booking_status
            params = {"bookingDateTimestamp": 1, "invoice.total": 1, "vehicleType.typeId": 1, "vehicleType.typeName": 1}

            result_data = pd.DataFrame(db.bookings_truckers.find(query, params))
            if result_data.shape[0]:
                result_data = result_data.rename(columns={"bookingDateTimestamp": "createdDate"})
                result_data["estimate_fare"] = result_data["invoice"].apply(
                    lambda x: x.get("total", 0) if isinstance(x, dict) else 0)
                result_data["typeId"] = result_data["vehicleType"].apply(
                    lambda x: x.get("typeId", 0) if isinstance(x, dict) else 0)
                result_data["typeName"] = result_data["vehicleType"].apply(
                    lambda x: x.get("typeName", 0) if isinstance(x, dict) else 0)
                result_data = result_data.drop("vehicleType", axis=1)
            return result_data

    def trucker_payment(self, start_timestamp, end_timestamp, vehicle_type_id, device_type, country_id, city_id, zone_id,
                     booking_status=0):
        """

        """
        try:
            vehicle_type_query = " AND vehicleType.typeId typeId == '{}'".format(
                vehicle_type_id) if vehicle_type_id else ""
            device_query = "AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            date_range_query = "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT bookingDateTimestamp, vehicleType.typeId typeId, vehicleType.typeName typeName, paymentType from bookings_truckers"
            booking_type_query = "AND bookingStatus == {}".format(booking_status) if booking_status else ""
            country_query = "AND countryId == '{}'".format(country_id) if country_id else ""
            city_query = "AND cityId.oid == '{}'".format(city_id) if city_id else ""
            zone_query = "AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id) if zone_id else ""
            query = " ".join(
                [query, date_range_query, booking_type_query, vehicle_type_query, device_query, country_query,
                 city_query, zone_query])
            print("query----------->", query)
            query = query.strip()
            assert False
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, 'serviceType': 5}
            if vehicle_type_id: query["vehicleType.typeId"] = vehicle_type_id
            if booking_status: query["bookingStatus"] = booking_status
            if device_type: query["slaveDetails.deviceType"] = device_type
            if country_id: query["countryId"] = country_id
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if booking_status: query["bookingStatus"] = booking_status
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            params = {"bookingDateTimestamp": 1, "paymentType": 1, "vehicleType.typeId": 1, "vehicleType.typeName": 1}

            result_data = pd.DataFrame(db.bookings_truckers.find(query, params))

            if result_data.shape[0]:
                result_data["typeId"] = result_data["vehicleType"].apply(
                    lambda x: x.get("typeId", 0) if isinstance(x, dict) else 0)
                result_data["typeName"] = result_data["vehicleType"].apply(
                    lambda x: x.get("typeName", 0) if isinstance(x, dict) else 0)
                result_data = result_data.drop("vehicleType", axis=1)
            return result_data

    def vehicle_type(self):
        query = {"isDeleted": False}
        projection = {"typeName": 1, "_id": 1}
        data = db.vehicleTypes.find(query, projection)
        return data

    def trucker_status(self, start_timestamp, end_timestamp, vehicle_type_id, device_type, country_id, city_id, zone_id):
        """

        """
        try:
            date_range_query = "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            #  "4": "Customer Cancelled", "5": "Driver Cancelled" for filter
            vehicle_type_query = "AND bookingStatus IN {}".format((4, 5))
            device_query = "AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            query = "SELECT bookingDateTimestamp, " \
                    "vehicleType.typeId typeId, " \
                    "vehicleType.typeName typeName, " \
                    "bookingStatus from bookings_truckers"

            country_query = "AND countryId == '{}'".format(country_id) if country_id else ""
            city_query = "AND cityId.oid == '{}'".format(city_id) if city_id else ""
            zone_query = "AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id) if zone_id else ""

            query = " ".join(
                [query, date_range_query, vehicle_type_query, device_query, country_query, city_query, zone_query])
            print("query----------->", query)
            query = query.strip()
            assert False
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp},
                     "bookingStatus": {"$in": [4, 5]}, 'serviceType': 5
                     }
            if country_id: query["countryId"] = country_id
            if device_type: query["slaveDetails.deviceType"] = device_type
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {"bookingDateTimestamp": 1, "vehicleType.typeId": 1, "vehicleType.typeName": 1,
                          "bookingStatus": 1}
            order = pd.DataFrame(db.bookings_truckers.find(query, projection))
            if order.shape[0]:
                order["typeId"] = order["vehicleType"].apply(lambda x: x.get("typeId", ""))
                order["typeName"] = order["vehicleType"].apply(lambda x: x.get("typeName", ""))
            return order

    def top_trucker(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id):
        try:
            assert False
            date_range_query = "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            #  "4": "Customer Cancelled", "5": "Driver Cancelled"for filter
            vehicle_type_query = "AND bookingStatus == {}".format(12)
            device_query = "AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""

            query = "SELECT bookingDateTimestamp, " \
                    "vehicleType.typeId typeId, " \
                    "vehicleType.typeName typeName, " \
                    "vehicleType.vehicleImgOff vehicleImgOff, " \
                    "countryId, " \
                    "countryName, " \
                    "cityId, " \
                    "cityName, " \
                    "pickup, " \
                    "pickupOperationZoneId pick_zone_id ," \
                    "dropActual drop, " \
                    "dropOperationZoneId drop_zone_id ," \
                    "invoice.total fare " \
                    "FROM bookings_truckers"

            country_query = "AND countryId == '{}'".format(country_id) if country_id else ""
            city_query = "AND cityId.oid == '{}'".format(city_id) if city_id else ""
            zone_query = "AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id) if zone_id else ""

            query = " ".join(
                [query, date_range_query, vehicle_type_query, device_query, country_query, city_query, zone_query])
            print("query----------->", query)
            query = query.strip()
            assert False
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp},
                     "bookingStatus": 12, 'serviceType': 5
                     }
            if country_id: query["countryId"] = country_id
            if device_type: query["slaveDetails.deviceType"] = device_type
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {"bookingDateTimestamp": 1, "vehicleType.typeId": 1, "vehicleType.typeName": 1,
                          "vehicleType.vehicleImgOff": 1, "countryId": 1, "countryName": 1, "cityId": 1,
                          "cityName": 1, "pickup": 1, "pickupOperationZoneId": 1, "dropActual": 1,
                          "dropOperationZoneId": 1, "invoice.total": 1
                          }
            print("query----->", query)
            order = pd.DataFrame(db.bookings_truckers.find(query, projection))
            if order.shape[0]:
                order["typeId"] = order["vehicleType"].apply(lambda x: x.get("typeId", ""))
                order["typeName"] = order["vehicleType"].apply(lambda x: x.get("typeName", ""))
                order["vehicleImgOff"] = order["vehicleType"].apply(lambda x: x.get("vehicleImgOff", ""))
                order["fare"] = order["invoice"].apply(lambda x: x.get("total", ""))
                order = order.rename(columns={"pickupOperationZoneId": "pick_zone_id",
                                              "dropActual": "drop", "dropOperationZoneId": "drop_zone_id"
                                              })
            return order

    def trucker_map_data(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id):
        try:
            assert False
            # Spark SQL query construction
            query = "SELECT bookingId, bookingDateTimestamp, drop.location drop, pickup.location pickup from bookings_truckers "
            date_query = "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            status_query = "AND bookingStatus == {}".format(12)
            device_type = "AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            # query = query + "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            # query = query + "AND bookingStatus == {}".format(12)
            # query = query + "AND slaveDetails.deviceType == {}".format(device_type) if device_type else " "
            # if zone_id: query = query + "AND deliveryAddress.zoneId == '{}' ".format(zone_id)
            # if city_id: query = query + "AND deliveryAddress.cityId == '{}' ".format(city_id)
            # if country_id: query = query + "AND deliveryAddress.countryId == '{}' ".format(country_id)
            country_query = "AND countryId == '{}'".format(country_id) if country_id else ""
            city_query = "AND cityId.oid == '{}'".format(city_id) if city_id else ""
            zone_query = "AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id) if zone_id else ""
            query = " ".join([query, date_query, status_query, device_type, country_query, city_query, zone_query])
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            return result_data
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp},
                     "bookingStatus": 12, 'serviceType': 5
                     }
            if country_id: query["countryId"] = country_id
            if device_type: query["slaveDetails.deviceType"] = device_type
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {"bookingId": 1, "bookingDateTimestamp": 1, "drop.location": 1, "pickup.location": 1}
            order = pd.DataFrame(db.bookings_truckers.find(query, projection))

            if order.shape[0]:
                order["drop"] = order["drop"].apply(lambda x: x.get("location", {}))
                order["pickup"] = order["pickup"].apply(lambda x: x.get("location", {}))
            return order

    def trucker_fare(self, start_timestamp, end_timestamp, vehicle_type_query, device_type, country_id, city_id, zone_id,
                  vehicle_mongo_query, booking_status=0, ):
        """

        """
        try:
            date_range_query = "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT bookingDateTimestamp, vehicleType.typeId typeId, vehicleType.typeName typeName, invoice.total fare from bookings_truckers" \
                    + " " + date_range_query + vehicle_type_query
            booking_type_query = " AND bookingStatus == {}".format(booking_status) if booking_status else ""
            device_query = " AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            query = query + device_query + booking_type_query
            if country_id: query = query + " AND countryId == '{}'".format(country_id)
            if city_id: query = query + " AND cityId.oid == '{}'".format(city_id)
            if zone_id: query = query + " AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id)
            print("query----------->", query)
            assert False
            # query = query.strip()
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, 'serviceType': 5}
            query.update(vehicle_mongo_query)
            if booking_status: query["bookingStatus"] = booking_status
            if device_type: query["slaveDetails.deviceType"] = device_type
            if country_id: query["countryId"] = country_id
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {
                "bookingDateTimestamp": 1,
                "vehicleType.typeId": 1,
                "vehicleType.typeName": 1,
                "invoice.total": 1

            }
            order = pd.DataFrame(db.bookings_truckers.find(query, projection))
            if order.shape[0]:
                order["typeId"] = order["vehicleType"].apply(lambda x: x.get("typeId", ""))
                order["typeName"] = order["vehicleType"].apply(lambda x: x.get("typeName", ""))
                order["fare"] = order["invoice"].apply(lambda x: x.get("total", 0))
            return order

    def surge_fare(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id):
        try:
            query = "SELECT bookingDateTimestamp, " \
                    "vehicleType.typeId typeId, " \
                    "vehicleType.typeName typeName, " \
                    "surgeApplied, " \
                    "invoice.total totalFare " \
                    "from bookings_truckers"
            query = query + " WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            if device_type: query = query + " AND slaveDetails.deviceType == {}".format(device_type)
            if country_id: query = query + " AND countryId == '{}'".format(country_id)
            if city_id: query = query + " AND cityId.oid == '{}'".format(city_id)
            if zone_id: query = query + " AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id)
            query = query.strip()
            assert False
            # order = sqlContext.sql(query)
            # order = order.toPandas()
            # return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, 'serviceType': 5}
            if device_type: query["slaveDetails.deviceType"] = device_type
            if country_id: query["countryId"] = country_id
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {
                "bookingDateTimestamp": 1,
                "vehicleType.typeId": 1,
                "vehicleType.typeName": 1,
                "surgeApplied": 1,
                "invoice.total": 1

            }
            order = pd.DataFrame(db.bookings_truckers.find(query, projection))
            if order.shape[0]:
                order["typeId"] = order["vehicleType"].apply(lambda x: x.get("typeId", ""))
                order["typeName"] = order["vehicleType"].apply(lambda x: x.get("typeName", ""))
                order["totalFare"] = order["invoice"].apply(lambda x: x.get("total", 0))
            return order



