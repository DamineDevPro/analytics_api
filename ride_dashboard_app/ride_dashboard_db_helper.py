import os, sys
from analytics.settings import db
from analytics.settings import  UTC, db, CURRENCY_API, BASE_CURRENCY
from bson import ObjectId
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application


class DbHelper:

    def ride_trip_data(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id,
                       booking_status=0):
        try:
            assert False
            query = "SELECT bookingDateTimestamp createdDate, " \
                    "invoice.total estimate_fare, " \
                    "vehicleType.typeId typeId, " \
                    "vehicleType.typeName typeName " \
                    "FROM bookings_rides " \
                    "WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            booking_type_query = " AND bookingStatus == {}".format(booking_status) if booking_status else ""
            device_query = " AND slaveDetails.deviceType == {}".format(device_type) if device_type else ""
            query = query + booking_type_query + device_query
            if country_id: query = query + " AND countryId == '{}'".format(country_id)
            if city_id: query = query + " AND cityId.oid == '{}'".format(city_id)
            if zone_id: query = query + " AND (pickupOperationZoneId == '{zone_id}' OR dropOperationZoneId == '{zone_id}')".format(
                zone_id=zone_id)
            query = query.replace("  ", " ")
            print("query----------->", query)
            query = query.strip()
            result_data = sqlContext.sql(query)
            result_data = result_data.toPandas()
            return result_data
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, 'serviceType': 2}
            if booking_status: query["bookingStatus"] = booking_status
            if device_type: query["slaveDetails.deviceType"] = device_type
            if country_id: query["countryId"] = {"$in": [country_id, ObjectId(country_id)]}
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [{"pickupOperationZoneId": zone_id},
                                        {"dropOperationZoneId": zone_id},
                                        ]
            projection = {"bookingDateTimestamp": 1,
                          "invoice.total": 1,
                          "vehicleType.typeId": 1,
                          "vehicleType.typeName": 1,
                          }
            print("query--->", query)
            result_data = pd.DataFrame(db.bookings_rides.find(query, projection))
            if result_data.shape[0]:
                result_data["estimate_fare"] = result_data["invoice"].apply(lambda x: x.get("total", 0))
                result_data["typeId"] = result_data["vehicleType"].apply(lambda x: x.get("typeId", ""))
                result_data["typeName"] = result_data["vehicleType"].apply(lambda x: x.get("typeName", ""))
                result_data = result_data.rename(columns={"bookingDateTimestamp": "createdDate"})
            return result_data

    def ride_status(self, start_timestamp, end_timestamp, device_type, country_id, city_id, zone_id, booking_status):
        """

        """
        try:
            assert False
            date_range_query = " WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            #  "4": "Customer Cancelled", "5": "Driver Cancelled" for filter
            vehicle_type_query = " AND bookingStatus IN {}".format((4, 5))
            query = "SELECT bookingDateTimestamp, vehicleType.typeId typeId, vehicleType.typeName typeName, bookingStatus from bookings_rides" \
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
            query = query.strip()
            order = sqlContext.sql(query)
            order = order.toPandas()
            return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp},
                     "bookingStatus": {'$in': [4, 5]}, 'serviceType': 2
                     }
            if device_type: query["slaveDetails.deviceType"] = device_type
            if booking_status: query["bookingStatus"] = booking_status
            if country_id: query["countryId"] = {"$in": [country_id, ObjectId(country_id)]}
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {"bookingDateTimestamp": 1, "vehicleType.typeId": 1,
                          "vehicleType.typeName": 1, "bookingStatus": 1
                          }
            print("query--->", query)
            result_data = pd.DataFrame(db.bookings_rides.find(query, projection))
            if result_data.shape[0]:
                result_data["typeId"] = result_data["vehicleType"].apply(lambda x: x.get("typeId", ""))
                result_data["typeName"] = result_data["vehicleType"].apply(lambda x: x.get("typeName", ""))
            return result_data

    def ride_payment(self, start_timestamp, end_timestamp, vehicle_type_query, device_type, country_id, city_id,
                     zone_id, booking_status, vehicle_mongo_query):
        """

        """
        try:
            assert False
            date_range_query = " WHERE bookingDateTimestamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT bookingDateTimestamp, vehicleType.typeId typeId, vehicleType.typeName typeName, paymentType from bookings_rides" \
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
            order = sqlContext.sql(query)
            order = order.toPandas()
            return order
        except:
            query = {"bookingDateTimestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, 'serviceType': 2}
            if device_type: query["slaveDetails.deviceType"] = device_type
            query.update(vehicle_mongo_query)
            if booking_status: query["bookingStatus"] = booking_status
            if country_id: query["countryId"] = country_id
            if city_id: query["cityId"] = {"$in": [city_id, ObjectId(city_id)]}
            if zone_id: query["$or"] = [
                {"pickupOperationZoneId": zone_id},
                {"dropOperationZoneId": zone_id}
            ]
            projection = {"bookingDateTimestamp": 1, "vehicleType.typeId": 1,
                          "vehicleType.typeName": 1, "paymentType": 1}

            print("query--->", query)
            result_data = pd.DataFrame(db.bookings_rides.find(query, projection))
            if result_data.shape[0]:
                result_data["typeId"] = result_data["vehicleType"].apply(lambda x: x.get("typeId", ""))
                result_data["typeName"] = result_data["vehicleType"].apply(lambda x: x.get("typeName", ""))
            return result_data
