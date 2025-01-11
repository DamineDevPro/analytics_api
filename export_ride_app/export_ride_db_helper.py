from analytics.settings import  UTC, db
from datetime import datetime
from bson import ObjectId
import pymongo


class DbHelper:
    def invoice_report(self, start_time, end_time, city_id, search, booking_status, country_id=""):
        query = {}
        if start_time and end_time: query["bookingDateTimestamp"] = {"$gte": start_time, "$lte": end_time}
        if country_id: query["countryId"] = country_id
        if city_id: query["cityId"] = city_id
        if search:
            query["$or"] = [
                {"bookingId": {"$regex": "^" + search + "$", "$options": "i"}},
                {"driverDetails.firstName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"cityName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"slaveDetails.name": {"$regex": "^" + search + "$", "$options": "i"}}
            ]
        if booking_status:
            query["bookingStatus"] = booking_status
        else:
            query["bookingStatus"] = {"$in": [4, 12]}
        print("query-------->", query)

        data = db["bookings_rides"].find(query)
        return data

    def fare_report(self, start_time, end_time, city_id, search, vehicle_type, booking_type):
        query = {}
        if start_time and end_time: query["time"] = {
            "$gte": datetime.utcfromtimestamp(start_time),
            "$lte": datetime.utcfromtimestamp(end_time)}
        if city_id: query["cityName"] = city_id
        if vehicle_type: query["vtId"] = vehicle_type
        if booking_type: query["bookingType"] = booking_type
        if search:
            query["$or"] = [
                {"uName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"vehicleTypeName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"cityName.name": {"$regex": "^" + search + "$", "$options": "i"}}
            ]
        print("query-------->", query)
        data = db.estimates_rides.find(query)
        return data

    def expired_booking(self, start_time, end_time, city_id, vehicle_type, booking_type, cancel_type, search,
                        country_id):
        query = {"bookingStatus": 13}
        if start_time and end_time: query["bookingDateTimestamp"] = {"$gte": start_time, "$lte": end_time}
        if country_id: query["countryId"] = country_id
        if city_id: query["cityId"] = ObjectId(city_id)
        if search:
            query["$or"] = [
                {"bookingId": {"$regex": "^" + search + "$", "$options": "i"}},
                {"driverDetails.firstName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"cityName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"slaveDetails.name": {"$regex": "^" + search + "$", "$options": "i"}}
            ]
        if vehicle_type: query["vehicleType.typeId"] = ObjectId(vehicle_type)
        if booking_type: query["bookingType"] = booking_type
        print("query-------->", query)

        data = db["bookings_rides"].find(query)
        return data

    def cancelled_booking(self, start_time, end_time, city_id, vehicle_type, booking_type, cancel_type, search,
                          country_id):
        _cancel = {0: [4, 5], 1: 5, 2: 4}[cancel_type]
        query = {}
        if cancel_type:
            query["bookingStatus"] = _cancel
        else:
            query["bookingStatus"] = {"$in": _cancel}
        if start_time and end_time: query["bookingDateTimestamp"] = {"$gte": start_time, "$lte": end_time}
        if country_id: query["countryId"] = country_id
        if city_id: query["cityId"] = ObjectId(city_id)
        if search:
            query["$or"] = [
                {"bookingId": {"$regex": "^" + search + "$", "$options": "i"}},
                {"driverDetails.firstName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"cityName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"slaveDetails.name": {"$regex": "^" + search + "$", "$options": "i"}}
            ]
        if vehicle_type: query["vehicleType.typeId"] = ObjectId(vehicle_type)
        if booking_type: query["bookingType"] = booking_type
        print("query-------->", query)

        data = db["bookings_rides"].find(query)
        return data

    def completed_booking(self, start_time, end_time, city_id, vehicle_type, booking_type, cancel_type, search,
                          country_id):
        query = {"bookingStatus": 12}
        if start_time and end_time: query["bookingDateTimestamp"] = {"$gte": start_time, "$lte": end_time}
        if country_id: query["countryId"] = country_id
        if city_id: query["cityId"] = ObjectId(city_id)
        if search:
            query["$or"] = [
                {"bookingId": {"$regex": "^" + search + "$", "$options": "i"}},
                {"driverDetails.firstName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"cityName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"slaveDetails.name": {"$regex": "^" + search + "$", "$options": "i"}}
            ]
        if vehicle_type: query["vehicleType.typeId"] = ObjectId(vehicle_type)
        if booking_type: query["bookingType"] = booking_type
        print("query-------->", query)

        data = db["bookings_rides"].find(query)
        return data

    def data(self, start_time, end_time, _type, file_status, platform, city_id=""):
        query = {"type": _type, "platform": platform}
        if _type in [8, 9]:
            query["file_type"] = file_status
        if start_time and end_time: query["create_ts"] = {"$gte": start_time, "$lte": end_time}
        projections = {"_id": 0}
        if _type == 8 and platform == 2: query["city_id"] = city_id
        print("query", query)
        print("projections", projections)
        export_data = db["analyticsExport"].find(query, projections).sort("create_ts", pymongo.DESCENDING)
        count = int(export_data.count())
        return export_data, count

    def acceptance_rate(self, search, city_id, country_id):
        query = {}
        if city_id:
            query["cityId"] = city_id
        elif country_id:
            query["countryId"] = country_id
        else:
            pass
        if search:
            query["$or"] = [
                {"firstName": {"$regex": "^" + search + "$", "$options": "i"}},
                {"lastName": {"$regex": "^" + search + "$", "$options": "i"}},
            ]
        projection = {"driverTypeText": 1, "email": 1, "firstName": 1,
                      "lastName": 1, "countryCode": 1,
                      "mobile": 1, "acceptance": 1, "countryName": 1, "cityName": 1}
        print("query -------->", query)
        data = db.driver.find(query, projection).sort("_id", -1)
        return data, data.count()
