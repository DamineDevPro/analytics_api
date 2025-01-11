from analytics.settings import UTC, db
import pandas as pd


class DbHelper:

    def data(self, start_time, end_time, _type, skip, limit, store_id, store_category_id, platform):
        query = {"type": _type, "platform": platform}
        if store_id in ["", "0"]:
            pass
        else:
            query["store_id"] = store_id
        if store_category_id:
            query = {"store_category_id": store_category_id}
        if start_time and end_time: query["create_ts"] = {"$gte": start_time, "$lte": end_time}
        # projections = {"create_date": 0,
        #                "start_date": 0,
        #                "end_date": 0,
        #                "_id": 0
        #                }
        print("query", query)
        projections = {"_id": 0}
        # print("projections", projections)
        export_data = db["analyticsExport"].find(query, projections).sort("create_ts", -1)
        count = int(export_data.count())
        return export_data.skip(skip).limit(limit), count

    def per_stop_data(self, start_time, end_time, customer_id):

        match = {
            "status.status": 8, "storeType": 23,
            "storeCategoryId": "61d7d9382f2f8c424df6fb16",  # TODO: Store categories need to integrated
            # "deliveryDetails.fareDetailsVehicleType.billingType": 1,
            # "accounting.deliveryDetails.fareDetailsVehicleType.strategyType": 4,
        }

        project = {
            "Booking ID": "$masterOrderId",
            "Booked On": "$timestamps.new",
            # "Posted By": "$deliveryTypeText",
            "Sender Name": {"$concat": ["$customerDetails.firstName", " ", "$customerDetails.lastName"]},
            "Vehicle Type": "$vehicleTypeName",
            "Requested Pickup Time": "$requestedForTimeStamp",
            "currencySymbol": "$accounting.currencySymbol",
            "Pickup Address": {
                "$concat": ["$pickupAddress.addressLine1", "$pickupAddress.addressArea", "$pickupAddress.city",
                            "$pickupAddress.state", "$pickupAddress.countryName"]},
            "Drop Time": "$timestamps.completed",
            "Drop Address": {
                "$concat": ["$deliveryAddress.addLine1", "$deliveryAddress.city", "$deliveryAddress.countryName"]},
            "Load Type": "$loadTypeText",
            "Driver Name": {
                "$concat": ["$driverDetails.firstName", " ", "$driverDetails.lastName", " ", "$driverDetails.mobile"]},
            "Completed On": "$timestamps.completed",
            "Total Amount": "$accounting.finalTotal",
            "Booking Date": "$timestamps.new",
        }

        if customer_id:
            match["customerId"] = customer_id

        if start_time or end_time:
            match["timestamps.new"] = {
                "$gte": start_time, "$lte": end_time}

        # ----------------------------------------------------
        print("Accounting match --->", match)
        print("Accounting match --->", project)

        # Mongo Query Operations
        query = [
            {"$match": match},
            {"$project": project}
            # {"$sort": {"_id": -1}},
        ]
        print("Accounting query --->", query)
        data = pd.DataFrame(db.driverJobs.aggregate(query))

        return data

    def mileage_stop_data(self, start_time, end_time, customer_id):

        match = {
            "status.status": 8, "storeType": 23,
            "storeCategoryId": "61d7d9382f2f8c424df6fb16",
            "accounting.deliveryDetails.fareDetailsVehicleType.strategyType": 2
        }

        project = {"Booking ID": "$masterOrderId",
                   # "Booked On":"$deliveryAddress.createdTimeStamp",
                   "Booked On": "$timestamps.new",
                   "currencySymbol": "$accounting.currencySymbol",
                   "Sender Name": {"$concat": ["$pickupAddress.name", " - ", "$pickupAddress.mobileNumber"]},
                   "Vehicle Type": "$vehicleTypeName",
                   "Requested Pickup Time": "$requestedForTimeStamp",
                   "Pickup Address": {
                       "$concat": ["$pickupAddress.addressLine1", "$pickupAddress.addressArea", "$pickupAddress.city",
                                   "$pickupAddress.state", "$pickupAddress.countryName"]},
                   "Drop Time": "$timestamps.completed",
                   "Drop Address": {"$concat": ["$deliveryAddress.addLine1", "$deliveryAddress.city",
                                                "$deliveryAddress.countryName"]},
                   "Load Type": "$loadTypeText",
                   "Amount": "$accounting.finalTotal",
                   "Driver Name": {"$concat": ["$driverDetails.firstName", " ", "$driverDetails.lastName", " ",
                                               "$driverDetails.mobile"]},
                   "Completed On": "$timestamps.completed",
                   "Booking Date": "$timestamps.new",
                   "parentOrderId": 1,
                   "Miles": "$deliveryDetails.distanceInMetrics",
                   "Mileage Price": "$deliveryDetails.fareDetailsVehicleType.distanceFeePerUnit",
                   "Free Mileage": "$deliveryDetails.fareDetailsVehicleType.mileageAfterXMetric",
                   "Base Fee": "$deliveryDetails.fareDetailsVehicleType.baseFee",
                   "Stop Fee per stop": "$accounting.deliveryDetails.fareDetailsVehicleType.feePerStop",
                   "Total time (in min)": "$accounting.totalTimeForJobInSeconds",
                   "Free Delivery Minutes": "$accounting.deliveryDetails.fareDetailsVehicleType.timeFeeXMinute",
                   "Time Fee per min": "$accounting.deliveryDetails.fareDetailsVehicleType.timeFeePerUnit",
                   }

        if customer_id:
            match["customerId"] = customer_id

        if start_time or end_time:
            match["timestamps.new"] = {
                "$gte": start_time, "$lte": end_time}

        # ----------------------------------------------------
        print("Accounting match --->", match)
        print("Accounting match --->", project)

        # Mongo Query Operations
        query = [
            {"$match": match},
            {"$project": project}
            # {"$sort": {"_id": -1}},
        ]
        print("Accounting query --->", query)
        data = pd.DataFrame(db.driverJobs.aggregate(query))

        return data

    def hourly_fee_data(self, start_time, end_time, customer_id):

        match = {
            "status.status": 8, "storeType": 23,
            "storeCategoryId": "61d7d9382f2f8c424df6fb16",
            "accounting.deliveryDetails.fareDetailsVehicleType.strategyType": 1
        }

        project = {
            "Booking ID": "$masterOrderId",
            "Booked On": "$timestamps.new",
            # "Posted By": "$deliveryTypeText",
            "Sender Name": {"$concat": ["$customerDetails.firstName", " ", "$customerDetails.lastName"]},
            "Vehicle Type": "$vehicleTypeName",
            "Requested Pickup Time": "$requestedForTimeStamp",
            "currencySymbol": "$accounting.currencySymbol",
            "Pickup Address": {"$concat": ["$pickupAddress.addressLine1", "$pickupAddress.addressArea",
                                           "$pickupAddress.city", "$pickupAddress.state",
                                           "$pickupAddress.countryName"
                                           ]},
            "Drop Time": "$timestamps.completed",
            "Drop Address": {"$concat": ["$deliveryAddress.addLine1", "$deliveryAddress.city",
                                         "$deliveryAddress.countryName"]},
            "Load Type": "$loadTypeText",
            "Amount": "$accounting.finalTotal",
            "Driver Name": {
                "$concat": ["$driverDetails.firstName", " ", "$driverDetails.lastName", " ", "$driverDetails.mobile"]},
            "Completed On": "$timestamps.completed",
            "Booking Date": "$timestamps.new",
            "Driver Id": "$driverDetails.driverId",
            "Hours": "$accounting.totalTimeForJobInMinutes",
            "Hourly Fee": "$accounting.totalHourlyFeeApplied",
            "Return Fee": "$accounting.totalReturnFee",
        }

        if customer_id:
            match["customerId"] = customer_id

        if start_time or end_time:
            match["timestamps.new"] = {
                "$gte": start_time, "$lte": end_time}

        # ----------------------------------------------------
        print("Accounting match --->", match)
        print("Accounting match --->", project)

        # Mongo Query Operations
        query = [
            {"$match": match},
            {"$project": project}
            # {"$sort": {"_id": -1}},
        ]
        print("Accounting query --->", query)
        data = pd.DataFrame(db.driverJobs.aggregate(query))

        return data

    def load_report_data(self, start_time, end_time, customer_id):

        match = {
            "status.status": 8, "storeType": 23,
            # "storeCategoryId": "61d7d9382f2f8c424df6fb16",
            "accounting.deliveryDetails.fareDetailsVehicleType.strategyType": 2
        }

        project = {"Booking ID": "$masterOrderId",
                   "Booked On": "$timestamps.new",
                   "Posted By": "$deliveryTypeText",
                   "currencySymbol": "$accounting.currencySymbol",
                   "Customer": {"$concat": ["$customerDetails.firstName", " ", "$customerDetails.lastName"]},
                   "Vehicle Type": "$vehicleTypeName",
                   "Requested Pickup Time": "$requestedForTimeStamp",
                   "Pickup Address": {
                       "$concat": ["$pickupAddress.addressLine1", "$pickupAddress.addressArea", "$pickupAddress.city",
                                   "$pickupAddress.state", "$pickupAddress.countryName"]},
                   "Drop Time": "$timestamps.completed",
                   "Drop Address": {"$concat": ["$deliveryAddress.addLine1", "$deliveryAddress.city",
                                                "$deliveryAddress.countryName"]},
                   "Load Type": "$loadTypeText",
                   "Amount": "$accounting.finalTotal",
                   "Driver Name": {"$concat": ["$driverDetails.firstName", " ", "$driverDetails.lastName", " ",
                                               "$driverDetails.mobile"]},
                   "Completed On": "$timestamps.completed",
                   "Booking Date": "$timestamps.new",
                   "_id":0
                #    "parentOrderId": 1,
                #    "Miles": "$deliveryDetails.distanceInMetrics",
                #    "Mileage Price": "$deliveryDetails.fareDetailsVehicleType.distanceFeePerUnit",
                #    "Free Mileage": "$deliveryDetails.fareDetailsVehicleType.mileageAfterXMetric",
                #    "Base Fee": "$deliveryDetails.fareDetailsVehicleType.baseFee",
                #    "Stop Fee per stop": "$accounting.deliveryDetails.fareDetailsVehicleType.feePerStop",
                #    "timeFee": "$accounting.deliveryDetails.fareDetailsVehicleType.timeFee",
                #    "Time To Delivery": "$accounting.deliveryDetails.fareDetailsVehicleType.timeFee",
                #    "Free Delivery Minutes": "$accounting.deliveryDetails.fareDetailsVehicleType.timeFee",
                   }

        if customer_id:
            match["customerId"] = customer_id

        if start_time or end_time:
            match["timestamps.new"] = {
                "$gte": start_time, "$lte": end_time}

        # ----------------------------------------------------
        print("Accounting match --->", match)
        print("Accounting match --->", project)

        # Mongo Query Operations
        query = [
            {"$match": match},
            {"$project": project}
            # {"$sort": {"_id": -1}},
        ]
        print("Accounting query --->", query)
        data = pd.DataFrame(db.driverJobs.aggregate(query))

        return data
        