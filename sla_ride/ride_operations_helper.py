import pandas as pd
from .ride_response_helper import RideResponses


class RideOperations:

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

    def driver_details(driver):
        if not isinstance(driver, dict):
            return driver
        driver["driverId"] = str(driver["driverId"])
        return driver

    def booking_data(booking_data, count=0):
        try:
            booking_data = pd.DataFrame(booking_data)
            for col in ["_id", "countryId", "cityId"]:
                booking_data[col] = booking_data[col].apply(lambda x: str(x))
            booking_data = pd.concat([booking_data, booking_data["timeStamp"].apply(pd.Series)], axis=1)
            booking_data["bookingAccepted"] = booking_data[["created", "accepted"]].apply(
                lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            booking_data["atPickup"] = booking_data[["accepted", "arrived"]]. \
                apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            booking_data["tripStarted"] = booking_data[["arrived", "journeyStart"]]. \
                apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)
            booking_data["tripCompleted"] = booking_data[["journeyStart", "journeyComplete"]]. \
                apply(lambda x: x[1] - x[0] if x[1] else x[1], axis=1)

            booking_data["driverDetails"] = booking_data["driverDetails"].apply(RideOperations.driver_details)
            booking_data = booking_data[["_id", "bookingId", "bookingStatus", "bookingStatusText", "serviceType",
                                         "serviceTypeText", "bookingType", "bookingTypeText", "createdDate",
                                         "bookingDate", "bookingDateTimestamp", "bookingDateDevice", "deliveryDate",
                                         "deliveryDateTimestamp", "countryId", "countryName", "cityId",
                                         "pickupOperationZoneId", "dropOperationZoneId", "cityName", "timeStamp",
                                         "driverDetails", "bookingAccepted", "atPickup", "tripStarted",
                                         "tripCompleted"]]
            booking_data = booking_data.fillna("")
            booking_data = booking_data.to_dict(orient="records")
            return RideResponses.get_status_200(data=booking_data, count=count)
        except Exception as ex:
            RideResponses.get_status_500(ex)

    def object_dict_remove(_dict, key):
        if not isinstance(_dict, dict):
            return _dict
        _dict[key] = str(_dict[key])
        return _dict

    def driver_data(data):
        print("Called ####################################")
        # try:
        data = pd.DataFrame(data)
        print("data------------>")
        print(data)
        data["_id"] = data["_id"].apply(lambda x: str(x))
        data["billingAddress"] = data["billingAddress"].apply(
            lambda x: RideOperations.object_dict_remove(_dict=x, key="id"))
        data["deliveryAddress"] = data["deliveryAddress"].apply(
            lambda x: RideOperations.object_dict_remove(_dict=x, key="id"))
        data = data.to_dict(orient="records")
        return RideResponses.get_status_200(data)
        # except Exception as ex:
        #     RideResponses.get_status_500(ex)