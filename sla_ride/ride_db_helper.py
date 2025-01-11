from analytics.settings import db


class DbHelper:
    def booking(skip, limit, count=False):
        query = {}
        projections = {"_id": 1, "bookingId": 1, "bookingStatus": 1, "bookingStatusText": 1, "serviceType": 1,
                       "serviceTypeText": 1, "bookingType": 1, "bookingTypeText": 1, "createdDate": 1,
                       "bookingDate": 1, "bookingDateTimestamp": 1, "bookingDateDevice": 1, "deliveryDate": 1,
                       "deliveryDateTimestamp": 1, "countryId": 1, "countryName": 1, "cityId": 1,
                       "pickupOperationZoneId": 1, "dropOperationZoneId": 1, "cityName": 1, "timeStamp": 1,
                       "driverDetails": 1
                       }
        if count:
            booking_data = db.bookings_rides.find(query, projections).sort("createdDate", -1)
        else:
            booking_data = db.bookings_rides.find(query, projections).sort("createdDate", -1).skip(skip).limit(limit)
        return booking_data


    def driver_performance(store_id, skip, limit, sort_by):
        query = {}
        if store_id: query["storeId"] = store_id
        projection = {"_id": 1, "jobId": 1, "parentOrderId": 1, "orderId": 1, "packageId": 1, "masterOrderId": 1,
                      "storeOrderId": 1, "status": 1, "customerId": 1, "customerDetails": 1, "storeType": 1,
                      "storeTypeMsg": 1, "createdBy": 1, "bookingType": 1, "bookingTypeMsg": 1, "orderType": 1,
                      "orderTypeMsg": 1, "customerPaymentType": 1, "customerPaymentTypeMsg": 1, "deliveryFeePaidBy": 1,
                      "deliveryFeePaidByText": 1, "driverTypeId": 1, "driverType": 1, "shopPickerAndPackerBy": 1,
                      "shopPickerAndPackerByText": 1, "autoDispatch": 1, "storeId": 1, "storeName": 1, "storeLogo": 1,
                      "billingAddress": 1, "pickupAddress": 1, "deliveryAddress": 1, "slotId": 1, "slotDetails": 1,
                      "shippingLabel": 1, "bags": 1, "timestamps": 1, "timings": 1, "distanceTravelled": 1,
                      "driverDetails": 1, "vehicleDetails": 1, }
        sort_by = ""
        data = db.driverJobs.find(query, projection).sort(sort_by, -1).skip(skip).limit(limit)
        return data