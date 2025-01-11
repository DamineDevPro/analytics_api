from rest_framework import status
from analytics.settings import UTC, db, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, S3_IMAGE_BUCKET, S3_IMAGE_PATH, \
    GOOGLE_BUCKET_NAME, GOOGLE_IMAGE_LINK, GOOGLE_CRED, UPLOAD_ON, \
    S3_REGION, IDENTITY_POOL_ID, SERVICE_PROVIDER_NAME, AWS_ARN_NAME
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as date_check
import os
import boto3
from .export_ride_response_helper import ExportResponses
from .export_ride_db_helper import DbHelper
import pytz
import requests
import json
import sys
from bson import ObjectId
import warnings
from dateutil import tz
from google.cloud import storage


warnings.filterwarnings("ignore")
RIDE_SERVICE_SUPPORT = {6: "Trip Invoice", 7: "Fare Estimate", 8: "Bookings", 9: "Financial Logs",
                        10: "Driver Acceptance Rate"}

ExportResponses = ExportResponses()
DbHelper = DbHelper()

class ExportOperations:

    def aws(self, service, file_name, file):
        print("file uploading in AWS..")
        f = open(file, 'rb')
        s3_suffix_path = "/".join(["analyticsExcel", service, file_name])
        print("s3_suffix_path: ", s3_suffix_path)
        extra_args = {'CacheControl': 'max-age=86400'}
        if AWS_ARN_NAME and SERVICE_PROVIDER_NAME and IDENTITY_POOL_ID and S3_REGION:
            aws_access_key_id, aws_secret_access_key, aws_session_token = self.get_access_token_for_congnito_user()
            s3 = boto3.resource(
                's3',
                region_name=S3_REGION,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )

            s3_object = s3.Object(S3_IMAGE_BUCKET, s3_suffix_path).put(
                Body=f, Metadata=extra_args, ACL='public-read')
        
        else:
            s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
            s3_object = s3.Object(S3_IMAGE_BUCKET, s3_suffix_path).put(Body=f, Metadata=extra_args, ACL='public-read')
        f.close()
        file_url = S3_IMAGE_PATH + s3_suffix_path
        os.remove(file)
        print("Completed..")
        return file_url

    def get_access_token_for_congnito_user(self):
        s3 = boto3.client(
            'cognito-identity',
            region_name=S3_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        response = s3.get_open_id_token_for_developer_identity(
            IdentityPoolId=IDENTITY_POOL_ID,
            Logins={
                SERVICE_PROVIDER_NAME: 'string'
            },
            TokenDuration=120
        )

        role_session_name = "my_session_name_here"
        role_arn = AWS_ARN_NAME

        sts_conn = boto3.client('sts', aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        result = sts_conn.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            WebIdentityToken=response['Token'])

        return result['Credentials']['AccessKeyId'], result['Credentials']['SecretAccessKey'], result['Credentials'][
            'SessionToken']

    def file_upload(self, df, service, _type, type_msg, start_time, end_time, request, platform,
                    file_type=0, file_type_name="",):
        file_created_at = datetime.today()
        file_created_at_ts = int(file_created_at.timestamp())
        file_name = "{}_{}.xlsx".format(service.lower().replace(" ", "_"), str(file_created_at_ts))
        current_path = str(os.getcwd())
        file = "/".join([current_path, file_name])
        print("path: ", file)
        df.to_excel(file, sheet_name=service, index=False)
        upload_on = {1: self.aws,
                     2: self.google
                     }
        file_url = upload_on[UPLOAD_ON](service=service.lower().replace(" ", "_"), file_name=file_name, file=file)
        # ------------------------------ Mongo Insert ------------------------------
        _dict = {
            "type": _type,
            "type_msg": type_msg,
            "create_date": file_created_at,
            "create_ts": file_created_at_ts,
            "start_date": datetime.fromtimestamp(start_time),
            "start_ts": start_time,
            "end_ts": end_time,
            "end_date": datetime.fromtimestamp(end_time),
            "file_name": file_name,
            "excel_url": file_url,
            "platform": platform,
            "request": request
        }
        if request.get("city_id") and _type == 8: _dict["city_id"] = request.get("city_id")
        if file_type:
            _dict["file_type"] = file_type
            _dict["file_type_name"] = file_type_name

        db["analyticsExport"].insert(_dict)
        # --------------------------------------------------------------------------
        return file_url

    def google(self, service, file_name, file):
        print("file uploading in Google Cloud..")
        google_client = storage.Client.from_service_account_json(json_credentials_path=GOOGLE_CRED)
        bucket = google_client.get_bucket(GOOGLE_BUCKET_NAME)
        google_sub_path = "excelExport/" + "{}/".format(service) + file_name
        object_name_in_gcs_bucket = bucket.blob(google_sub_path)
        object_name_in_gcs_bucket.upload_from_filename(file)
        file_url = GOOGLE_IMAGE_LINK + google_sub_path
        os.remove(file)
        print("Completed..")
        return file_url

    def trip_invoice(self, request):
        platform = int(request.data["platform"])
        _type = int(request.data.get("type"))
        type_msg = RIDE_SERVICE_SUPPORT[_type]
        # ---------------------- time stamp ----------------------
        try:
            start_time = int(request.data.get("start_time", 0))
            end_time = int(request.data.get("end_time", 0))
        except:
            response = {"message": "Incorrect Time stamp"}
            return ExportResponses.get_status_422(response)
        # ---------------------- country id ----------------------
        try:
            country_id = str(request.data.get("country_id", ""))
            if country_id:
                assert ObjectId(country_id)
        except:
            response = {"message": "Incorrect country_id"}
            return ExportResponses.get_status_422(response)
        # ---------------------- city id ----------------------
        try:
            city_id = str(request.data.get("city_id", ""))
            if city_id:
                city_id = ObjectId(city_id)
        except:
            response = {"message": "Incorrect city_id"}
            return ExportResponses.get_status_422(response)
        # ---------------------- status ----------------------
        booking_status_support = {0: "both", 4: "Cancelled", 12: "Completed"}
        try:
            booking_status = int(request.data.get("status"))
            assert booking_status_support[booking_status]
        except:
            response = {"message": "Incorrect booking status", "parameter": booking_status_support}
            return ExportResponses.get_status_422(response)
        # ------------------- Time Zone ----------------------------
        try:
            time_zone = request.data["timezone"]
            time_zone = pytz.timezone(time_zone)
        except:
            response = {"message": "time zone is missing/incorrect"}
            return ExportResponses.get_status_422(response)
        # --------------------- Search ----------------------
        search = str(request.data.get("search", ""))
        # --------------------- Offset -----------------
        """
        Timestamp will be converted from provide timezone to utc timezone to fetch a data from database
        respective offset will be subtracted/added as per the timezone with respect to UTC/GMT
        """
        offset_start_time = datetime.utcfromtimestamp(start_time).replace(tzinfo=UTC).astimezone(time_zone)
        offset_start_time = int((offset_start_time - offset_start_time.utcoffset()).astimezone(UTC).timestamp())

        offset_end_time = datetime.utcfromtimestamp(end_time).replace(tzinfo=UTC).astimezone(time_zone)
        offset_end_time = int((offset_end_time - offset_end_time.utcoffset()).astimezone(UTC).timestamp())

        print("start_time", start_time)
        print("offset_start_time", offset_start_time)
        print("end_time", end_time)
        print("offset_end_time", offset_end_time)
        # ------------------ Data Base -----------------

        data = DbHelper.invoice_report(start_time=offset_start_time,
                                       end_time=offset_end_time,
                                       country_id=country_id,
                                       city_id=city_id,
                                       search=search,
                                       booking_status=booking_status)

        date_format = '%d-%m-%Y %I:%M:%S %p'
        if data.count() == 0:
            return ExportResponses.get_status_204()
        data = pd.DataFrame(data)
        export_data = pd.DataFrame()
        export_data["BOOKING ID"] = data["bookingIdStr"].fillna("")
        export_data["BOOKING & BUSINESS TYPE"] = data["bookingTypeText"] + " & " + data["serviceTypeText"]
        export_data["TRIP DATE"] = data["bookingDate"].apply(
            lambda x: x.replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format))
        export_data["CITY"] = data["cityName"]
        export_data["CORPORATE NAME"] = data["instituteName"].fillna("")
        export_data["CUSTOMER"] = data["slaveDetails"].apply(lambda x: x.get("name", ""))
        export_data["DRIVER"] = data["driverDetails"].apply(
            lambda driver: " ".join([driver["firstName"], driver["lastName"]]))
        export_data["BILLED AMOUNT"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("total", 0)) + float(bill.get("tip", 0)))]))

        # ------------------------------------------------------------------------------------
        export_data["Total"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("subTotal", 0)))]))
        export_data["Last Due"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("lastDue", 0)))]))
        export_data["Tip"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("tip", 0)))]))

        export_data["Pay By Cash"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("cashCollected", 0)))]))
        export_data["Pay By Card"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("cardDeduct", 0)))]))
        export_data["Pay By Wallet"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("walletTransaction", 0)))]))

        # ------------------------------------------------------------------------------------

        export_data["invoiceTotal"] = data["invoice"].apply(lambda x: x.get("total", 0))
        export_data["APP COMMISSION"] = data["invoice"].apply(lambda bill: " ".join(
            [bill["currency"], str(float(bill.get("appCom", 0)) - float(bill.get("discount", 0)))]))
        export_data["DRIVER EARNINGS"] = data["invoice"].apply(lambda bill: " ".join(
            [bill["currency"], str(float(bill.get("masEarning", 0)) + float(bill.get("tipMasEarning", 0)))]))
        export_data["REFERRAL EARNINGS"] = data["invoice"].apply(
            lambda bill: " ".join([bill["currency"], str(float(bill.get("masterReferralEarnings", 0)))]))
        export_data["PG EARNINGS"] = data["invoice"].apply(lambda bill: " ".join(
            [bill["currency"], str(float(bill.get("pgCommission", 0)) + float(bill.get("tipPgCommission", 0)))]))
        export_data["PAYMENT METHOD"] = data["paymentTypeText"]
        export_data["STATUS"] = data["bookingStatusText"]
        export_data = export_data.replace(r'^\s*$', np.NaN, regex=True)
        export_data = export_data.fillna("N/A")
        export_data = export_data[export_data.invoiceTotal > 0]
        export_data = export_data.drop("invoiceTotal", axis=1, errors="ignore")
        file_url = self.file_upload(df=export_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=start_time,
                                    end_time=end_time,
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)
        # ----------------------------------------------

    def fare_estimate(self, request):
        platform = int(request.data["platform"])
        _type = int(request.data.get("type"))
        type_msg = RIDE_SERVICE_SUPPORT[_type]
        # ---------------------- time stamp ----------------------
        try:
            start_time = int(request.data.get("start_time", 0))
            end_time = int(request.data.get("end_time", 0))
        except:
            response = {"message": "Incorrect Time stamp"}
            return ExportResponses.get_status_422(response)
        # ---------------------- city id ----------------------
        try:
            city_id = str(request.data.get("city_id", ""))
        except:
            response = {"message": "Incorrect city_id"}
            return ExportResponses.get_status_422(response)
        # ---------------------- vehicle Type ----------------------
        try:
            vehicle_type = str(request.data.get("vehicle_type", ""))
            if vehicle_type:
                vehicle_type = ObjectId(vehicle_type)
        except:
            response = {"message": "Incorrect vehicle_type"}
            return ExportResponses.get_status_422(response)
        # ---------------------- Booking Type ----------------------
        booking_type = int(request.data.get("booking_type"))
        # ------------------- Time Zone ----------------------------
        try:
            time_zone = request.data["timezone"]
            time_zone = pytz.timezone(time_zone)
        except:
            response = {"message": "time zone is missing/incorrect"}
            return ExportResponses.get_status_422(response)
        # --------------------- Search ----------------------
        search = str(request.data.get("search", ""))
        # --------------------- Offset -----------------
        """
        Timestamp will be converted from provide timezone to utc timezone to fetch a data from database
        respective offset will be subtracted/added as per the timezone with respect to UTC/GMT
        """
        offset_start_time = datetime.utcfromtimestamp(start_time).replace(tzinfo=UTC).astimezone(time_zone)
        offset_start_time = int((offset_start_time - offset_start_time.utcoffset()).astimezone(UTC).timestamp())

        offset_end_time = datetime.utcfromtimestamp(end_time).replace(tzinfo=UTC).astimezone(time_zone)
        offset_end_time = int((offset_end_time - offset_end_time.utcoffset()).astimezone(UTC).timestamp())

        print("start_time", start_time)
        print("offset_start_time", offset_start_time)
        print("end_time", end_time)
        print("offset_end_time", offset_end_time)
        # ------------------ Data Base -----------------
        data = DbHelper.fare_report(start_time=offset_start_time,
                                    end_time=offset_end_time,
                                    city_id=city_id,
                                    search=search,
                                    vehicle_type=vehicle_type,
                                    booking_type=booking_type)

        date_format = '%d-%m-%Y %I:%M:%S %p'
        if data.count() == 0:
            return ExportResponses.get_status_204()
        data = pd.DataFrame(data)
        export_data = pd.DataFrame()
        export_data["ESTIMATE ID"] = data["_id"].astype(str)
        export_data["CITY"] = data["cityName"].fillna("N/A")
        export_data["BIZ TYPE"] = data["serviceTypeText"].fillna("N/A")
        export_data["TYPE"] = data["bookingTypeText"].fillna("N/A")
        export_data["CUSTOMER NAME"] = data["uName"].fillna("N/A")
        export_data["VEHICLE TYPE"] = data["vehicleTypeName"].fillna("N/A")
        export_data["REQUESTED AT"] = data["time"].apply(
            lambda x: x.replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format))
        export_data["PICKUP"] = data["pickup"].fillna("N/A")
        export_data["DROP"] = data["drop"].fillna("N/A")
        export_data["DISTANCE"] = data["dis"].fillna("").astype(str) + " " + data["mileageMetricText"].fillna(
            "").astype(str)
        export_data["ETA"] = data["duration"].fillna("").astype(str) + " " + data["durationTxt"].fillna("").astype(str)
        export_data["ESTIMATED FARE"] = data["finalAmount"].fillna("")
        export_data = export_data.replace(r'^\s*$', np.NaN, regex=True)
        export_data = export_data.fillna("N/A")
        file_url = self.file_upload(df=export_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=start_time,
                                    end_time=end_time,
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)

    def completed_booking(self, data, time_zone, date_format, type_msg, _type, start_time, end_time, platform, request
                          ):
        data = pd.DataFrame(data)
        export_data = pd.DataFrame()
        export_data["BOOKING ID"] = data["bookingIdStr"].fillna("")
        export_data["CITY"] = data["cityName"]
        export_data["BOOKING & BUSINESS TYPE"] = data["bookingTypeText"] + " & " + data["serviceTypeText"]
        export_data["DRIVER"] = data["driverDetails"].apply(
            lambda driver: " ".join([driver["firstName"], driver["lastName"]]))
        export_data["CUSTOMER"] = data["slaveDetails"].apply(lambda x: x.get("name", ""))
        export_data["VEHICLE TYPE"] = data["vehicleType"].apply(lambda vehicle_type: vehicle_type["typeName"])
        export_data["REQUESTED AT"] = data["createdDate"].apply(
            lambda _date: _date.replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format))
        export_data["REQUESTED FOR"] = data["bookingDate"].apply(
            lambda _date: _date.replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format))
        export_data["PICKUP ADDRESS"] = data["pickup"].apply(lambda pickup: pickup["address"])

        # ---------------------------------------------------------------------------------
        export_data["Driver's Location to Pickup Distance"] = data["tripStats"].apply(
            lambda stats: stats["arrivedActualDistance"] / 1000
            if stats["arrivedActualDistance"] > 0
            else stats["arrivedActualDistance"])
        export_data["Pickup to Drop Distance"] = data["tripStats"].apply(
            lambda stats: stats["completeActualDistance"] / 1000
            if stats["completeActualDistance"] > 0
            else stats["completeActualDistance"])
        # ---------------------------------------------------------------------------------

        export_data["DROP ADDRESS"] = data["drop"].apply(lambda drop: drop["address"])
        export_data["DROPPED AT"] = data["timeStamp"].apply(
            lambda ts: datetime.utcfromtimestamp(int(ts["journeyComplete"])).replace(tzinfo=pytz.UTC).astimezone(
                time_zone).strftime(date_format))
        export_data["FARE CHARGED"] = data["invoice"].apply(
            lambda invoice: " ".join([invoice["currency"], str(round(float(invoice["estimateFare"]), 2))]))

        export_data["FINAL PAYMENT METHOD"] = data["paymentTypeText"].fillna("")
        export_data["PREFERENCES"] = data["bookingPreferenceText"].apply(
            lambda preferences: " ,".join(
                list(map(lambda preference: preference["title"], preferences))
            )
        )
        export_data = export_data.replace(r'^\s*$', np.NaN, regex=True)
        export_data = export_data.fillna("N/A")

        file_url = self.file_upload(df=export_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=start_time,
                                    end_time=end_time,
                                    file_type=12,
                                    file_type_name="completed",
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)

    def cancelled_booking(self, data, time_zone, date_format, type_msg, _type, start_time, end_time, platform, request
                          ):
        data = pd.DataFrame(data)
        export_data = pd.DataFrame()
        export_data["BOOKING ID"] = data["bookingIdStr"].fillna("")
        export_data["CITY"] = data["cityName"]
        export_data["BOOKING & BUSINESS TYPE"] = data["bookingTypeText"] + " & " + data["serviceTypeText"]
        export_data["DRIVER"] = data["driverDetails"].apply(
            lambda driver: " ".join([driver["firstName"], driver["lastName"]]))
        export_data["CUSTOMER"] = data["slaveDetails"].apply(lambda x: x.get("name", ""))
        export_data["VEHICLE TYPE"] = data["vehicleType"].apply(lambda vehicle_type: vehicle_type["typeName"])

        export_data["CANCELLED BY"] = data["bookingStatusText"].fillna("")
        export_data["CANCELLATION FEE"] = data["invoice"].apply(
            lambda invoice: " ".join([invoice["currency"], str(round(float(invoice["cancelationFee"]), 2))]))
        export_data["CANCELLED AT"] = data["dateAndTime"].apply(
            lambda _date: _date["cancelled"].replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format)
        )
        export_data["REASON"] = data["reason"].fillna("")
        export_data["PREFERENCES"] = data["bookingPreferenceText"].apply(
            lambda preferences: " ,".join(
                list(map(lambda preference: preference["title"], preferences))
            )
        )
        export_data = export_data.replace(r'^\s*$', np.NaN, regex=True)
        export_data = export_data.fillna("N/A")

        file_url = self.file_upload(df=export_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=start_time,
                                    end_time=end_time,
                                    file_type=4,
                                    file_type_name="cancelled",
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)

    def expired_booking(self, data, time_zone, date_format, type_msg, _type, start_time, end_time, platform, request
                        ):
        data = pd.DataFrame(data)
        export_data = pd.DataFrame()
        export_data["BOOKING ID"] = data["bookingIdStr"].fillna("")
        export_data["CITY"] = data["cityName"]
        export_data["BOOKING & BUSINESS TYPE"] = data["bookingTypeText"] + " & " + data["serviceTypeText"]
        export_data["DRIVER"] = data["driverDetails"].apply(
            lambda driver: " ".join([driver["firstName"], driver["lastName"]]))

        export_data["CUSTOMER"] = data["slaveDetails"].apply(lambda user: user.get("name", ""))
        export_data["VEHICLE TYPE"] = data["vehicleType"].apply(lambda vehicle_type: vehicle_type["typeName"])
        export_data["REQUESTED AT"] = data["createdDate"].apply(
            lambda _date: _date.replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format))

        export_data["REQUESTED FOR"] = data["bookingDate"].apply(
            lambda _date: _date.replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(date_format))

        export_data["PICKUP ADDRESS"] = data["pickup"].apply(lambda pickup: pickup["address"])
        export_data["DROP ADDRESS"] = data["drop"].apply(lambda drop: drop["address"])
        export_data["FARE ESTIMATE"] = data["invoice"].apply(
            lambda invoice: " ".join([invoice["currency"], str(round(float(invoice["estimateFare"]), 2))]))

        export_data["PAYMENT METHOD"] = data["paymentTypeText"].astype(str)
        export_data["EXPIRED AT"] = data["timeStamp"].apply(
            lambda ts: datetime.utcfromtimestamp(int(ts["expired"])).replace(tzinfo=pytz.UTC).astimezone(
                time_zone).strftime(date_format))
        export_data["NO OF DRIVERS ATTEMPT"] = data["dispatched"].apply(lambda dispatcher: len(dispatcher))
        export_data["PREFERENCES"] = data["bookingPreferenceText"].apply(
            lambda preferences: " ,".join(
                list(map(lambda preference: preference["title"], preferences))
            )
        )
        export_data = export_data.replace(r'^\s*$', np.NaN, regex=True)
        export_data = export_data.fillna("N/A")

        file_url = self.file_upload(df=export_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=start_time,
                                    end_time=end_time,
                                    file_type=13,
                                    file_type_name="expired",
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)

    def bookings(self, request):
        platform = int(request.data["platform"])
        _type = int(request.data.get("type"))
        type_msg = RIDE_SERVICE_SUPPORT[_type]
        # ---------------------- status ----------------------
        booking_status_support = {13: "Expired", 4: "Cancelled", 12: "Completed"}
        try:
            booking_status = int(request.data.get("status"))
            assert booking_status_support[booking_status]
        except:
            response = {"message": "Incorrect booking status", "parameter": booking_status_support}
            return ExportResponses.get_status_422(response)
        type_msg = " ".join([type_msg, booking_status_support[booking_status].lower()])
        # ---------------------- time stamp ----------------------
        try:
            start_time = int(request.data.get("start_time", 0))
            end_time = int(request.data.get("end_time", 0))
        except:
            response = {"message": "Incorrect Time stamp"}
            return ExportResponses.get_status_422(response)
        # ---------------------- country id ----------------------
        try:
            country_id = str(request.data.get("country_id", ""))
            if country_id:
                assert ObjectId(country_id)
        except:
            response = {"message": "Incorrect country_id"}
            return ExportResponses.get_status_422(response)
        # ---------------------- city id ----------------------
        try:
            city_id = str(request.data.get("city_id", ""))
            if city_id:
                assert ObjectId(city_id)
        except:
            response = {"message": "Incorrect city_id"}
            return ExportResponses.get_status_422(response)
        # ---------------------- vehicle Type ----------------------

        vehicle_type = str(request.data.get("vehicle_type", ""))

        # ---------------------- booking Type ----------------------
        booking_type_support = {0: "All", 1: "Now", 2: "Later"}
        try:
            booking_type = int(request.data.get("booking_type"))
            assert booking_type_support[booking_type]
        except:
            response = {"message": "Incorrect booking type", "parameter": booking_type_support}
            return ExportResponses.get_status_422(response)
        if booking_status == 4:
            # ---------------------- cancel Type ----------------------
            cancel_type_support = {0: "All", 1: "Driver", 2: "Customer"}
            try:
                cancel_type = int(request.data.get("cancel_type"))
                assert cancel_type_support[cancel_type]
            except:
                response = {"message": "Incorrect cancel type", "parameter": cancel_type_support}
                return ExportResponses.get_status_422(response)
        else:
            cancel_type = None
        # ------------------- Time Zone ----------------------------
        try:
            time_zone = request.data["timezone"]
            time_zone = pytz.timezone(time_zone)
        except:
            response = {"message": "time zone is missing/incorrect"}
            return ExportResponses.get_status_422(response)
        # --------------------- Search ----------------------
        search = str(request.data.get("search", ""))
        # --------------------- Offset -----------------
        """
        Timestamp will be converted from provide timezone to utc timezone to fetch a data from database
        respective offset will be subtracted/added as per the timezone with respect to UTC/GMT
        """
        offset_start_time = datetime.utcfromtimestamp(start_time).replace(tzinfo=UTC).astimezone(time_zone)
        offset_start_time = int((offset_start_time - offset_start_time.utcoffset()).astimezone(UTC).timestamp())

        offset_end_time = datetime.utcfromtimestamp(end_time).replace(tzinfo=UTC).astimezone(time_zone)
        offset_end_time = int((offset_end_time - offset_end_time.utcoffset()).astimezone(UTC).timestamp())

        print("start_time", start_time)
        print("offset_start_time", offset_start_time)
        print("end_time", end_time)
        print("offset_end_time", offset_end_time)

        # ---------------------- Data Query ---------------------
        query_parser = {13: DbHelper.expired_booking,
                        4: DbHelper.cancelled_booking,
                        12: DbHelper.completed_booking}

        data = query_parser[booking_status](
            start_time=offset_start_time,
            end_time=offset_end_time,
            city_id=city_id,
            vehicle_type=vehicle_type,
            booking_type=booking_type,
            cancel_type=cancel_type,
            search=search,
            country_id=country_id

        )
        print("data.count() --->", data.count())
        if not data.count():
            return ExportResponses.get_status_204()
        # ----------------- data manipulation ----------------
        date_format = '%d-%m-%Y %I:%M:%S %p'
        data_parser = {13: self.expired_booking,
                       4: self.cancelled_booking,
                       12: self.completed_booking}
        return data_parser[booking_status](data=data,
                                           time_zone=time_zone,
                                           date_format=date_format,
                                           type_msg=type_msg,
                                           _type=_type,
                                           start_time=start_time,
                                           end_time=end_time,
                                           request=request,
                                           platform=platform
                                           )

    def financial_logs(self, request):
        platform = int(request.data["platform"])
        _type = int(request.data.get("type"))
        type_msg = RIDE_SERVICE_SUPPORT[_type]
        # ---------------------- status ----------------------
        payment_type_support = {1: "online", 2: "cash"}
        try:
            payment_type = int(request.data.get("payment_type"))
            assert payment_type_support[payment_type]
        except:
            response = {"message": "Incorrect booking status", "parameter": payment_type_support}
            return ExportResponses.get_status_422(response)
        type_msg = " ".join([type_msg, payment_type_support[payment_type].lower()])
        # ------------------- Time Zone ----------------------------
        try:
            time_zone = request.data["timezone"]
            time_zone = pytz.timezone(time_zone)
        except:
            response = {"message": "time zone is missing/incorrect"}
            return ExportResponses.get_status_422(response)
        # ---------------------- time stamp ----------------------
        try:
            start_time = int(request.data.get("start_time", 0))
            end_time = int(request.data.get("end_time", 0))
        except:
            response = {"message": "Incorrect Time stamp"}
            return ExportResponses.get_status_422(response)

        cql_query = "SELECT * FROM paymentlog WHERE mode = '{payment_type}'".format(
            payment_type={1: "ONLINE", 2: "OFFLINE"}[payment_type])
        if start_time and end_time:
            start_date = (datetime.utcfromtimestamp(start_time) - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = (datetime.utcfromtimestamp(end_time) + timedelta(days=1)).strftime("%Y-%m-%d")
            date_filter = "txntimestamp > '{start_time}' AND txntimestamp < '{end_time}'".format(start_time=start_date,
                                                                                                 end_time=end_date)
            cql_query = " AND ".join([cql_query, date_filter])

        cql_query = cql_query + " ALLOW FILTERING"
        print("cql_query ----->", cql_query)
        rows = wallet_casandra.execute(cql_query)
        data = pd.DataFrame(list(rows))
        if data.shape[0] == 0:
            return ExportResponses.get_status_204()

        export_data = pd.DataFrame()
        export_data["TXN ID"] = data["txnid"]
        export_data["DATE & TIME"] = data["txntimestamp"].apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))
        export_data["ORDER NUMBER"] = data["productid"]
        export_data["PAID FROM"] = data["usertype"]
        export_data["AMOUNT"] = data["amount"]
        export_data["STATUS"] = data["status"]
        export_data["USER’S NAME"] = data["username"]
        export_data["PHONE"] = data["phone"]
        export_data["EMAIL"] = data["email"]
        export_data["TRIGGER"] = data["trigger"]
        export_data = export_data.replace(r'^\s*$', np.NaN, regex=True)
        export_data = export_data.replace("undefined", "N/A")
        export_data = export_data.fillna("N/A")
        # ----------- time filter ---------------------
        st = datetime.utcfromtimestamp(start_time).replace(tzinfo=time_zone)
        ed = datetime.utcfromtimestamp(end_time).replace(tzinfo=time_zone)
        print("st  ---->", st)
        print("ed  ---->", ed)
        export_data = export_data[(export_data["DATE & TIME"] >= st) & (export_data["DATE & TIME"] <= ed)]
        date_format = '%d-%m-%Y %I:%M:%S %p'
        export_data["DATE & TIME"] = export_data["DATE & TIME"].apply(lambda x: x.strftime(date_format))
        file_url = self.file_upload(df=export_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=start_time,
                                    end_time=end_time,
                                    file_type=payment_type,
                                    file_type_name={1: "ONLINE", 2: "OFFLINE"}[payment_type],
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)

    def acceptance_rate(self, request):
        platform = int(request.data["platform"])
        print("request")
        _type = int(request.data.get("type"))
        type_msg = RIDE_SERVICE_SUPPORT[_type]
        search = str(request.data.get("search", ""))
        country_id = str(request.data.get("country_id", ""))
        city_id = str(request.data.get("city_id", ""))
        for id, key in zip([country_id, city_id], ["country_id", "city_id"]):
            if id != "":
                try:
                    assert ObjectId(id)
                except:
                    response = {"message": "unsupported entry", "variable": key}
                    return ExportResponses.get_status_422(response)

        data, count = DbHelper.acceptance_rate(search=search, country_id=country_id, city_id=city_id)
        data = pd.DataFrame(data)
        if count == 0:
            return ExportResponses.get_status_204()
        excel_data = pd.DataFrame()
        excel_data["Name"] = data["firstName"] + " " + data["lastName"]
        excel_data["Name"] = excel_data["Name"].apply(lambda x: x.strip())
        excel_data["email"] = data["email"]
        excel_data["mobile"] = data[["countryCode", "mobile"]].apply(
            lambda x: "-".join([x.get("countryCode"), x.get("mobile")]) if x.get("countryCode") else x.get("mobile"),
            axis=1)
        excel_data["Type"] = data["driverTypeText"]
        excel_data["Country"] = data["countryName"]
        excel_data["City"] = data["cityName"]
        excel_data["Total Bookings"] = data["acceptance"].apply(lambda x: x.get("totalBookings"))
        excel_data["Ignored Bookings"] = data["acceptance"].apply(lambda x: x.get("ignoredBookings"))
        excel_data["Rejected Bookings"] = data["acceptance"].apply(lambda x: x.get("rejectedBookings"))
        excel_data["Cancelled Bookings"] = data["acceptance"].apply(lambda x: x.get("cancelledBookings"))
        excel_data["Accepted Bookings"] = data["acceptance"].apply(lambda x: x.get("acceptedBookings"))
        excel_data["Acceptance Rate"] = data["acceptance"].apply(lambda x: x.get("acceptanceRate"))

        file_url = self.file_upload(df=excel_data,
                                    service=type_msg,
                                    _type=_type,
                                    type_msg=type_msg,
                                    start_time=0,
                                    end_time=0,
                                    request=dict(request.data),
                                    platform=platform
                                    )
        data = {"excel_url": file_url}
        return ExportResponses.get_status_200(data=data)
