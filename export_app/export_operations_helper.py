from rest_framework import status
from sqlalchemy import column
from analytics.settings import UTC, db, \
    AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, S3_IMAGE_BUCKET, S3_IMAGE_PATH, \
    S3_REGION, IDENTITY_POOL_ID, SERVICE_PROVIDER_NAME, AWS_ARN_NAME, \
    GOOGLE_BUCKET_NAME, GOOGLE_IMAGE_LINK, UPLOAD_ON, google_client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import boto3
from .export_response_helper import ExportResponses
import pytz
from bson import ObjectId
import warnings
from dateutil import tz
import traceback
from analytics.settings import UTC, db, ECOMMERCE_SERVICE as SERVICE_SUPPORT, PLATFORM_SUPPORT
import xlsxwriter
import re

date_format = '%d-%m-%Y %H:%M:%S %p'

warnings.filterwarnings("ignore")

ExportResponses = ExportResponses()


class ExportOperations:
    def aws(self, service, file_name, file):

        s3 = boto3.resource('s3',
                            aws_access_key_id=AWS_ACCESS_KEY,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        print("file uploading in AWS..", file)
        f = open(file, 'rb')
        s3_suffix_path = "/".join(["analyticsExcel", service, file_name])
        print("s3_suffix_path: ", s3_suffix_path)
        extra_args = {'CacheControl': 'max-age=86400'}
        print("AWS_ARN_NAME--->", AWS_ARN_NAME)
        print("SERVICE_PROVIDER_NAME--->", SERVICE_PROVIDER_NAME)
        print("IDENTITY_POOL_ID--->", IDENTITY_POOL_ID)
        print("S3_REGION--->", S3_REGION)
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

    def google(self, service, file_name, file):
        print("file uploading in Google Cloud..")
        # google_client = storage.Client.from_service_account_json(json_credentials_path=GOOGLE_CRED)
        bucket = google_client.get_bucket(GOOGLE_BUCKET_NAME)
        google_sub_path = "excelExport/" + "{}/".format(service) + file_name
        object_name_in_gcs_bucket = bucket.blob(google_sub_path)
        object_name_in_gcs_bucket.upload_from_filename(file)
        file_url = GOOGLE_IMAGE_LINK + google_sub_path
        os.remove(file)
        print("Completed..")
        return file_url

    def buyer(self, request):
        try:
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            store_id = request.data["storeId"]
            # ---------------------- time stamp ----------------------
            if "start_time" not in request.data or "end_time" not in request.data:
                response = {
                    "message": "mandatory file missing, 'start_time' or 'end_time'"}
                return ExportResponses.get_status_400(response)
            try:
                start_time = int(request.data.get("start_time"))
                end_time = int(request.data.get("end_time"))
            except:
                response = {"message": "Incorrect Time stamp"}
                return ExportResponses.get_status_422(response)

            # -----------------------------------------------------------------------------------------
            query = {"isRegisteredCustomer": 1, "status": 1}
            # if start_time: query["registeredOn"] = {"$gte": start_time}
            if start_time and end_time:
                query["registeredOn"] = {"$gte": start_time, "$lte": end_time}
            projection = {"firstName": 1, "lastName": 1, "email": 1,
                          "countryCode": 1, "mobile": 1, "city": 1,
                          "customerTypeText": 1
                          }
            print("Buyer query", query)
            response_data = db["customer"].find(query, projection)
            # -----------------------------------------------------------------------------------------
            # while limiting < count:
            #     api_payload["skip"] = limiting
            #     api_payload["limit"] = 100
            #     response = requests.post(api_url, headers=headers, json=api_payload)
            #     if response.status_code == 200:
            #         _data = json.loads(response.content.decode('utf-8'))
            #         response_data.extend(_data["data"])
            #     limiting += 100
            excel_df = pd.DataFrame()
            if not response_data.count():
                return ExportResponses.get_status_204()
            response_df = pd.DataFrame(response_data)
            excel_df["Name"] = response_df["firstName"].astype(
                str) + response_df["lastName"].astype(str)
            excel_df["Customer Type"] = response_df["customerTypeText"]
            excel_df["Email"] = response_df["email"]
            # excel_df["Mobile"] = response_df["mobile"]
            excel_df["Mobile"] = response_df[["countryCode", "mobile"]].apply(
                lambda x: "-".join([str(x[0]), str(x[1])]) if x[1].strip() else "-", axis=1)
            excel_df["City"] = response_df["city"]
            file_url = self.file(df=excel_df, service=type_msg, _type=_type, type_msg=type_msg,
                                 start_time=start_time, end_time=end_time, store_id=store_id, request=request.data,
                                 platform=int(request.GET.get("platform", 1)))
            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def inventory(self, request):

        try:
            print("REQUEST INVENTORY  --->", request.data)
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            # ---------------------- time stamp ----------------------
            if "start_time" not in request.data or "end_time" not in request.data:
                response = {
                    "message": "mandatory file missing, 'start_time' or 'end_time'"}
                return ExportResponses.get_status_400(response)
            try:
                start_time = int(request.data.get("start_time"))
                end_time = int(request.data.get("end_time"))
            except:
                response = {"message": "Incorrect Time stamp"}
                return ExportResponses.get_status_422(response)
            # --------------------------------------------------------
            product_data = []
            language = request.META["HTTP_LANGUAGE"] if "HTTP_LANGUAGE" in request.META else "en"
            print("language", language)
            print(request.data)
            store_id = request.data['storeId'] if "storeId" in request.data else "0"
            from_data = 0
            to_data = 1000
            status = request.data['status']
            inventory_type_data = 0
            search_type = int(
                request.META["HTTP_TYPE"]) if "HTTP_TYPE" in request.META else 1
            product_for = [0, 1, 2]
            mongo_query = {"status": int(status)}
            if store_id == "0":
                mongo_query['storeId'] = str(store_id)
            else:
                mongo_query['storeId'] = ObjectId(store_id)
            if int(status) != 5:
                res = db.childProducts.find(mongo_query).sort(
                    [("_id", -1)]).skip(int(from_data)).limit(int(to_data))
                res_count = db.childProducts.find(mongo_query).count()
            else:
                res = db.deleteChildProducts.find(mongo_query).sort(
                    [("_id", -1)])  # .skip(int(from_data)).limit(int(to_data))
                res_count = db.deleteChildProducts.find(mongo_query).count()
            print("res_count  --------->", res_count)
            if res.count() == 0:
                response = {
                    "inventoryData": product_data,
                    "total_count": res_count,
                    "message": "Data not found"
                }
                return ExportResponses.get_status_404(response)
            res = list(res)
            # if res_count > 1000:
            #     from_data = 1000
            #     while from_data < res_count:
            #         print(from_data)
            #         if int(status) != 5:
            #             new_res = db.childProducts.find(mongo_query).sort([("_id", -1)]).skip(int(from_data)).limit(
            #                 int(to_data))
            #         else:
            #             new_res = db.deleteChildProducts.find(mongo_query).sort([("_id", -1)]).skip(
            #                 int(from_data)).limit(
            #                 int(to_data))
            #         new_res = list(new_res)
            #         res.extend(new_res)
            #         from_data += 1000

            for child_product_details in res:
                if "units" in child_product_details:
                    try:
                        unit_details = child_product_details['units'][0]
                    except:
                        unit_details = child_product_details['units']
                    unit_id = unit_details['unitId']
                    try:
                        current_qty = unit_details['availableQuantity']
                    except:
                        current_qty = 0
                    barcode = unit_details['barcode'] if 'barcode' in unit_details else ""
                    try:
                        inventory_type = unit_details['seller'][
                            'inventoryType'] if 'inventoryType' in unit_details['seller'] else "1"
                    except:
                        inventory_type = "1"
                    sku = unit_details['sku'] if 'sku' in unit_details else ""
                    batch_id = ""
                    expiredStock = 0
                    status = "Active"
                    upc = unit_details['upc'] if 'upc' in unit_details else ""
                    # ======================================mou data===========================================
                    if child_product_details['b2cbulkPackingEnabled'] == 0:
                        if "en" in child_product_details['b2cunitPackageType']:
                            mou_data = child_product_details['b2cunitPackageType']['en']
                        else:
                            mou_data = ""
                    else:
                        if "en" in child_product_details['b2cpackingPackageType']:
                            package_type = child_product_details['b2cpackingPackageType']['en']
                        else:
                            package_type = ""
                        mou_data = package_type
                    if current_qty != 0 and current_qty != "":
                        outOfStock = True
                    else:
                        outOfStock = False
                    # =================================variant data=========================================================
                    attr_list_data = []
                    try:
                        for attr in unit_details['attributes']:
                            if "attrlist" in attr:
                                for attr_list in attr['attrlist']:
                                    try:
                                        if "linkedtounit" in attr_list:
                                            if attr_list['linkedtounit'] == 1:
                                                attr_list_data.append(
                                                    {"attrname": attr_list['attrname'],
                                                     "value": attr_list['value'],
                                                     "measurementUnit": attr_list['measurementUnit']
                                                     if "measurementUnit" in attr_list else "", })
                                    except:
                                        pass
                    except:
                        pass
                    # if len(attr_list_data) == 0:
                    if "colorName" in unit_details:
                        if unit_details['colorName'] != "":
                            attr_list_data.append({
                                "attrname": {"en": "Color"},
                                "value": {"en": unit_details['colorName']},
                                "measurementUnit": "",
                            })
                        else:
                            pass
                    else:
                        pass

                    if "unitSizeGroupValue" in unit_details:
                        if unit_details['unitSizeGroupValue'] == None:
                            pass
                        else:
                            if len(unit_details['unitSizeGroupValue']) != 0:
                                attr_list_data.append({
                                    "attrname": {"en": "Size"},
                                    "value": {"en": unit_details['unitSizeGroupValue']['en']},
                                    "measurementUnit": "",
                                })
                            else:
                                pass
                    else:
                        pass

                    if "inventoryData" in child_product_details:
                        inventory_count = len(
                            child_product_details['inventoryData'])
                    else:
                        inventory_count = 0

                    in_transit_stock = 0
                    default_stock = 0
                    if child_product_details is not None:
                        if "intransitStock" in child_product_details:
                            for intras in child_product_details['intransitStock']:
                                in_transit_stock = in_transit_stock + \
                                                   int(intras['qty'])
                        else:
                            pass
                        if "inventoryData" in child_product_details:
                            for inv_data in child_product_details["inventoryData"]:
                                if inv_data["batchId"] == "DEFAULT":
                                    default_stock = inv_data["availableQuantity"]
                                else:
                                    pass
                        else:
                            pass

                    product_data.append({
                        "id": str(child_product_details['_id']),
                        "productName": child_product_details['pName'][language] if "pName" in child_product_details else
                        child_product_details['pPName']['en'],
                        "categoryName": "N/A",
                        "subCategoryName": "N/A",
                        "subSubCategoryName": "N/A",
                        "linkedtoUnit": attr_list_data,
                        "unitName": unit_details['unitName'][language],
                        "batchDetails": child_product_details[
                            'batchDetails'] if "batchDetails" in child_product_details else False,
                        "expiryDateMandatory": child_product_details[
                            'expiryDateMandatory'] if "expiryDateMandatory" in child_product_details else False,
                        "currentQty": current_qty,
                        "inventoryCount": inventory_count,
                        "inTransitStock": in_transit_stock,
                        "barcode": barcode,
                        "defaultQty": default_stock,
                        "mouData": mou_data,
                        "inventoryType": inventory_type,
                        "sku": sku,
                        "batchId": batch_id,
                        "expiredStock": expiredStock,
                        "status": status,
                        "outOfStock": outOfStock,
                        "upc": upc,
                        "unitId": unit_id
                    })

            # ----------------------------- Excel Transformation ----------------------------------------
            product_data = pd.DataFrame(product_data)
            excel_df = pd.DataFrame()
            excel_df["PRODUCT NAME"] = product_data["productName"]
            excel_df["VARIANT NAME"] = product_data["unitName"]

            # --------------------- VARIANT CONFIG ----------------------
            def string_process(_list):
                try:
                    if isinstance(_list, list) and _list:
                        return "; ".join(
                            ["{}: {}".format(x.get("attrname").get(language), x.get("value").get(language)) for x in
                             _list]).strip()
                    else:
                        return ""
                except:
                    return ""

            # --------------------- VARIANT CONFIG ----------------------
            excel_df["VARIANT CONFIG"] = product_data['linkedtoUnit'].apply(
                string_process)
            excel_df["UPC"] = product_data["upc"]
            excel_df["AVAILABLE STOCK"] = product_data["currentQty"]
            excel_df["IN-TRANSIT STOCK"] = product_data["inTransitStock"]
            excel_df["STATUS"] = product_data["outOfStock"].apply(
                lambda x: "In Stock" if x else "Out of Stock")
            # -------------------------------------------------------------------------------------------
            file_url = self.file(df=excel_df, service=type_msg, _type=_type, type_msg=type_msg,
                                 start_time=start_time, end_time=end_time, store_id=store_id, request=request.data,
                                 platform=int(request.GET.get("platform", 1)))
            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)
            # -------------------------------------------------------------------------------------------
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def promo_code_for(self, _dict):
        promo_code_for = _dict["promo_code_for"]
        service_type = _dict["service_type"]
        if service_type == 2:
            return {1: "Vehicle Type", 3: "Customer"}.get(promo_code_for.get("type", 1))
        elif service_type == 1:
            return {1: "Categories", 2: "Products", 3: "Customer"}.get(promo_code_for.get("type", 1))

    def promo(self, request):
        try:
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            service_validation_list = [0, 1, 2, 3]
            # check service type
            if "service_type" not in request.data.keys():
                response = {"message": "Service type is missing"}
                return ExportResponses.get_status_400(response)

            try:
                time_zone = request.data["timezone"]
                time_zone = pytz.timezone(time_zone)
            except:
                response = {"message": "time zone is missing/incorrect"}
                return ExportResponses.get_status_422(response)

            service_type_validation = (not isinstance(int(request.data["service_type"]), int)) or (
                    int(request.data["service_type"]) not in service_validation_list)
            if service_type_validation:
                response = {"message": "Service Type value is either string or invalid.",
                            "value": "service_type"}
                return ExportResponses.get_status_422(response)
            service_type = int(request.data["service_type"])

            # store id
            store_id = request.data.get('storeId', "")
            if store_id == "0":
                store_id = ""

            store_category_id = request.data.get('storeCategoryId', "")
            # if store_category_id == "0": store_category_id = ""

            search = request.data.get('search', "").strip()

            if "start_time" not in request.data.keys() or "end_time" not in request.data.keys():
                response = {
                    "message": "mandatory file missing, 'start_time' or 'end_time'"}
                return ExportResponses.get_status_400(response)
            try:
                start_time = int(request.data.get("start_time"))
                end_time = int(request.data.get("end_time"))
            except:
                response = {"message": "Incorrect Time Stamp"}
                return ExportResponses.get_status_422(response)

            # query
            query = {"time_stamp": {"$gte": start_time, "$lte": end_time}}
            if service_type:
                query["service_type"] = service_type
            if store_id:
                query["products.products.store_id"] = store_id
            if store_category_id:
                query["promo_discount_details.promo_applicable_categories.categoryId"] = store_category_id
            if search:
                query["promo_name"] = {
                    "$regex": "^" + search + "$", "$options": "i"}
                query["promo_code"] = {
                    "$regex": "^" + search + "$", "$options": "i"}

            log = db.promo_consumption_history.find(
                query).sort("promo_create_time", -1)
            if not log.count():
                return ExportResponses.get_status_204()
            _list = []
            for per_promo in log:
                service_type = per_promo.get("service_type")
                _dict = {
                    "currencySymbol": per_promo.get('currency_details').get("current_symbol"),
                    "currencyCode": per_promo.get('currency_details').get("currency_name"),
                    'user_id': per_promo.get('user_id'),
                    'user_name': per_promo.get('user_name'),
                    'user_email': per_promo.get('email_id'),
                    'user_mobile': per_promo.get('user_mobile'),
                    'promo_name': per_promo.get('promo_name'),
                    'promo_code': per_promo.get('promo_code'),
                    'total_purchase_value': per_promo.get('total_purchase_value'),
                    'email_id': per_promo.get('email_id'),
                    'time_stamp': per_promo.get('time_stamp'),
                    'discount_provided': per_promo.get('discount_details').get('discount_provided'),
                    "service_type": service_type,
                    "promo_usage_count": per_promo.get("promo_usage_count"),
                    "applied_on": per_promo.get("applied_on"),
                    "applied_on_amount": per_promo.get("applied_on_amount"),
                    "transaction_id": per_promo.get("transaction_id"),
                    "products": per_promo.get("products").get("products", []),
                    "accounting": per_promo.get("accounting", {}),
                    "transaction_status": per_promo.get("transaction_status", {}),
                    "promo_discount_details": per_promo.get("promo_discount_details", {}),
                    "region_applied_on": per_promo.get("region_applied_on"),
                    "city_name": per_promo.get("region_applied_on").get("city"),
                    "order_id": per_promo.get("order_id") if service_type == 1 else per_promo.get("cart_id"),
                    "store_order_id": list(filter(lambda x: x.get("storeId") == store_id,
                                                  per_promo.get("store_order_id"))) if store_id else per_promo.get(
                        "store_order_id") if service_type == 1 else per_promo.get("cart_id"),
                    "is_visible": per_promo.get("is_visible", 1),
                    "promo_code_for": per_promo.get("promo_code_for", {})

                }
                _list.append(_dict)
            promo_df = pd.DataFrame(_list)
            df = pd.DataFrame()

            df["COUPON CODE"] = promo_df["promo_code"]
            df["CITY"] = promo_df["city_name"]

            df["SERVICE CATEGORY"] = promo_df["service_type"]
            df[~df["SERVICE CATEGORY"].isin(
                [1, 2, 3])]["SERVICE CATEGORY"] = "Other or All"
            df["SERVICE CATEGORY"] = df["SERVICE CATEGORY"].replace(
                {1: "Delivery", 2: "Ride", 3: "Service"})

            df["STORE CATEGORY"] = promo_df["promo_discount_details"].apply(
                lambda x: x.get("promo_applicable_categories").get("categoryName", "N/A"))

            df["C. ORDER / BOOKING ID"] = promo_df["order_id"]
            df["C. ORDER / BOOKING ID"] = df["C. ORDER / BOOKING ID"].fillna(
                "N/A")

            df["DATE AND TIME"] = promo_df["time_stamp"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))

            df["CUSTOMER NAME"] = promo_df["user_name"]
            df["APPLIED ON"] = promo_df["applied_on"].apply(
                lambda x: {1: "CART", 2: "PRODUCT"}.get(int(x.get("applied_on_status_code"))))
            df[df["SERVICE CATEGORY"] == "Ride"]["APPLIED ON"] = "RIDE BOOKING"

            df["PRICE"] = promo_df["accounting"].apply(
                lambda x: x.get("finalUnitPrice", "N/A"))

            df["CART / BOOKING TOTAL"] = promo_df["accounting"].apply(
                lambda x: x.get("subTotal", "N/A"))

            df["DELIVERY FEE"] = promo_df["accounting"].apply(
                lambda x: x.get("deliveryFee", "N/A"))

            df["PROMO DISCOUNT"] = promo_df["accounting"].apply(
                lambda x: x.get("promoDiscount", "N/A"))

            df["TAXES"] = promo_df["accounting"].apply(
                lambda x: x.get("taxAmount", "N/A"))

            df["OTHER DISCOUNT"] = promo_df["accounting"].apply(
                lambda x: x.get("offerDiscount", "N/A"))

            df["FINAL CART / BOOKING TOTAL"] = promo_df["accounting"].apply(
                lambda x: x.get("finalTotal", "N/A"))

            df["STATUS"] = promo_df["transaction_status"].apply(
                lambda x: x.get("transaction_message", "N/A"))

            df["PROMO-CODE VISIBILITY"] = promo_df["is_visible"].apply(
                lambda x: "Enabled" if x == 1 else "Disabled")

            df["PROMO-CODE FOR"] = promo_df[["promo_code_for",
                                             "service_type"]].apply(self.promo_code_for, axis=1)

            df = df.replace(np.nan, '', regex=True)
            file_url = self.file(df=df, service="promo_logs", _type=_type, type_msg=type_msg,
                                 start_time=start_time, end_time=end_time, store_id=request.data.get(
                    'storeId', "0"),
                                 request=request.data, platform=int(request.GET.get("platform", 1)))
            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def file(self, df, service, _type, type_msg, start_time, end_time, store_id, status=None, pdf_url="", request={},
             store_category_id="", platform=None):
        if service == "Orders" and status == 4:
            # pdf_url = self.pdf_file(df, service, _type, type_msg, start_time, end_time, store_id)
            df = df.drop("totalAmt", axis=1, errors="ignore")
        file_created_at = datetime.today()
        file_created_at_ts = int(file_created_at.timestamp())
        file_name = "{}_{}.xlsx".format(service, str(file_created_at_ts))
        current_path = str(os.getcwd())
        file = "/".join([current_path, file_name])
        print("path: ", file)
        df.to_excel(file, sheet_name=service, index=False)
        upload_on = {1: self.aws,
                     2: self.google
                     }
        file_url = upload_on[UPLOAD_ON](
            service=service.lower().replace(" ", "_"),
            file_name=file_name,
            file=file)

        # ------------------------------ Mongo Insert ------------------------------
        db["analyticsExport"].insert(
            {
                "type": _type,
                "type_msg": type_msg,
                "create_date": file_created_at,
                "create_ts": file_created_at_ts,
                "start_date": datetime.fromtimestamp(start_time),
                "start_ts": start_time,
                "end_ts": end_time,
                "end_date": datetime.fromtimestamp(end_time),
                "file_name": file_name,
                "url": file_url,
                "pdf_url": pdf_url,
                "store_id": store_id,
                "store_category_id": store_category_id,
                "platform": platform,
                "request": request
            }
        )
        
        return file_url

    def orders(self, request):
        try:
            print("ORDER")
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            store_id = request.data["storeId"]


            # ---------------------- timezone ----------------------
            try:
                time_zone = request.data["timezone"]
                print("time_zone------>", time_zone)
                time_zone = pytz.timezone(time_zone)
            except:
                response = {"message": "time zone is incorrect/missing"}
                return ExportResponses.get_status_422(response)

            # ---------------------- time stamp ----------------------
            try:
                start_time = int(request.data.get("start_time"))
                print("start_time before conversion------->", start_time)
                start_time = datetime.fromtimestamp(start_time).replace(tzinfo=time_zone)
                start_time = int(round(start_time.timestamp())) + 1380
                print("start_time after conversion------->", start_time)
                end_time = int(request.data.get("end_time"))
                print("end_time before conversion------->", end_time)
                end_time = datetime.fromtimestamp(end_time).replace(tzinfo=time_zone)
                end_time = int(round(end_time.timestamp())) + 1380
                print("end_time after conversion------->", end_time)
            except:
                response = {
                    "message": "Missing/Incorrect Time stamp, 'start_time' or 'end_time' must be integer"}
                return ExportResponses.get_status_400(response)
                
            # ---------------------- status ----------------------
            status_support = {1: "New", 2: "Accepted", 4: "Packed", 5: "Ready For Pickup", 6: "In-delivery", 7: "Completed"}

            try:
                status = int(request.data["status"])
                assert status_support[status]
            except:
                response = {"message": "status is incorrect/missing",
                            "support": status_support}
                return ExportResponses.get_status_422(response)

            response_data = []
            # -------------------------------------- Mongo Query --------------------------------------
            # orderType = int(request.data.get("orderType", 0)) if request.data.get("orderType") != "0" else 0
            # storeType = int(request.data.get("storeType", 0)) if request.data.get("storeType") != "0" else 0
            storeCategoryId = request.data.get("storeCategoryId", "") if request.data.get(
                "storeCategoryId") != "0" else ""
            # storeCategoryId = ''
            storeId = request.data.get("storeId", "") if request.data.get(
                "storeId") != "0" else ""
            cityId = request.data.get("cityId", "") if request.data.get(
                "cityId") != "0" else ""

            query = {}
            if cityId:
                query = {"$or": [{"pickupAddress.cityId": cityId},
                                 {"deliveryAddress.cityId": cityId}]}
            # if start_time: query["createdTimeStamp"] = {"$gte": start_time}
            if start_time and end_time:
                query["createdTimeStamp"] = {
                    "$gte": start_time, "$lte": end_time}
            else:
                query = {}

            query["status.status"] = status
            # if storeType: query["sellerType"] = storeType
            if storeCategoryId:
                query["storeCategoryId"] = storeCategoryId
            if storeId:
                query["storeId"] = storeId
            print("Order query", query)
            pdf_url = ""
            # ------------------------------------ For Completed Order Only ------------------------------------
            if status in [4,5,6,7,8,9]:
                projection = {"masterOrderId": 1, "storeOrderId": 1, "childStoreOrderId": 1, "fullFilledByDC": 1,
                              "DCDetails": 1, "storeName": 1, "bags": 1, "createdTimeStamp": 1, "bookingType": 1,
                              "deliverySlotDetails": 1, "requestedForTimeStamp": 1, "customerDetails": 1,
                              "activityTimeline": 1, "timestamps": 1, "packageId": 1, "accounting": 1,
                              "deliveryAddress": 1, "driverDetails": 1, "pickerDetails": 1, "createdBy": 1,
                              "productOrderIds": 1, "pickupAddress": 1, "partnerDetails": 1
                              }
                # status_support = {1: "New", 2: "Accepted", 4: "Packed", 5: "Ready For Pickup", 6: "In-delivery", 7: "Completed"}
                deliveryOrderStatus = {4:1 ,5:2 , 6:3, 7:4}
                query["deliveryType"] = 1
                print("projection---->", projection)
                query["status.status"] = deliveryOrderStatus[status]
                print("querry---->", query)
                response_data = db["deliveryOrder"].find(
                    query, projection).sort([("createdTimeStamp", -1)])
                if not response_data.count():
                    return ExportResponses.get_status_204()
                excel_df = pd.DataFrame()
                response_df = pd.DataFrame(response_data)
                excel_df["C ORDER ID"] = response_df["masterOrderId"]
                excel_df["S ORDER ID"] = response_df["storeOrderId"]
                excel_df["CHILD S ORDER ID"] = response_df["childStoreOrderId"]
                excel_df["FULFILLED BY"] = response_df["fullFilledByDC"].apply(
                    lambda x: "DC" if x else "Store")
                excel_df["Store/DC Name"] = response_df[["fullFilledByDC", "DCDetails", "storeName"]].apply(
                    lambda x: x[1]["name"] if x[0] == 1 else x[2], axis=1)
                excel_df["No of Bags"] = response_df["bags"].apply(
                    lambda x: len(x) if isinstance(x, list) else 0)
                excel_df["DMS ID/Package ID"] = response_df["packageId"]
                excel_df["Value of Items "] = response_df["accounting"].apply(
                    lambda x: x.get("finalTotal"))
                excel_df['AWB'] = response_df['partnerDetails'].apply(
                    lambda x: x['trackingId'] if isinstance(x, dict) and 'trackingId' in x.keys() else '')
                excel_df['Carrier Name'] = response_df['partnerDetails'].apply(
                    lambda x: x['name'] if isinstance(x, dict) and 'name' in x.keys() else '')
                excel_df['Shipped Date'] = response_df['timestamps'].apply(
                    lambda x: datetime.fromtimestamp(x['inDispatch']).replace(tzinfo=UTC).astimezone(
                        time_zone).strftime(date_format)
                    if isinstance(x, dict) and 'inDispatch' in x.keys() and x["inDispatch"] else '')
                excel_df['Delivery Date'] = response_df['timestamps'].apply(
                    lambda x: datetime.fromtimestamp(x['completed']).replace(tzinfo=UTC).astimezone(time_zone).strftime(
                        date_format)
                    if isinstance(x, dict) and 'completed' in x.keys() and x["completed"] else '')
                # excel_df["No of Items"] = response_df["bags"].apply(lambda x: len(x) if isinstance(x, list) else 0)
                excel_df["Ordered On"] = response_df["createdTimeStamp"].apply(
                    lambda x: datetime.utcfromtimestamp(x).replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(
                        "%d-%m-%Y %I:%M %p"))
                excel_df["Customer Name"] = response_df["customerDetails"].apply(
                    lambda x: " ".join([x["firstName"].strip(), x["lastName"].strip()]))
                excel_df["Pickup Address"] = response_df["pickupAddress"].apply(
                    lambda x: x.get("address"))
                excel_df["Delivery Address"] = response_df["deliveryAddress"].apply(
                    lambda x: x.get("fullAddress"))

                # -------------------------- Products ------------------------------
                def product_process(product_pair):
                    product_order_ids = product_pair[0]
                    if product_order_ids:
                        product = product_pair[1]
                        if product["productOrderId"] in product_order_ids:
                            return {"name": product.get(
                                "name"), "sku": product.get("sku"),
                                "accounting": product.get('accounting')}
                    return {"name": "", "sku": "", "accounting": ""}

                store_order_ids = list(response_df["storeOrderId"].unique())
                product_name_df = pd.DataFrame(db["storeOrder"].find(
                    {"storeOrderId": {"$in": store_order_ids}},
                    {"_id": 0, "storeOrderId": 1, "products.productOrderId": 1, "products.name": 1, "products.sku": 1,
                     "products.accounting": 1}))
                product_name_df = product_name_df.explode('products')
                response_df = response_df.merge(
                    product_name_df, how="right", on="storeOrderId")
                response_df['process_product'] = response_df[["productOrderIds", "products"]].apply(product_process,
                                                                                                    axis=1)
                excel_df["Products"] = response_df['process_product'].apply(lambda x: x['name'] if x else "")
                excel_df["SKU"] = response_df['process_product'].apply(lambda x: x['sku'] if x else "")
                excel_df["Total Before Tax"] = response_df['process_product'].apply(
                    lambda x: x['accounting']['taxableAmount'] if isinstance(x['accounting'], dict) else "")
                excel_df["Tax Name with %"] = response_df['process_product'].apply(
                    lambda x: x['accounting']['tax'][0]['taxName'] if isinstance(x['accounting'], dict) and
                                                                      x['accounting']['tax'] else "")
                excel_df["Tax Value"] = response_df['process_product'].apply(
                    lambda x: x['accounting']['tax'][0]['totalValue'] if isinstance(x['accounting'], dict) and
                                                                         x['accounting']['tax'] else "")
                excel_df["Total After Tax"] = response_df['process_product'].apply(
                    lambda x: x['accounting']['finalUnitPrice'] if isinstance(x['accounting'], dict) else "")
                excel_df["App Commission"] = response_df['process_product'].apply(
                    lambda x: x['accounting']['appEarningWithTax'] if isinstance(x['accounting'], dict) else "")
                # -------------------------- Products ------------------------------

                excel_df["Ordered From"] = response_df["createdBy"].apply(
                    lambda x: x.get("deviceTypeText", "-"))
                excel_df["totalAmt"] = response_df["accounting"].apply(
                    lambda x: x.get("finalTotal"))
                pdf_url = ""

            # ------------------------------------  For New and Accepted    ------------------------------------
            else:
                projection = {"masterOrderId": 1, "storeOrderId": 1, "childStoreOrderId": 1, "fullFilledByDC": 1,
                              "DCDetails": 1, "storeName": 1, "bags": 1, "createdTimeStamp": 1, "bookingType": 1,
                              "deliverySlotDetails": 1, "requestedForTimeStamp": 1, "customerDetails": 1,
                              "activityTimeline": 1, "timestamps": 1, "packageId": 1, "accounting": 1,
                              "deliveryAddress": 1, "driverDetails": 1, "pickerDetails": 1, "createdBy": 1,
                              "products.name": 1, "products.sku": 1, "pickupAddress": 1, "partnerDetails": 1,
                              "products.accounting": 1, "products.shippingDetails": 1
                              }

                response_data = db["storeOrder"].find(
                    query, projection).sort([("createdTimeStamp", -1)])
                if not response_data.count():
                    return ExportResponses.get_status_204()
                excel_df = pd.DataFrame()
                response_df = pd.DataFrame(response_data)
                response_df = response_df.explode('products')
                excel_df["C ORDER ID"] = response_df["masterOrderId"]
                excel_df["S ORDER ID"] = response_df["storeOrderId"]
                excel_df["CHILD S ORDER ID"] = response_df["childStoreOrderId"]
                excel_df["FULFILLED BY"] = response_df["fullFilledByDC"].apply(
                    lambda x: "DC" if x else "Store")
                excel_df["Store/DC Name"] = response_df[["fullFilledByDC", "DCDetails", "storeName"]].apply(
                    lambda x: x[1]["name"] if x[0] == 1 else x[2], axis=1)
                # excel_df["No of Bags"] = response_df["bags"].apply(
                #     lambda x: len(x) if isinstance(x, list) else 0)
                # excel_df["DMS ID/Package ID"] = ""
                excel_df["Value of Items "] = response_df["accounting"].apply(
                    lambda x: x.get("finalTotal"))
                # excel_df['AWB'] = response_df['products'].apply(
                #     lambda x: x['shippingDetails']['trackingId'] if isinstance(x,
                #                                                                dict) and 'trackingId' in x.keys() else '')
                # excel_df['Carrier Name'] = response_df['products'].apply(
                #     lambda x: x['shippingDetails']['name'] if isinstance(x, dict) and 'name' in x.keys() else '')
                # excel_df['Shipped Date'] = response_df['timestamps'].apply(
                #     lambda x: datetime.fromtimestamp(x['inDispatch']).replace(tzinfo=UTC).astimezone(
                #         time_zone).strftime(date_format)
                #     if isinstance(x, dict) and 'inDispatch' in x.keys() and x["inDispatch"] else '')
                # excel_df["No of Items"] = response_df["bags"].apply(lambda x: len(x) if isinstance(x, list) else 0)
                excel_df["Ordered On"] = response_df["createdTimeStamp"].apply(
                    lambda x: datetime.utcfromtimestamp(x).replace(tzinfo=pytz.UTC).astimezone(time_zone).strftime(
                        "%d-%m-%Y %I:%M %p"))
                excel_df["Customer Name"] = response_df["customerDetails"].apply(
                    lambda x: " ".join([x["firstName"].strip(), x["lastName"].strip()]))
                excel_df["Pickup Address"] = response_df["pickupAddress"].apply(
                    lambda x: x.get("address"))
                excel_df["Delivery Address"] = response_df["deliveryAddress"].apply(
                    lambda x: x.get("fullAddress"))
                excel_df["Delivery Zone"] = response_df["deliveryAddress"].apply(
                    lambda x: x.get("zoneId"))
                unique_zone_ids = list(excel_df["Delivery Zone"].unique())
                print("unique_zone_ids", unique_zone_ids)

                excel_df["Products"] = response_df['products'].apply(lambda x: x['name'] if x else "")
                excel_df["SKU"] = response_df['products'].apply(lambda x: x['sku'] if x else "")
                excel_df["Total Before Tax"] = response_df['products'].apply(
                    lambda x: x['accounting']['taxableAmount'] if isinstance(x['accounting'], dict) else "")
                excel_df["Tax Name with %"] = response_df['products'].apply(
                    lambda x: x['accounting']['tax'][0]['taxName'] if isinstance(x['accounting'], dict) and
                                                                      x['accounting']['tax'] else "")
                excel_df["Tax Value"] = response_df['products'].apply(
                    lambda x: x['accounting']['tax'][0]['totalValue'] if isinstance(x['accounting'], dict) and
                                                                         x['accounting']['tax'] else "")
                excel_df["Total After Tax"] = response_df['products'].apply(
                    lambda x: x['accounting']['finalUnitPrice'] if isinstance(x['accounting'], dict) else "")
                excel_df["App Comission"] = response_df['products'].apply(
                    lambda x: x['accounting']['appEarningWithTax'] if isinstance(x['accounting'], dict) else "")

                # excel_df["Products"] = response_df["products"].apply(
                #     lambda x: x[0]['name'] if isinstance(x, list) and isinstance(x[0], dict) else "")
                excel_df["Ordered From"] = response_df["createdBy"].apply(
                    lambda x: x.get("deviceTypeText", "-"))

            file_url = self.file(df=excel_df, service=type_msg, _type=_type, type_msg=type_msg,
                                 start_time=start_time, end_time=end_time, store_id=store_id, pdf_url=pdf_url,
                                 request=request.data, store_category_id=storeCategoryId,
                                 platform=int(request.GET.get("platform", 1)))

            data = {'file_url': file_url, "pdf_url": pdf_url}
            return ExportResponses.get_status_200(data=data)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def notify(self, request):
        print("NOTIFY")
        _type = int(request.data.get("type"))
        type_msg = SERVICE_SUPPORT[_type]
        store_id = request.data["storeId"]

        # ---------------------- time stamp ----------------------
        if "start_time" not in request.data or "end_time" not in request.data:
            response = {
                "message": "mandatory file missing, 'start_time' or 'end_time'"}
            return ExportResponses.get_status_400(response)
        try:
            start_time = int(request.data.get("start_time"))
            end_time = int(request.data.get("end_time"))
        except:
            response = {"message": "Incorrect Time stamp"}
            return ExportResponses.get_status_422(response)

        # -----------------------------------------------------------------------------------------
        query = {"timestamp": {"$gte": start_time, "$lte": end_time}}
        product_details = db.productNotify.find(
            query, no_cursor_timeout=True).sort([("_id", -1)])
        count = product_details.count()
        print("COUNT -------->", count)
        total_products = []
        print("START: ", datetime.now())
        for product in product_details:
            try:
                product_data = db.childProducts.find_one(
                    {"_id": ObjectId(product["childProductId"])})
                if product_data is not None:
                    user_details = db.customer.find_one(
                        {"_id": ObjectId(product["userId"])})
                    if user_details is not None:
                        try:
                            dt_object = datetime.fromtimestamp(
                                product['timestamp'])
                        except:
                            dt_object = product['timestamp']
                        address = {
                            "product name": product_data['pName']['en'],
                            "user name": user_details['firstName'],
                            "email": user_details['email'],
                            "mobile": user_details['mobile'],
                            "timestamp": product['timestamp'],
                        }
                        saved_address = db.savedAddress.find(
                            {"userId": str(user_details['_id'])})
                        address_count = 1
                        for saved in saved_address:
                            address['address' +
                                    str(address_count)] = saved['addLine1']
                            address_count = address_count + 1
                        from_zone = tz.gettz('UTC')
                        to_zone = tz.gettz('Asia/Kolkata')
                        date_time1 = (dt_object).strftime(
                            "%Y-%m-%d %H:%M:%S %p")
                        date_time1 = datetime.strptime(
                            date_time1, "%Y-%m-%d %H:%M:%S %p")
                        utc = date_time1.replace(tzinfo=from_zone)
                        central = utc.astimezone(to_zone)
                        address['date'] = (central).strftime(
                            "%Y-%m-%d %H:%M:%S %p")
                        total_products.append(address)
            except:
                pass
            count = count - 1
            print(count)
        res_data_dataframe = pd.DataFrame(total_products)
        res_data_dataframe = res_data_dataframe.drop_duplicates(
            subset=["timestamp", "email"], keep="first")
        print("END: ", datetime.now())
        file_url = self.file(df=res_data_dataframe, service=type_msg, _type=_type, type_msg=type_msg,
                             start_time=start_time, end_time=end_time, store_id=store_id, request=request.data,
                             platform=int(request.GET.get("platform", 1)))
        data = {'file_url': file_url}
        return ExportResponses.get_status_200(data=data)

    def per_stop_report(self, request):
        print("NOTIFY")
        _type = int(request.data.get("type"))
        type_msg = SERVICE_SUPPORT[_type]
        store_id = request.data["storeId"]

        # ---------------------- time stamp ----------------------
        if "start_time" not in request.data or "end_time" not in request.data:
            response = {
                "message": "mandatory file missing, 'start_time' or 'end_time'"}
            return ExportResponses.get_status_400(response)
        try:
            start_time = int(request.data.get("start_time"))
            end_time = int(request.data.get("end_time"))
        except:
            response = {"message": "Incorrect Time stamp"}
            return ExportResponses.get_status_422(response)

        # -----------------------------------------------------------------------------------------
        query = {"timestamp": {"$gte": start_time, "$lte": end_time}}
        product_details = db.productNotify.find(
            query, no_cursor_timeout=True).sort([("_id", -1)])
        count = product_details.count()
        print("COUNT -------->", count)
        total_products = []
        print("START: ", datetime.now())
        for product in product_details:
            try:
                product_data = db.childProducts.find_one(
                    {"_id": ObjectId(product["childProductId"])})
                if product_data is not None:
                    user_details = db.customer.find_one(
                        {"_id": ObjectId(product["userId"])})
                    if user_details is not None:
                        try:
                            dt_object = datetime.fromtimestamp(
                                product['timestamp'])
                        except:
                            dt_object = product['timestamp']
                        address = {
                            "product name": product_data['pName']['en'],
                            "user name": user_details['firstName'],
                            "email": user_details['email'],
                            "mobile": user_details['mobile'],
                            "timestamp": product['timestamp'],
                        }
                        saved_address = db.savedAddress.find(
                            {"userId": str(user_details['_id'])})
                        address_count = 1
                        for saved in saved_address:
                            address['address' +
                                    str(address_count)] = saved['addLine1']
                            address_count = address_count + 1
                        from_zone = tz.gettz('UTC')
                        to_zone = tz.gettz('Asia/Kolkata')
                        date_time1 = (dt_object).strftime(
                            "%Y-%m-%d %H:%M:%S %p")
                        date_time1 = datetime.strptime(
                            date_time1, "%Y-%m-%d %H:%M:%S %p")
                        utc = date_time1.replace(tzinfo=from_zone)
                        central = utc.astimezone(to_zone)
                        address['date'] = (central).strftime(
                            "%Y-%m-%d %H:%M:%S %p")
                        total_products.append(address)
            except:
                pass
            count = count - 1
            print(count)
        res_data_dataframe = pd.DataFrame(total_products)
        res_data_dataframe = res_data_dataframe.drop_duplicates(
            subset=["timestamp", "email"], keep="first")
        print("END: ", datetime.now())
        file_url = self.file(df=res_data_dataframe, service=type_msg, _type=_type, type_msg=type_msg,
                             start_time=start_time, end_time=end_time, store_id=store_id, request=request.data,
                             platform=int(request.GET.get("platform", 1)))
        data = {'file_url': file_url}
        return ExportResponses.get_status_200(data=data)

    def pdf_file(self, df, service, _type, type_msg, start_time, end_time, store_id):
        file_created_at = datetime.today()
        file_created_at_ts = int(file_created_at.timestamp())
        new_df = pd.DataFrame()
        new_df["index"] = df.index
        new_df["index"] = new_df["index"] + 1
        new_df["dateReady_catone"] = df["readyForPickup"].apply(
            lambda x: datetime.strptime(x, '%d-%m-%Y %I:%M %p').strftime("%d/%m/%Y"))
        new_df["dateReady_cattwo"] = df["readyForPickup"].apply(
            lambda x: datetime.strptime(x, '%d-%m-%Y %I:%M %p').strftime("%I:%M %p"))
        new_df["orderId_catone"] = df["CHILD S ORDER ID"]
        new_df["orderId_cattwo"] = df["Delivered by"].apply(lambda x: str(x))
        new_df["origin"] = df["Pickup Address"]
        new_df["destination"] = df["Delivery Address"]
        new_df["delivery_date"] = df["completed"].apply(
            lambda x: datetime.strptime(x, '%d-%m-%Y %I:%M %p').strftime("%d/%m/%Y"))
        new_df["delivery_time"] = df["completed"].apply(
            lambda x: datetime.strptime(x, '%d-%m-%Y %I:%M %p').strftime("%I:%M %p"))
        new_df["refBillingGroup"] = df["totalAmt"].apply(lambda x: round(x, 2))
        total = round(float(df["totalAmt"].sum()), 2)
        current_path = str(os.getcwd())
        # pug = env.get_template('./export_app/pug/indexpg.pug')
        due_date = file_created_at + timedelta(days=15)
        _html = pug.render(
            logo='https://d15k2d11r6t6rl.cloudfront.net/public/users/Integrators/BeeProAgency/659336_641638/logo.png',
            transfer='https://d15k2d11r6t6rl.cloudfront.net/public/users/Integrators/BeeProAgency/659336_641638/Group%20639.png',
            tclink='https://superadmin.allpronow.net/index.php?/Privacypolicy/getPrivacypolicy/1/en',
            invoiceNumber=file_created_at_ts,
            invoiceDate=file_created_at.strftime("%d/%m/%Y"),
            date='{start} To {end}'.format(start=datetime.fromtimestamp(start_time).strftime("%m/%d/%Y"),
                                           end=datetime.fromtimestamp(end_time).strftime("%m/%d/%Y")),
            currencySymbol="$",
            totalInvoiceNumber=total,
            paymentTerms='15 Days',
            paymentDueDate=due_date.strftime("%m/%d/%Y"),
            tableData=new_df.to_dict(orient="records")
        )

        html_file_name = "{}_{}.html".format(service, str(file_created_at_ts))
        html_file = "/".join([current_path, html_file_name])
        Html_file = open(html_file, "w")
        Html_file.write(_html)
        pdf_file_name = "{}_{}.pdf".format(service, str(file_created_at_ts))
        pdf_file = "/".join([current_path, pdf_file_name])
        # pdfkit.from_file(html_file_name, pdf_file)
        extra_args = {'CacheControl': 'max-age=86400'}
        f = open(pdf_file, 'rb')
        s3_suffix_path = "/".join(["analyticsExcel", service, pdf_file_name])
        print("s3_suffix_path: ", s3_suffix_path)
        s3_object = s3.Object(S3_IMAGE_BUCKET, s3_suffix_path).put(
            Body=f, Metadata=extra_args, ACL='public-read')
        f.close()
        file_url = S3_IMAGE_PATH + s3_suffix_path
        # os.remove(pdf_file_name)
        # os.remove(html_file_name)
        print("pdf created")
        print("file_url", file_url)
        return file_url

    def accounting(self, request):
        try:
            print("ACCOUNTING")
            print("Request--->", request.data)
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            store_id = request.data["storeId"]
            
            # ---------------------- time stamp ----------------------
            try:
                start_time = int(request.data.get("start_time", 0))
                end_time = int(request.data.get("end_time", 0))
            except:
                response = {"message": "Missing/Incorrect Time stamp, 'start_time' or 'end_time' must be integer"}
                return ExportResponses.get_status_400(response)

            # ---------------------- timezone ----------------------
            try:
                time_zone = pytz.timezone(request.data["timezone"])
            except:
                response = {"message": "time zone is incorrect/missing"}
                return ExportResponses.get_status_422(response)
            # ---------------------- status ----------------------
            storeId = request.data.get("storeId", "") if request.data.get("storeId") != "0" else ""
            orderType = int(request.data.get("orderType"))
            paymentType = int(request.data.get("paymentType"))
            userType = int(request.data.get("userType"))
            driverId = str(request.data.get("driverId", ""))
            cityId = str(request.data.get("cityId", ""))
            countryId = str(request.data.get("countryId", ""))

            match = {
                "status.status": 7,
                # "orderType": orderType,
                "storeType": {"$nin": [23]},
                # "storeId": storeId,
                # "paymentType": paymentType,
                # "driverDetails.driverId": driverId,
                # "customerDetails.userType": userType,
                # "createdTimeStamp": {"$gte": start_time, "$lte": end_time}
            }
            if start_time or end_time: match["createdTimeStamp"] = {"$gte": start_time, "$lte": end_time}
            if cityId:
                match["$or"] = [{"pickupAddress.cityId": cityId}, {"deliveryAddress.cityId": cityId}]
            elif countryId:
                match["$or"] = [{"pickupAddress.countryId": countryId}, {"deliveryAddress.countryId": countryId}]
            else:
                pass
            if orderType: match["orderType"] = orderType
            if storeId: match["storeId"] = storeId
            if paymentType: match["paymentType"] = paymentType
            if driverId: match["driverDetails.driverId"] = {"$in": [driverId, ObjectId(driverId)]}
            if userType: match["customerDetails.userType"] = userType
            if storeId: match["storeId"] = storeId

            # ----------------------------------------------------
            print("Accounting match --->", match)

            # Mongo Query Operations
            query = [
                {"$match": match},
                {"$lookup": {"from": "masterOrder",
                             "localField": "masterOrderId",
                             "foreignField": "orderId",
                             "as": "masterOrder"}},
                # {"$sort": {"_id": -1}},
            ]
            print("Accounting query --->", query)
            data = pd.DataFrame(db.storeOrder.aggregate(query))
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()
            export_data = pd.DataFrame()
            data = data.sort_values(by="createdTimeStamp", ascending=False)
            export_data["C.ORDER ID"] = data["masterOrderId"]
            export_data["S.ORDER ID"] = data["storeOrderId"]
            export_data["DATE & TIME"] = data["createdTimeStamp"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            export_data["ORDER TYPE"] = data["orderTypeMsg"]
            export_data["CUSTOMER TYPE"] = data["customerDetails"].apply(
                lambda x: {1: "B2C", 2: "B2B"}.get(x["userType"], ""))
            export_data["CUSTOMER NAME"] = data["customerDetails"].apply(
                lambda x: " ".join([x.get("firstName", ""), x.get("lastName", "")]))
            export_data["ORDER NET PRICE"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("subTotal"))]))
            export_data["DELIVERY/SHIPPING FEE"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("deliveryFee"))]))
            export_data["BILLED AMT"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("finalTotal"))]))
            export_data["STORE NAME"] = data["storeName"]
            export_data["STORE PLAN"] = "" if "storePlan" not in data.columns else data["storePlan"].apply(
                lambda x: x.get("name", "") if isinstance(x, dict) else x)
            export_data["STORE EARNINGS"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("storeEarning"))]))
            export_data["APP EARNINGS"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("appEarningWithTax"))]))
            data["driverDetails"] = data["driverDetails"].apply(lambda x: x if isinstance(x, dict) else {})
            export_data["DRIVER NAME"] = data["driverDetails"].apply(
                lambda x: " ".join([x.get("firstName", ""), x.get("lastName", "")]) if isinstance(x, dict) else "")
            export_data["DRIVER PLAN"] = data["driverDetails"].apply(
                lambda x: x.get("driverPlan", {}).get("planName") if x.get("driverId") else "").fillna("")
            export_data["DRIVER EARNINGS"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("driverEarning"))]))
            export_data["PG COMMISSION"] = data["accounting"].apply(
                lambda x: " ".join([str(x.get("currencySymbol")), str(x.get("pgEarning"))]))
            export_data["PAYMENT METHOD"] = data["paymentTypeText"]
            export_data["ORDER STATUS"] = data["status"].apply(lambda x: x["statusText"])
            file_url = self.file(df=export_data, service=type_msg, _type=_type, type_msg=type_msg,
                                 start_time=start_time, end_time=end_time, store_id=store_id, request=request.data,
                                 platform=int(request.GET.get("platform", 1)))
            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    @staticmethod
    def sku_link(row):
        if row['name']:
            name = str(row['name']['en'])
            name = name.replace(" ","%20")
            link = f"https://website.meolaa.com/product/{name}?pid={row['_id']}&cpid={row['productId']}"
            return link
        return ''

    @staticmethod
    def gst_tax(row):
        if row['units']:
            if isinstance(row['units'][0],dict) and 'tax' in row['units'][0].keys() and row['units'][0]['tax'] and row['units'][0]['tax'][0]['taxValue']:
                print()
                print(row['units'][0]['tax'][0])
                return int(row['units'][0]['tax'][0]['taxValue'])
        return 'NA'
    
    @staticmethod
    def price_with_tax(row):
        if row['units']:
            if isinstance(row['units'][0],dict) and 'tax' in row['units'][0].keys() and row['units'][0]['tax'] and row['units'][0]['tax'][0]['taxValue']:
                return (1+(int(row['units'][0]['tax'][0]['taxValue']))/100)*int(row['units'][0]['price']['en'])
            return int(row['units'][0]['price']['en'])
        return 'Null'

    @staticmethod
    def price_without_tax(row):
        if row['units'] and isinstance(row['units'][0],dict):
            return int(row['units'][0]['price']['en'])
        return 'Null'

    def sku_sheet(self, request):
        try:
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            end_time = int(datetime.now().timestamp())
            service = 'SKU_Sheet'
            
            query = [
                {"$match":{"status":1,"storeId":{"$nin":[0,"0"]}}},
                {"$lookup":{"from":"stores","localField":"storeId","foreignField":"_id","as":"storedata"}},
                {"$project":{"units":1,"brandName":1,"productId":1,"childproductid":1,"name":1,'inventoryData':1,"images":1,'detailDescription':1}}
            ]
            print("Accounting query --->", query)
            data = pd.DataFrame(db.childProducts.aggregate(query))
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            data['_id'] = data['_id'].astype('str')

            data = data.dropna(subset=['productId'])
            data.reset_index(drop=True, inplace=True)
            data.fillna('', inplace=True)
            
            data['Name'] = data.name.apply(lambda x: x['en'] if isinstance(x, dict) and 'en' in x.keys() else 'NA')
            data['Discription'] = data.detailDescription.apply(lambda x: re.sub('<.*?>','',x['en']))
            data['MRP'] = data.apply(ExportOperations.price_with_tax,axis=1)
            data['Price (without tax)'] = data.apply(ExportOperations.price_without_tax,axis=1)
            data['Tax (in %)'] = data.apply(ExportOperations.gst_tax, axis=1)
            data['Inventory'] = data.inventoryData.apply(lambda x: x[0]['availableQuantity'] if x and isinstance(x[0],dict) else 'NA')
            data['Image'] = data.images.apply(lambda x: x[0]['small'] if x and isinstance(x[0],dict) else 'NA')
            data['Discount Price'] = data.units.apply(lambda x:x[0]['discountPrice'] if x and isinstance(x,list) and isinstance(x[0],dict) else "")
            data['SKU Name'] = data.units.apply(lambda x:x[0]['sku'] if isinstance(x,list) and isinstance(x[0]['sku'],str) else "")
            data['SKU Link'] = data.apply(ExportOperations.sku_link, axis=1)
            data.drop(columns=['units','productId','childproductid','name','_id','inventoryData','images','detailDescription'], inplace=True)
            data.rename(columns={'brandName':'Brand Name'}, inplace=True)

            file_name = f"{type_msg}_{end_time}.csv"
            data.to_csv(file_name, index=False)
            current_path = str(os.getcwd())
            file = "/".join([current_path, file_name])

            upload_on = {1: self.aws,
                     2: self.google
                     }
            file_url = upload_on[UPLOAD_ON](
                service=service.lower().replace(" ", "_"),
                file_name= file_name,
                file=file)
            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def weekly_payout(self, request):

        try:
            print("weekly_payout")
            print("Request--->", request.data)
            _type = int(request.data.get("type"))
            type_msg = SERVICE_SUPPORT[_type]
            store_id = request.data["storeId"]
            # ---------------------- time stamp ----------------------
            ct = datetime.now()
            ts_now = ct.timestamp()
            ts_week = (ct - timedelta(days=7)).timestamp()
            try:
                start_time = int(request.data.get("start_time", ts_week))
                end_time = int(request.data.get("end_time", ts_now))
            except:
                response = {
                    "message": "Missing/Incorrect Time stamp, 'start_time' or 'end_time' must be integer"}
                return ExportResponses.get_status_400(response)

            # ---------------------- timezone ----------------------
            try:
                time_zone = pytz.timezone(request.data["timezone"])
            except:
                response = {"message": "time zone is incorrect/missing"}
                return ExportResponses.get_status_422(response)
            # ---------------------- status ----------------------
            storeId = request.data.get("storeId", "") if request.data.get(
                "storeId") != "0" else ""
            # orderType = int(request.data.get("orderType"))
            # paymentType = int(request.data.get("paymentType"))
            # userType = int(request.data.get("userType"))
            # driverId = str(request.data.get("driverId", ""))
            # cityId = str(request.data.get("cityId", ""))
            # countryId = str(request.data.get("countryId", ""))

            match = {
                "status.status": 7,
                # "orderType": orderType,
                "storeType": {"$nin": [23]},
                # "storeId": storeId,
                # "paymentType": paymentType,
                # "driverDetails.driverId": driverId,
                # "customerDetails.userType": userType,
                # "createdTimeStamp": {"$gte": start_time, "$lte": end_time}
            }
            if start_time or end_time:
                match["createdTimeStamp"] = {
                    "$gte": start_time, "$lte": end_time}
            if storeId:
                match["storeId"] = storeId

            # ----------------------------------------------------
            print("Accounting match --->", match)

            # Mongo Query Operations
            query = [
                {"$match": match},
                {"$sort": {"_id": -1}},
            ]
            print("Accounting query --->", query)
            data = pd.DataFrame(db.storeOrder.aggregate(query))
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            data = data.sort_values(by="createdTimeStamp", ascending=False)
            account_data = data["accounting"].apply(pd.Series)
            currency_symbol = account_data['currencySymbol'].iloc[0]
            data = pd.concat([data, account_data], axis=1)

            ######### Checking if Merchant Handling fee in data ################

            if "merchantHandlingFeesTotal" in list(data.columns):
                doc = data[['paymentType', "subTotal", "finalTotal", "appEarningCart",
                            "vatStoreEarning", "merchantHandlingFeesTotal"]].groupby(by="paymentType").sum()
                doc["Total Commission/Fee"] = doc['vatStoreEarning'].apply(
                    lambda x: round(x, 2)) + doc['appEarningCart'].apply(lambda x: round(x, 2))
                doc = doc.merge(data[['paymentType', "taxableAmount"]].groupby(by="paymentType")[
                                    'paymentType'].count().reset_index(name="Order Quantity"), on='paymentType',
                                how='left')
                doc.set_index('paymentType', inplace=True)
                doc.rename(index={1: "Card Payment",
                                  2: "Cash Payment"}, inplace=True)
                doc.index.name = "Description"
                doc.rename(
                    columns={'subTotal': "Net Total", "finalTotal": "Order Value", "appEarningCart": "Commission/ Fee",
                             "vatStoreEarning": "Vat (20%)", "merchantHandlingFeesTotal": "Merchant Fee"}, inplace=True)
                doc.loc["Total"] = doc.sum()

                doc[["Net Total", "Order Value", "Commission/ Fee", "Vat (20%)", "Total Commission/Fee",
                     "Merchant Fee"]] = doc[[
                    "Net Total", "Order Value", "Commission/ Fee", "Vat (20%)", "Total Commission/Fee",
                    "Merchant Fee"]].applymap(lambda x: " ".join([currency_symbol, str(round(x, 2))]))
                doc = doc.reset_index(level=0)
                doc = doc[['Description', 'Order Quantity', 'Order Value', 'Commission/ Fee',
                           "Merchant Fee", 'Vat (20%)', 'Total Commission/Fee', 'Net Total']]

            else:
                doc = data[['paymentType', "subTotal", "finalTotal", "appEarningCart",
                            "vatStoreEarning"]].groupby(by="paymentType").sum()

                doc["Total Commission/Fee"] = doc['vatStoreEarning'].apply(
                    lambda x: round(x, 2)) + doc['appEarningCart'].apply(lambda x: round(x, 2))
                doc = doc.merge(data[['paymentType', "taxableAmount"]].groupby(by="paymentType")[
                                    'paymentType'].count().reset_index(name="Order Quantity"), on='paymentType',
                                how='left')
                doc.set_index('paymentType', inplace=True)
                doc.rename(index={1: "Card Payment",
                                  2: "Cash Payment"}, inplace=True)
                doc.index.name = "Description"
                doc.rename(columns={'subTotal': "Net Total", "finalTotal": "Order Value",
                                    "appEarningCart": "Commission/ Fee", "vatStoreEarning": "Vat (20%)"}, inplace=True)
                # doc.rename(columns={'subTotal':"Net Total","finalTotal":"Order Value","appEarningCart":"Commission/ Fee","vatStoreEarning":"Vat (20%)"},inplace=True)
                doc.loc["Total"] = doc.sum()

                doc[["Net Total", "Order Value", "Commission/ Fee", "Vat (20%)", "Total Commission/Fee"]] = doc[[
                    "Net Total", "Order Value", "Commission/ Fee", "Vat (20%)", "Total Commission/Fee"]].applymap(
                    lambda x: " ".join([currency_symbol, str(round(x, 2))]))
                doc = doc.reset_index(level=0)
                doc = doc[['Description', 'Order Quantity', 'Order Value',
                           'Commission/ Fee', 'Vat (20%)', 'Total Commission/Fee', 'Net Total']]

            doc["Order Quantity"] = doc["Order Quantity"].astype('int')
            file_url = self.file(df=doc, service=type_msg, _type=_type, type_msg=type_msg,
                                 start_time=start_time, end_time=end_time, store_id=store_id, request=request.data,
                                 platform=int(request.GET.get("platform", 1)))
            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)
        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def store_invoice(self, request):
        try:
            print("store_invoice_payout")
            # print("Request--->", request.data)
            _type = int(request.data.get("type"))
            service = SERVICE_SUPPORT[_type]
            orderId = request.data["orderId"]

            try:
                storeOrder = str(request.data['store_type'])

            except:
                response = {"message": "Missing store_type key"}
                return ExportResponses.get_status_400(response)

            # Mongo Query Operations
            query = [
                {"$match": {'storeOrderId': orderId}},
                {"$lookup": {
                    'from': 'deliveryOrder',
                    'localField': 'storeOrderId',
                    'foreignField': 'storeOrderId',
                    'as': 'packingDetails'
                }},
                # {"$sort": {"_id": -1}},
            ]

            data = pd.DataFrame(db.storeOrder.aggregate(query))
            print("----------------", data.shape)
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            order_account_data = data["accounting"].apply(pd.Series)

            product_data = data['products'][0]
            product_df = pd.DataFrame(product_data)

            product_df_name = list(product_df['name'])
            single_unit_price = list(product_df['singleUnitPrice'])
            quantity = list(product_df['quantity'])
            quantity_df = pd.DataFrame(quantity)
            quantity_df = quantity_df['value']

            unit_price = pd.DataFrame(single_unit_price)
            unit_price.rename(
                columns={'unitPrice': 'Unit Price'}, inplace=True)
            unit_price = unit_price['Unit Price']

            account_data = product_df["accounting"].apply(pd.Series)

            currency = account_data['currencySymbol'].iloc[0]
            print('currency---->', currency)

            storeEarning = order_account_data['storeEarning'].sum()
            deliveryFee = order_account_data['deliveryFee'].sum()
            tip = order_account_data['tip'].sum()
            serviceFeeTotal = order_account_data['serviceFeeTotal'].sum() if 'serviceFeeTotal' in list(
                order_account_data.columns) else 0
            merchantfee = order_account_data['merchantHandlingFeesTotal'].sum(
            ) if 'merchantHandlingFeesTotal' in list(account_data.columns) else 0
            driverEarning = order_account_data['driverEarning'].sum()
            appEarning = order_account_data['appEarning'].sum()

            account_data.insert(0, "Product Name", product_df_name)
            account_data = pd.concat(
                [unit_price, account_data, quantity_df], axis=1)

            final_data = account_data[['Product Name', 'Unit Price', 'value',
                                       'unitPrice', 'offerDiscount', 'taxAmount', 'subTotal']]
            final_data.rename(
                columns={'Product Name': "Product Details", "value": 'Quantity ', "unitPrice": 'Gross Price',
                         "offerDiscount": "Discount", 'taxAmount': 'Tax', 'subTotal': 'Net'}, inplace=True)

            ##################### Excel Creation #################
            file_created_at = datetime.today()
            file_created_at_ts = int(file_created_at.timestamp())
            file_name = "{}_{}.xlsx".format(service, str(file_created_at_ts))
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:

                workbook = writer.book

                final_data.to_excel(writer, sheet_name="order_invoice",
                                    index=False, startrow=0, startcol=0)
                row = final_data.shape[0] + 3
                worksheet = writer.sheets['order_invoice']
                worksheet.write(row, 0, "Delivery Fee")
                worksheet.write(row + 1, 0, "ASAP Fee")
                worksheet.write(row + 2, 0, "Tip")
                worksheet.write(row + 3, 0, "Service Fee")

                worksheet.write(row, 6, deliveryFee)
                worksheet.write(row + 1, 6, 0)
                worksheet.write(row + 2, 6, tip)
                worksheet.write(row + 3, 6, serviceFeeTotal)
                row += 3
                worksheet.write(row + 1, 0, "Total",
                                workbook.add_format({'bold': True}))
                worksheet.write(row + 2, 0, "Merchant Fee")
                worksheet.write(row + 3, 0, "Store Earnings")
                worksheet.write(row + 4, 0, "Driver Payout")
                worksheet.write(row + 6, 0, "Net App Earnings",
                                workbook.add_format({'bold': True}))

                worksheet.write(row + 2, 6, merchantfee)
                worksheet.write(row + 3, 6, storeEarning)
                worksheet.write(row + 4, 6, driverEarning)
                worksheet.write(row + 6, 6, appEarning,
                                workbook.add_format({'bold': True}))

                worksheet.write(
                    row + 1, 1, '=SUM(B2:B{row})'.format(row=row + 1), workbook.add_format({'bold': True}))
                worksheet.write(
                    row + 1, 2, '=SUM(C2:C{row})'.format(row=row + 1), workbook.add_format({'bold': True}))
                worksheet.write(
                    row + 1, 3, '=SUM(D2:D{row})'.format(row=row + 1), workbook.add_format({'bold': True}))
                worksheet.write(
                    row + 1, 4, '=SUM(E2:E{row})'.format(row=row + 1), workbook.add_format({'bold': True}))
                worksheet.write(
                    row + 1, 5, '=SUM(F2:F{row})'.format(row=row + 1), workbook.add_format({'bold': True}))
                worksheet.write(
                    row + 1, 6, '=SUM(G2:G{row})'.format(row=row + 1), workbook.add_format({'bold': True}))

            # relatedOrderCond = {'parentStoreOrderId': responseObj['parentStoreOrderId'], 'storeOrderId': {'$ne': responseObj['storeOrderId']}}

            # responseObj['relatedOrders'] = list(db['storeOrder'].find(relatedOrderCond))
            current_path = str(os.getcwd())
            file = "/".join([current_path, file_name])
            print("path: ", file)
            upload_on = {1: self.aws,
                         2: self.google
                         }
            file_url = upload_on[UPLOAD_ON](
                service=service.lower().replace(" ", "_"),
                file_name=file_name,
                file=file)

            data = {'file_url': file_url}
            return ExportResponses.get_status_200(data=data)

        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)
