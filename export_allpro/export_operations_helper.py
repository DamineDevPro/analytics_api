from analytics.settings import UTC, db, \
    AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, S3_IMAGE_BUCKET, S3_IMAGE_PATH, \
    S3_REGION, IDENTITY_POOL_ID, SERVICE_PROVIDER_NAME, AWS_ARN_NAME,\
    GOOGLE_BUCKET_NAME, GOOGLE_IMAGE_LINK, UPLOAD_ON, google_client
import pandas as pd
import numpy as np
from datetime import datetime
import os
import boto3
from .export_response_helper import ExportResponses
import pytz
from .export_db_helper import DbHelper
import warnings
import traceback
from analytics.settings import UTC, db
import xlsxwriter
from math import ceil

date_format = '%m-%d-%Y %H:%M:%S'

BASE_DIR = str(os.getcwd())

logo = BASE_DIR + "/export_allpro/static/logo.png"
all_pro_logo = BASE_DIR + "/export_allpro/static/allpro_logo.png"

warnings.filterwarnings("ignore")
# SERVICE_SUPPORT = {1: "Buyers", 2: "Inventory", 3: "Promo Code Logs", 4: "Orders", 5: "Notify"}

ExportResponses = ExportResponses()
dbhelper = DbHelper()


class ExportOperations:
    def aws(self, service, file_name, file):
        print("file uploading in AWS..")
        f = open(file, 'rb')
        s3_suffix_path = "/".join(["analyticsExcel", service, file_name])
        print("s3_suffix_path: ", s3_suffix_path)
        extra_args = {'CacheControl': 'max-age=86400'}
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
    
    def mongo_insertion(self,_type,file_created_at,file_created_at_ts,start_time,
                        end_time,customer_id,file_name,file_url, service):
        # ------------------------------ Mongo Insert ------------------------------
        db["truckrInvoice"].insert(
            {
                "type": _type,
                "create_date": file_created_at,
                "create_ts": file_created_at_ts,
                "start_date": datetime.fromtimestamp(start_time),
                "start_ts": start_time,
                "end_ts": end_time,
                "end_date": datetime.fromtimestamp(end_time),
                "file_name": file_name,
                "url": file_url,
                "customer_id": customer_id,
                "service": service
            }
        )

    def per_stop_report(self, request):
        try:
            print(" per_stop_report Report")
            print("Request--->", request.data)
            _type = int(request.data.get("type"))
            # service = SERVICE_SUPPORT[_type]
            service = "minute_man"
            # type_msg = SERVICE_SUPPORT[_type]
            # Orderid = # masterOrderId
            # userid = # userId
            # ---------------------- time stamp ----------------------
            try:
                start_time = int(request.data["start_time"])
                end_time = int(request.data["end_time"])
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

            customer_id = request.data.get("customerId", 0)

            try:
                invoice_id = int(request.data["invoiceId"])
                invoiceDate = int(request.data["invoiceDate"])
                paymentDueOn = int(request.data["paymentDueOn"])

            except:
                response = {
                    "message": "Missing 'invoiceId', 'paymentDueOn' or 'invoiceDate' must be integer"}
                return ExportResponses.get_status_400(response)

            data = dbhelper.per_stop_data(start_time, end_time, customer_id)
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            col = ['S. No.', 'Booking ID', 'Booked On', 'Sender Name',
                   'Vehicle Type', 'Requested Pickup Time',
                   'Pickup Address', 'Drop Time', 'Drop Address',
                   'Load Type', 'Driver Name',
                   'Completed On', 'Total Amount']

            data["Requested Pickup Time"] = np.where(data["Requested Pickup Time"] == 0, data["Booking Date"],
                                                     data["Requested Pickup Time"])

            currencySymbol = data["currencySymbol"].loc[0]

            data.sort_values(by='Requested Pickup Time', ascending=False, inplace=True)
            data["Requested Pickup Time"] = data["Requested Pickup Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booked On"] = data["Booked On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Drop Time"] = data["Drop Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Completed On"] = data["Completed On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))

            data["Booking Date"] = data["Booking Date"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booking Date"] = data["Booking Date"].apply(lambda x: x.split(" ")[0])

            data['S. No.'] = data.reset_index().index + 1
            data = data[col]
            total = round(data["Total Amount"].sum(), 2)

            data.rename(columns={'Total Amount': f'Total Amount ({currencySymbol})'}, inplace=True)

            ##################### Excel Creation #################

            file_created_at = datetime.today()
            file_created_at_ts = int(file_created_at.timestamp())
            file_name = "{}_{}.xlsx".format(service, str(file_created_at_ts))

            row = 0
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                workbook = writer.book

                merge_format_1 = workbook.add_format({
                    'bold': 1,
                    'border': 1,
                    'fg_color': '#ADD8E6',
                    'text_wrap': 1
                })

                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#ADD8E6',
                    'border': 1})

                data.to_excel(writer, sheet_name="minute_man Invoicing Report", index=False, startrow=8, startcol=0)
                worksheet = writer.sheets['minute_man Invoicing Report']
                worksheet.write('G8', 'Invoice Transportation Services',
                                workbook.add_format({'align': 'center', "bold": True, "font_size": 28}))
                worksheet.write('E7', f'Total Invoice Amount= {total}',
                                workbook.add_format({"bold": True, "font_size": 14}))
                worksheet.write('M1', 'All Pro Now Delivery, LLC',
                                workbook.add_format({'align': 'right', "bold": True, "font_size": 14}))
                worksheet.write('M2', '1006 Crocker Road',
                                workbook.add_format({'align': 'right', "bold": True, "font_size": 14}))
                worksheet.write('M3', 'Westlake, Ohio 44145',
                                workbook.add_format({'align': 'right', "bold": True, "font_size": 14}))
                worksheet.write('M4', 'United States',
                                workbook.add_format({'align': 'right', "bold": True, "font_size": 14}))
                worksheet.write('M5', 'Toll free: (833) 961-1099',
                                workbook.add_format({'align': 'right', "bold": True, "font_size": 14}))
                worksheet.write('M6', 'www.allpronow.net',
                                workbook.add_format({'align': 'right', "bold": True, "font_size": 14}))

                worksheet.write('A1', 'Invoice Number', workbook.add_format({"bold": True, "font_size": 14}))
                worksheet.write('A2', invoice_id, workbook.add_format({"bold": True, "font_size": 14}))
                worksheet.write('A3', 'Invoice Date ', workbook.add_format({"bold": True, "font_size": 14}))
                worksheet.write('A4', invoiceDate, workbook.add_format({"bold": True, "font_size": 14}))
                worksheet.write('A5', 'Delivery Dates ', workbook.add_format({"bold": True, "font_size": 14}))
                worksheet.write('A6', paymentDueOn, workbook.add_format({"bold": True, "font_size": 14}))

                for col_num, value in enumerate(data.columns.values):
                    worksheet.write(8, col_num, value, header_format)

                worksheet.insert_image('E1', 'allpro_logo.png', {'x_scale': .25, 'y_scale': .25})
                worksheet.merge_range(f"A{10 + data.shape[0]}:L{10 + data.shape[0]}", 'Total', merge_format_1)
                #     worksheet.write(f'A{10+data.shape[0]}', 'Total',workbook.add_format({"bold":True,"font_size":14}))
                worksheet.write(f'M{10 + data.shape[0]}', total,
                                workbook.add_format({"bold": True, "font_size": 14, "bg_color": "#ADD8E6"}))
                border_fmt = workbook.add_format({'bottom': 2, 'top': 2, 'left': 2, 'right': 2})
                worksheet.conditional_format(
                    xlsxwriter.utility.xl_range(9 + data.shape[0], 0, 9 + data.shape[0], len(data.columns) - 1),
                    {'type': 'no_errors', 'format': border_fmt})

            current_path = str(os.getcwd())
            file = "/".join([current_path, file_name])
            print("path: ", file)
            upload_on = {1: self.aws,
                         2: self.google
                         }

            file_url = upload_on[UPLOAD_ON](
                service=service.lower().replace(" ", "_"),
                file_name=file_name,
                file=file
            )

            try:
                self.mongo_insertion(_type,file_created_at,file_created_at_ts,start_time,
                        end_time,customer_id,file_name,file_url,service)

                data = {'file_url': file_url}
                return ExportResponses.get_status_200(data=data)
            except Exception as ex:
                return ExportResponses.get_status_500(ex)

        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def mileage_stop_off(self, request):
        try:
            print(" Mileage+stop-off Report")
            print("Request--->", request.data)
            _type = int(request.data.get("type"))
            # service = SERVICE_SUPPORT[_type]
            service = "ced_sprinter_van"
            # type_msg = SERVICE_SUPPORT[_type]
            # ---------------------- time stamp ----------------------
            try:
                start_time = int(request.data["start_time"])
                end_time = int(request.data["end_time"])
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

            customer_id = request.data.get("customerId", 0)

            data = dbhelper.mileage_stop_data(start_time, end_time, customer_id)
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            data["Requested Pickup Time"] = np.where(data["Requested Pickup Time"] == 0, data["Booking Date"],
                                                     data["Requested Pickup Time"])

            currencySymbol = data["currencySymbol"].loc[0]

            data.sort_values(by='Requested Pickup Time', ascending=False, inplace=True)
            data["Requested Pickup Time"] = np.where(data["Requested Pickup Time"] == 0, data["Booking Date"],
                                                     data["Requested Pickup Time"])
            data.sort_values(by='Requested Pickup Time', ascending=False, inplace=True)
            data["Requested Pickup Time"] = data["Requested Pickup Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booked On"] = data["Booked On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Drop Time"] = data["Drop Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Completed On"] = data["Completed On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booking Date"] = data["Booking Date"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booking Date"] = data["Booking Date"].apply(lambda x: x.split(" ")[0])
            data[["Free Mileage"]].fillna(0, inplace=True)
            data["Stop Fee per stop"].fillna(0, inplace=True)
            data["Miles"].fillna(0, inplace=True)
            data["Base Fee"].fillna(0, inplace=True)
            # data["Stop Fee Applied"].fillna(0, inplace=True)
            data["Mileage Price"].fillna(0, inplace=True)
            data["Total time (in min)"] = round(data["Total time (in min)"] / 60, 3)

            col = ['Booking ID', "Booked On", 'Sender Name',
                   'Vehicle Type', 'Requested Pickup Time', 'Pickup Address', 'Drop Time',
                   'Drop Address', 'Load Type', 'Amount', 'Driver Name',
                   'Completed On', 'Booking Date', 'Miles', "Stop Fee per stop", 'Free Mileage',
                   'Base Fee', "Mileage Price", 'Stop Offs', 'Total time (in min)', 'Free Delivery Minutes',
                   'Time Fee per min', 'Stop Off Charge', "Base + Mileage Charge"]

            col1 = ['Booking ID', "Booked On", 'Sender Name', 'Vehicle Type',
                    'Requested Pickup Time', 'Pickup Address', 'Drop Time', 'Drop Address',
                    'Load Type', 'Amount', 'Driver Name', 'Completed On', 'Miles',
                    'Stop Offs', "Stop Fee per stop", 'Free Mileage', 'Base Fee',
                    "Mileage Price", 'Total time (in min)', 'Free Delivery Minutes', 'Time Fee per min',
                    'Stop Off Charge', 'Base + Mileage Charge', ]

            currency_addon = {
                'Stop Fee per stop': f'Stop Fee per stop ({currencySymbol})',
                'Base Fee': f'Base Fee ({currencySymbol})',
                'Mileage Price': f'Mileage Price ({currencySymbol})',
                'Stop Off Charge': f'Stop Off Charge ({currencySymbol})',
                'Base + Mileage Charge': f'Base + Mileage Charge ({currencySymbol})',
                'Total Amount': f'Total Amount ({currencySymbol})',
                'Time Fee per min.': f'Time Fee per min. ({currencySymbol})',
            }

            final_df = pd.DataFrame(columns=col)
            for key, dataframe in data.groupby("Booking Date"):
                if key == "":
                    for i in range(dataframe.shape[0]):
                        df1 = pd.DataFrame(dataframe.iloc[i])
                        df1 = df1.T
                        df1["Stop Offs"] = 0
                        df1.drop(
                            columns=["_id", "parentOrderId", 'Stop Fee per stop', 'Free Mileage',
                                     'Base Fee', 'Mileage Price'], axis=1, inplace=True)
                        df1.loc[df1.shape[0]] = np.nan
                        final_df = pd.concat([df1, final_df], axis=0)
                        final_df = final_df[col1]

                else:
                    dataframe["Miles"] = dataframe["Miles"].sum()
                    dataframe["Stop Offs"] = dataframe.shape[0] - 1
                    dataframe.drop(columns=["_id", "parentOrderId"], axis=1, inplace=True)
                    dataframe.reset_index(drop=True, inplace=True)
                    dataframe.loc[1:, ["Miles", "Stop Offs",
                                       'Stop Fee per stop', 'Free Mileage', 'Base Fee', 'Mileage Price']] = ""
                    dataframe.loc[dataframe.shape[0]] = np.nan
                    final_df = pd.concat([dataframe, final_df], axis=0)
                    final_df = final_df[col1]

            final_df_len = final_df.shape[0]
            final_df["Booking ID"].fillna(0, inplace=True)
            final_df["Stop Off Charge"] = [f'=PRODUCT(N{row + 8}:O{row + 8})' for row in
                                           range(2, final_df.shape[0] + 2)]
            final_df["Base + Mileage Charge"] = [f'=(M{row + 8}-P{row + 8})*R{row + 8}+Q{row + 8}' for row in
                                                 range(2, final_df.shape[0] + 2)]
            final_df['Total Time Fee'] = [f'=(S{row + 8}-T{row + 8})*U{row + 8}' for row in
                                          range(2, final_df.shape[0] + 2)]
            final_df["Total Amount"] = [f'=SUM(V{row + 8}:X{row + 8})' for row in range(2, final_df.shape[0] + 2)]

            final_df.rename(columns=currency_addon, inplace=True)

            def highlight_greaterthan_1(s):
                if s["Booking ID"] == 0:
                    return ['background-color: black'] * 25
                else:
                    return ['background-color: white'] * 25

            final_df.reset_index(drop=True, inplace=True)
            final_df = final_df.style.apply(highlight_greaterthan_1, axis=1)

            ##################### Excel Creation #################

            file_created_at = datetime.today()
            file_created_at_ts = int(file_created_at.timestamp())
            file_name = "{}_{}.xlsx".format(service, str(file_created_at_ts))

            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, sheet_name="Sprinter-Van Invoicing Report", index=False, startrow=8,
                                  startcol=0)
                workbook = writer.book
                # merge_format_1 = workbook.add_format({
                #     'bold': 1,
                #     'border': 1,
                #     'fg_color': '#92d050',
                #     'text_wrap': 1
                # })
                #
                # merge_format_2 = workbook.add_format({
                #     'bold': 1,
                #     'border': 1,
                #     'fg_color': '#fbe4d5',
                #     'text_wrap': 1
                # })

                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#d9e2f3',
                    'border': 1})

                worksheet = writer.sheets['Sprinter-Van Invoicing Report']
                worksheet.write('L4', 'Invoice Detail', workbook.add_format({"bold": True, "font_size": 28}))

                #     worksheet.set_row(9, cell_format=data_format2)

                # worksheet.merge_range('D1:G1', 'Base Fee:  $34.95 - 15 Miles FREE', merge_format_1)
                # #     worksheet.write('D1', 'Base Fee:  $34.95 - 15 Miles FREE',workbook.add_format({"bold":True}))
                # worksheet.merge_range('D2:F2', 'Distance Fee:  Per Mile Charge after 15 Miles', merge_format_2)
                # worksheet.write('G2', '1.35', merge_format_2)
                #
                # worksheet.merge_range('D3:G3',
                #                       'Waiting Fee:  15 Mins FREE Time:  $1.00 Per Minute After 15 Minutes',
                #                       merge_format_2)
                # worksheet.merge_range('D4:G4', 'Time Fee:  $1.00 Per Min After 2 hours (120 Miles)', merge_format_2)
                # worksheet.merge_range('D5:G5', 'Stop Off Fee:  $12.50', merge_format_2)
                # worksheet.merge_range('D6:G6', 'Tolls:  Pass Through', merge_format_2)
                # worksheet.merge_range('D7:G7', 'No Weekend / After Hour Surcharges', merge_format_1)
                worksheet.insert_image('A2', logo, {'x_scale': 1.5, 'y_scale': 1})
                worksheet.insert_image('S2', all_pro_logo, {'x_scale': .3, 'y_scale': .3})

                border_fmt = workbook.add_format({'bottom': 2, 'top': 2, 'left': 2, 'right': 2})
                worksheet.conditional_format(
                    xlsxwriter.utility.xl_range(8, 0, 8 + final_df_len, len(final_df.columns) - 1),
                    {'type': 'no_errors', 'format': border_fmt})

                for col_num, value in enumerate(final_df.columns.values):
                    worksheet.write(8, col_num, value, header_format)

            current_path = str(os.getcwd())
            file = "/".join([current_path, file_name])
            print("path: ", file)
            upload_on = {1: self.aws,
                         2: self.google
                         }

            file_url = upload_on[UPLOAD_ON](
                service=service.lower().replace(" ", "_"),
                file_name=file_name,
                file=file
            )
            try:
                self.mongo_insertion(_type,file_created_at,file_created_at_ts,start_time,
                        end_time,customer_id,file_name,file_url,service)

                data = {'file_url': file_url}
                return ExportResponses.get_status_200(data=data)
            except Exception as ex:
                return ExportResponses.get_status_500(ex)

        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def hourly_fee_report(self, request):
        try:
            print(" hourly_fee Report")
            print("Request--->", request.data)
            _type = int(request.data.get("type"))
            # service = SERVICE_SUPPORT[_type]
            service = "ced_box_truck"
            # type_msg = SERVICE_SUPPORT[_type]
            # ---------------------- time stamp ----------------------
            try:
                start_time = int(request.data["start_time"])
                end_time = int(request.data["end_time"])
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

            customer_id = request.data.get("customerId", 0)

            data = dbhelper.hourly_fee_data(start_time, end_time, customer_id)
            print("data.shape----->", data.shape)
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            data["Requested Pickup Time"] = np.where(data["Requested Pickup Time"] == 0, data["Booking Date"],
                                                     data["Requested Pickup Time"])

            currencySymbol = data["currencySymbol"].loc[0]

            print("Hitted---->", data.shape)
            data.sort_values(by='Requested Pickup Time', ascending=False, inplace=True)
            data["Requested Pickup Time"] = data["Requested Pickup Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Drop Time"] = data["Drop Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Completed On"] = data["Completed On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booking Date"] = data["Booking Date"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))

            data["Booking Date"] = data["Booking Date"].apply(lambda x: x.split(" ")[0])
            data["Hours"] = round(data["Hours"] / 60, 3)
            data["Hours"].fillna(0, inplace=True)
            data["Hourly Fee"].fillna(0, inplace=True)

            data["Driver Id"] = data["Driver Id"].astype('str')

            col = ['Booking ID', 'Sender Name',
                   'Vehicle Type', 'Requested Pickup Time', 'Pickup Address', 'Drop Time',
                   'Drop Address', 'Load Type', 'Amount', 'Driver Name', 'Completed On',
                   'Booking Date', 'Driver Id', 'Hours', 'Hourly Fee', 'Return Fee']

            col1 = ['Booking ID', 'Sender Name',
                    'Vehicle Type', 'Requested Pickup Time', 'Pickup Address', 'Drop Time',
                    'Drop Address', 'Load Type', 'Amount', 'Driver Name', 'Completed On', 'Hours',
                    'Hourly Fee', 'Return Fee']

            data = data[col]

            pre_date = ""
            final_df = pd.DataFrame(columns=col)
            for key, dataframe in data.groupby(["Booking Date", "Driver Id"]):
                print("key---->", key)
                if key[0] == pre_date:
                    dataframe.reset_index(drop=True, inplace=True)
                    dataframe.loc[1:, ["Hours", "Hourly Fee", 'Return Fee']] = ""
                    dataframe.loc[dataframe.shape[0]] = ""
                    dataframe.loc[dataframe.shape[0] - 1, :12] = "Return"
                    dataframe.loc[dataframe.shape[0] - 1, "Drop Address"] = dataframe.loc[0]["Pickup Address"]
                    dataframe.loc[dataframe.shape[0] - 1, "Pickup Address"] = dataframe.loc[0]["Drop Address"]
                    dataframe.loc[dataframe.shape[0] + 1] = ""
                    final_df = pd.concat([dataframe, final_df], axis=0)
                    final_df = final_df[col1]

                else:
                    dataframe.reset_index(drop=True, inplace=True)
                    dataframe.loc[1:, ["Hours", "Hourly Fee", 'Return Fee']] = ""
                    dataframe.loc[dataframe.shape[0]] = ""
                    dataframe.loc[dataframe.shape[0] - 1, :12] = "Return"
                    dataframe.loc[dataframe.shape[0] - 1, "Drop Address"] = dataframe.loc[0]["Pickup Address"]
                    dataframe.loc[dataframe.shape[0] - 1, "Pickup Address"] = dataframe.loc[0]["Drop Address"]
                    dataframe.loc[dataframe.shape[0]] = np.nan
                    final_df = pd.concat([dataframe, final_df], axis=0)
                    final_df = final_df[col1]

                pre_date = key[0]

            final_df.reset_index(drop=True, inplace=True)
            final_df_len = final_df.shape[0]
            final_df["Booking ID"].fillna(0, inplace=True)
            final_df["Total Amount"] = [f'=(L{row + 8}*M{row + 8})+N{row + 8}' for row in
                                        range(2, final_df.shape[0] + 2)]

            currency_addon = {
                'Amount': f'Amount ({currencySymbol})',
                'Return Fee': f'Return Fee ({currencySymbol})',
                'Hourly Fee': f'Hourly Fee ({currencySymbol})',
                'Total Amount': f'Total Amount ({currencySymbol})',
            }

            final_df.rename(columns=currency_addon, inplace=True)

            def highlight_greaterthan_1(s):
                if s["Booking ID"] == 0:
                    return ['background-color: black'] * 15
                elif s["Booking ID"] == "Return":
                    return ["font-weight: bold"] * 15
                else:
                    return ['background-color: white'] * 15

            final_df = final_df.style.apply(highlight_greaterthan_1, axis=1)

            ##################### Excel Creation #################

            file_created_at = datetime.today()
            file_created_at_ts = int(file_created_at.timestamp())
            file_name = "{}_{}.xlsx".format(service, str(file_created_at_ts))

            row = 0
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, sheet_name="Sprinter-Van Invoicing Report", index=False, startrow=8,
                                  startcol=0)
                workbook = writer.book
                # merge_format_1 = workbook.add_format({
                #     'bold': 1,
                #     'border': 1,
                #     'fg_color': '#92d050',
                #     'text_wrap': 1
                # })
                #
                # merge_format_2 = workbook.add_format({
                #     'bold': 1,
                #     'border': 1,
                #     'fg_color': '#fbe4d5',
                #     'text_wrap': 1
                # })

                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#d9e2f3',
                    'border': 1})

                worksheet = writer.sheets['Sprinter-Van Invoicing Report']
                worksheet.write('G4', 'Invoice Detail', workbook.add_format({"bold": True, "font_size": 28}))

                # #     worksheet.set_row(9, cell_format=data_format2)
                #
                # worksheet.merge_range('D1:G1', 'Base Fee:  $34.95 - 15 Miles FREE', merge_format_1)
                #
                # worksheet.merge_range('D2:F2', 'Distance Fee:  Per Mile Charge after 15 Miles', merge_format_2)
                #
                # worksheet.write('G2', '1.35', merge_format_2)
                #
                # worksheet.merge_range('D3:G3', 'Waiting Fee:  15 Mins FREE Time:  $1.00 Per Minute After 15 Minutes',
                #                       merge_format_2)
                #
                # worksheet.merge_range('D4:G4', 'Time Fee:  $1.00 Per Min After 2 hours (120 Miles)', merge_format_2)
                #
                # worksheet.merge_range('D5:G5', 'Stop Off Fee:  $12.50', merge_format_2)
                #
                # worksheet.merge_range('D6:G6', 'Tolls:  Pass Through', merge_format_2)
                #
                # worksheet.merge_range('D7:G7', 'No Weekend / After Hour Surcharges', merge_format_1)

                worksheet.insert_image('A2', logo, {'x_scale': 1.5, 'y_scale': 1})
                worksheet.insert_image('M2', all_pro_logo, {'x_scale': .3, 'y_scale': .3})

                #     format = workbook.add_format({'text_wrap': 1})

                border_fmt = workbook.add_format({'bottom': 2, 'top': 2, 'left': 2, 'right': 2})
                worksheet.conditional_format(
                    xlsxwriter.utility.xl_range(8, 0, 8 + final_df_len, len(final_df.columns) - 1),
                    {'type': 'no_errors', 'format': border_fmt})

                for col_num, value in enumerate(final_df.columns.values):
                    worksheet.write(8, col_num, value, header_format)

            current_path = str(os.getcwd())
            file = "/".join([current_path, file_name])
            print("path: ", file)
            upload_on = {1: self.aws,
                         2: self.google
                         }

            file_url = upload_on[UPLOAD_ON](
                service=service.lower().replace(" ", "_"),
                file_name=file_name,
                file=file
            )
            try:
                self.mongo_insertion(_type,file_created_at,file_created_at_ts,start_time,
                        end_time,customer_id,file_name,file_url,service)

                data = {'file_url': file_url}
                return ExportResponses.get_status_200(data=data)
            except Exception as ex:
                return ExportResponses.get_status_500(ex)

        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)

    def loads(self, request):
        try:
            print("########################### Loads Report #########################")
            print("Request--->", request.data)
            service = "loads_report"
            _type = 5
            # type_msg = SERVICE_SUPPORT[_type]
            # ---------------------- time stamp ----------------------
            try:
                start_time = int(request.data["start_time"])
                end_time = int(request.data["end_time"])
            except:
                response = {
                    "message": "Missing/Incorrect Time stamp, 'start_time' or 'end_time' must be integer"}
                return ExportResponses.get_status_400(response)
            # ---------------------- timezone ------------------------
            try:
                time_zone = pytz.timezone(request.data["timezone"])
            except:
                response = {"message": "time zone is incorrect/missing"}
                return ExportResponses.get_status_422(response)

            customer_id = request.data.get("customerId", 0)

            data = dbhelper.load_report_data(start_time, end_time, customer_id)
            if data.shape[0] == 0:
                return ExportResponses.get_status_204()

            data["Requested Pickup Time"] = np.where(data["Requested Pickup Time"] == 0, data["Booking Date"],
                                                     data["Requested Pickup Time"])

            currencySymbol = data["currencySymbol"].loc[0]

            data["Requested Pickup Time"] = np.where(data["Requested Pickup Time"] == 0, data["Booking Date"],
                                                     data["Requested Pickup Time"])
            data.sort_values(by='Requested Pickup Time', ascending=False, inplace=True)
            data["Requested Pickup Time"] = data["Requested Pickup Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Booked On"] = data["Booked On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Drop Time"] = data["Drop Time"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))
            data["Completed On"] = data["Completed On"].apply(
                lambda x: datetime.fromtimestamp(x).replace(tzinfo=UTC).astimezone(time_zone).strftime(date_format))

            def week_number_of_month(date_value):
                first_day = date_value.replace(day=1)

                dom = date_value.day
                adjusted_dom = dom + first_day.weekday()

                return int(ceil(adjusted_dom / 7.0))

            data['Completed On'] = pd.to_datetime(data['Completed On'], errors='coerce')

            data['Year'] = data['Completed On'].dt.year
            data['QTR'] = data['Completed On'].dt.quarter
            data['Month'] = data['Completed On'].dt.month
            data['Week'] = data['Completed On'].apply(week_number_of_month)
            data['Day'] = data['Completed On'].dt.day
            data.sort_values(by='Completed On', ascending=False, inplace=True)

            list_seq = ['Year', 'QTR', 'Month', 'Week', 'Day', 'Booking ID', 'Booked On', 'Posted By', "Customer",
                        'Vehicle Type', 'Requested Pickup Time', 'Pickup Address', 'Drop Time', 'Drop Address',
                        'Load Type', 'Amount', 'Driver Name', 'Completed On']

            data = data[list_seq]

            currency_addon = {
                'Amount': f'Amount ({currencySymbol})',
            }

            data.rename(columns=currency_addon, inplace=True)

            ##################### Excel Creation #################

            file_created_at = datetime.today()
            file_created_at_ts = int(file_created_at.timestamp())
            file_name = "{}_{}.xlsx".format(service, str(file_created_at_ts))

            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                data.to_excel(writer, sheet_name="Loads Report", index=False, startrow=0,
                              startcol=0)
                workbook = writer.book

                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#d9e2f3',
                    'border': 1})

                worksheet = writer.sheets['Loads Report']

                border_fmt = workbook.add_format({'bottom': 2, 'top': 2, 'left': 2, 'right': 2})
                worksheet.conditional_format(
                    xlsxwriter.utility.xl_range(0, 0, data.shape[0], len(data.columns) - 1),
                    {'type': 'no_errors', 'format': border_fmt})

                for col_num, value in enumerate(data.columns.values):
                    worksheet.write(0, col_num, value, header_format)

            current_path = str(os.getcwd())
            file = "/".join([current_path, file_name])
            print("path: ", file)
            upload_on = {1: self.aws,
                         2: self.google
                         }

            file_url = upload_on[UPLOAD_ON](
                service=service.lower().replace(" ", "_"),
                file_name=file_name,
                file=file
            )
            try:
                self.mongo_insertion(_type,file_created_at,file_created_at_ts,start_time,
                        end_time,customer_id,file_name,file_url,service)

                data = {'file_url': file_url}
                return ExportResponses.get_status_200(data=data)
            except Exception as ex:
                return ExportResponses.get_status_500(ex)

        except Exception as ex:
            traceback.print_exc()
            return ExportResponses.get_status_500(ex)
