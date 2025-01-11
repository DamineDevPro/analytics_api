import numpy as np
import pandas as pd
from analytics.function import Process
from datetime import datetime, timedelta, date
from .ride_dashboard_response_helper import RideResponses
from analytics.settings import  UTC, db, CURRENCY_API, BASE_CURRENCY
import traceback

RideResponses = RideResponses()
GROUP_BY_SUPPORT = {0: "Hour", 1: "Day", 2: "Week", 3: "Month", 4: "Quarter", 5: 'Year', 6: "Hour of Day",
                    7: "Day of Week"}


class RideOperations:

    def ride_fare_graph(self, result_data, time_zone, start_timestamp, end_timestamp, group_by, conversion_rate,
                        currency_symbol):
        try:
            # result_data["createdDate"] = pd.to_datetime(result_data["createdDate"], unit='s')
            # result_data['createdDate'] = result_data['createdDate'].apply(
            #     lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

            result_data['createdDate'] = result_data['createdDate'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))

            result_data["dateTime"] = result_data.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            try:
                result_data['typeId'] = result_data['typeId'].apply(
                    lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                result_data['typeId'] = result_data['typeId'].astype(str)
            vehicle_type = result_data[['typeId', "typeName"]].drop_duplicates(subset=["typeId"], keep='last')
            vehicle_name_list = list(vehicle_type["typeName"])
            vehicle_type = vehicle_type.to_dict(orient="records")
            result_data = result_data[['dateTime', 'typeId', "estimate_fare"]]
            result_data = pd.get_dummies(result_data, columns=['typeId'])
            rename_dict = {"typeId_{}".format(vehicle["typeId"]): vehicle["typeName"] for vehicle in vehicle_type}
            result_data = result_data.rename(columns=rename_dict)
            for vehicle in vehicle_name_list:
                result_data[vehicle] = result_data[vehicle].astype(int) * \
                                       result_data["estimate_fare"].astype(float) * \
                                       conversion_rate
            # date filler (add missing date time in an result_data frame)
            result_data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                              time_zone=time_zone, data_frame=result_data, date_column="dateTime",
                                              group_by=group_by)

            # date converter as per group by
            result_data = Process.date_conversion(group_by=group_by, data_frame=result_data, date_col='dateTime')
            result_data = result_data.groupby(['dateTime']).sum().reset_index()

            # result_data frame sorting
            result_data = result_data.sort_values(by='dateTime', ascending=True)
            if group_by == 3: result_data = Process.month_sort(data_frame=result_data, date_column="dateTime")
            if group_by == 4: result_data = Process.quarter_sort(data_frame=result_data, date_column="dateTime")
            if group_by == 7: result_data = Process.day_sort(data_frame=result_data, date_column="dateTime")
            for vehicle in vehicle_name_list:
                result_data[vehicle] = result_data[vehicle].astype(float)
            graph = {
                "series": [
                    {"name": vehicle, "data": list(result_data[vehicle])} for vehicle in vehicle_name_list
                ],
                "xaxis": {"title": GROUP_BY_SUPPORT[group_by], "categories": list(result_data['dateTime'])},
                "yaxis": {"title": "Fare({})".format(currency_symbol)}
            }
            count = 0
            for vehicle in vehicle_name_list:
                count = count + result_data[vehicle].sum()

            response = {'message': 'success', 'graph': graph, "count": float(count)}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def ride_count_graph(self, result_data, time_zone, start_timestamp, end_timestamp, group_by):
        try:
            result_data["createdDate"] = pd.to_datetime(result_data["createdDate"], unit='s')
            result_data['createdDate'] = result_data['createdDate'].apply(
                lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))
            result_data["dateTime"] = result_data.createdDate.apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            # result_data["count"] = 1
            result_data = result_data.drop("estimate_fare", axis=1, errors="ignore")
            try:
                result_data['typeId'] = result_data['typeId'].apply(
                    lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                result_data['typeId'] = result_data['typeId'].astype(str)
            vehicle_type = result_data[['typeId', "typeName"]].drop_duplicates(subset=["typeId"], keep='last')
            vehicle_name_list = list(vehicle_type["typeName"])
            vehicle_type = vehicle_type.to_dict(orient="records")
            result_data = result_data[['dateTime', 'typeId']]
            result_data = pd.get_dummies(result_data, columns=['typeId'])
            rename_dict = {"typeId_{}".format(vehicle["typeId"]): vehicle["typeName"] for vehicle in vehicle_type}
            result_data = result_data.rename(columns=rename_dict)
            # date filler (add missing date time in an result_data frame)
            result_data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                              time_zone=time_zone, data_frame=result_data, date_column="dateTime",
                                              group_by=group_by)

            # date converter as per group by
            result_data = Process.date_conversion(group_by=group_by, data_frame=result_data, date_col='dateTime')
            result_data = result_data.groupby(['dateTime']).sum().reset_index()

            # result_data frame sorting
            result_data = result_data.sort_values(by='dateTime', ascending=True)
            if group_by == 3: result_data = Process.month_sort(data_frame=result_data, date_column="dateTime")
            if group_by == 4: result_data = Process.quarter_sort(data_frame=result_data, date_column="dateTime")
            if group_by == 7: result_data = Process.day_sort(data_frame=result_data, date_column="dateTime")
            for vehicle in vehicle_name_list:
                result_data[vehicle] = result_data[vehicle].astype(int)
            graph = {
                "series": [
                    {"name": vehicle, "data": list(result_data[vehicle])} for vehicle in vehicle_name_list
                ],
                "xaxis": {"title": GROUP_BY_SUPPORT[group_by], "categories": list(result_data['dateTime'])},
                "yaxis": {"title": "Count"}
            }
            count = 0
            for vehicle in vehicle_name_list:
                count = count + result_data[vehicle].sum()
            response = {'message': 'success', 'graph': graph, "count": int(count)}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def ride_status(self, order, time_zone, start_timestamp, end_timestamp, group_by):
        try:
            order = pd.DataFrame(order)
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            try:
                order['typeId'] = order['typeId'].apply(lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                order['typeId'] = order['typeId'].astype(str)
            order['bookingStatus'] = order["bookingStatus"].astype(int)
            order['bookingStatus'] = order["bookingStatus"].replace({4: "customer_cancelled", 5: "driver_cancelled"})
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            order = order[['bookingDateTimestamp', 'typeId', 'bookingStatus']]
            payment_type = ["customer_cancelled"]  # , "driver_cancelled"]
            # Graph Data Construction
            unique_order = list(order.bookingStatus.unique())
            if len(unique_order) == 1:
                order = order.rename(columns={'bookingStatus': 'bookingStatus_{}'.format(unique_order[0])})
                order['bookingStatus_{}'.format(unique_order[0])] = order[
                    'bookingStatus_{}'.format(unique_order[0])].replace(unique_order[0], 1)
                for method_type in payment_type:
                    if method_type not in unique_order:
                        order['bookingStatus_{}'.format(method_type)] = 0
            else:
                order = pd.get_dummies(order, columns=['bookingStatus'])
                for method_type in payment_type:
                    if method_type not in unique_order:
                        order['bookingStatus_{}'.format(method_type)] = 0

            order = order[
                ['bookingDateTimestamp', 'bookingStatus_customer_cancelled']]  # , 'bookingStatus_driver_cancelled']]
            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)
            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")
            bookings_group_graph = order.groupby(['bookingDateTimestamp']).sum().reset_index()
            bookings_group_graph = bookings_group_graph.sort_values(by='bookingDateTimestamp', ascending=True)

            if group_by == 3: bookings_group_graph = Process.month_sort(data_frame=bookings_group_graph,
                                                                        date_column="bookingDateTimestamp")
            if group_by == 4: bookings_group_graph = Process.quarter_sort(data_frame=bookings_group_graph,
                                                                          date_column="bookingDateTimestamp")
            if group_by == 7: bookings_group_graph = Process.day_sort(data_frame=bookings_group_graph,
                                                                      date_column="bookingDateTimestamp")
            graph = {
                'series': [
                    {
                        'name': 'Customer Cancelled',
                        'data': list(bookings_group_graph['bookingStatus_customer_cancelled'])
                    },
                    # {
                    #     'name': 'Driver Cancelled',
                    #     'data': list(bookings_group_graph['bookingStatus_driver_cancelled'])
                    # },
                ],
                'xaxis': {
                    'title': GROUP_BY_SUPPORT[group_by],
                    'categories': list(bookings_group_graph['bookingDateTimestamp'])
                },
                'yaxis': {
                    'title': 'Payment Count'
                }
            }
            booking_type = ['bookingStatus_customer_cancelled']  # , 'bookingStatus_driver_cancelled']
            count = 0
            for _type in booking_type:
                count = count + bookings_group_graph[_type].sum()
            data = {'graph': graph, "count": int(count)}
            response = {'message': 'success', 'data': data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def ride_payment(self, order, time_zone, start_timestamp, end_timestamp, group_by):
        try:
            order = pd.DataFrame(order)
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            try:
                order['typeId'] = order['typeId'].apply(lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                order['typeId'] = order['typeId'].astype(str)
            order['paymentType'] = order["paymentType"].astype(int)
            order['paymentType'] = order["paymentType"].replace({1: "card", 2: "cash", 3: "corporate_wallet"})
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            order = order[['bookingDateTimestamp', 'typeId', 'paymentType']]
            payment_type = ["card", "cash", "corporate_wallet"]
            # Graph Data Construction
            order = order[['bookingDateTimestamp', 'typeId', 'paymentType']]
            unique_order = list(order.paymentType.unique())
            if len(unique_order) == 1:
                order = order.rename(columns={'paymentType': 'paymentType_{}'.format(unique_order[0])})
                order['paymentType_{}'.format(unique_order[0])] = order[
                    'paymentType_{}'.format(unique_order[0])].replace({unique_order[0]: 1})
                for method_type in payment_type:
                    if method_type not in unique_order:
                        order['paymentType_{}'.format(method_type)] = 0
            else:
                order = pd.get_dummies(order, columns=['paymentType'])
                for method_type in payment_type:
                    if method_type not in unique_order:
                        order['paymentType_{}'.format(method_type)] = 0

            order = order[
                ['bookingDateTimestamp', 'paymentType_card', 'paymentType_cash', "paymentType_corporate_wallet"]]
            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)
            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")
            bookings_group_graph = order.groupby(['bookingDateTimestamp']).sum().reset_index()
            bookings_group_graph = bookings_group_graph.sort_values(by='bookingDateTimestamp', ascending=True)

            if group_by == 3: bookings_group_graph = Process.month_sort(data_frame=bookings_group_graph,
                                                                        date_column="bookingDateTimestamp")
            if group_by == 4: bookings_group_graph = Process.quarter_sort(data_frame=bookings_group_graph,
                                                                          date_column="bookingDateTimestamp")
            if group_by == 7: bookings_group_graph = Process.day_sort(data_frame=bookings_group_graph,
                                                                      date_column="bookingDateTimestamp")

            graph = {
                'series': [
                    {
                        'name': 'Card',
                        'data': list(bookings_group_graph['paymentType_card'])
                    },
                    {
                        'name': 'Cash',
                        'data': list(bookings_group_graph['paymentType_cash'])
                    },
                    {
                        'name': 'Corporate Wallet',
                        'data': list(bookings_group_graph['paymentType_corporate_wallet'])
                    }
                ],
                'xaxis': {
                    'title': GROUP_BY_SUPPORT[group_by],
                    'categories': list(bookings_group_graph['bookingDateTimestamp'])
                },
                'yaxis': {
                    'title': 'Payment Count'
                }
            }
            count = 0
            for p_type in ['paymentType_card', 'paymentType_cash', 'paymentType_corporate_wallet']:
                count = count + bookings_group_graph[p_type].sum()
            data = {'graph': graph}
            response = {'message': 'success', 'data': data, "count": float(count)}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)
