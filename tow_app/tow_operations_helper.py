import numpy as np
import pandas as pd
from analytics.function import Process
from datetime import datetime, timedelta, date
from .tow_response_helper import RideResponses
from analytics.settings import  UTC, db, CURRENCY_API, BASE_CURRENCY
# from geopy.geocoders import Nominatim
from bson import ObjectId
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

            # print(result_data.head(3).to_dict(orient="records"))
            # result_data.to_csv("data.csv")
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
                result_data[vehicle] = result_data[vehicle].astype(int) * result_data["estimate_fare"].astype(
                    float) * conversion_rate
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
            response = {'message': 'success', 'graph': graph}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
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
            response = {'message': 'success', 'graph': graph}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def ride_payment(self, order, time_zone, start_timestamp, end_timestamp, group_by, export):
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
            order_copy = order.copy()
            vehicle = order[["typeId", "typeName"]].drop_duplicates(subset=["typeId"], keep='last')
            order = order[['bookingDateTimestamp', 'typeId', 'paymentType']]
            payment_type = ["card", "cash", "corporate_wallet"]
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

            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)

            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")

            if group_by == 3: order = Process.month_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 4: order = Process.quarter_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 7: order = Process.day_sort(data_frame=order, date_column="bookingDateTimestamp")
            for col in ['paymentType_card', 'paymentType_cash', 'paymentType_corporate_wallet']:
                order[col] = order[col].astype(int)
            order_group = pd.pivot_table(order, values=['paymentType_card', 'paymentType_cash',
                                                        'paymentType_corporate_wallet'],
                                         index=['bookingDateTimestamp', 'typeId'],
                                         aggfunc=np.sum, fill_value=0)
            order_group = order_group.reset_index()

            mapping = {}
            for index in list(vehicle.index):
                mapping[vehicle["typeId"].loc[index]] = vehicle["typeName"].loc[index]
            order_group = pd.merge(order_group, vehicle, on="typeId", how="left")
            order_group = order_group.drop("typeId", axis=1)
            order_group = order_group.rename(columns={"typeName": "typeId"})
            order_group['bookingDateTimestamp'] = order_group['bookingDateTimestamp'].astype(str)

            # construction of history data
            history = {}
            for index in range(order_group.shape[0]):
                if history.get(order_group['bookingDateTimestamp'].loc[index]):
                    value = history[order_group['bookingDateTimestamp'].loc[index]]
                    value.append({"Vehicle Type": str(order_group['typeId'].loc[index]),
                                  "card": int(order_group['paymentType_card'].loc[index]),
                                  "cash": int(order_group['paymentType_cash'].loc[index]),
                                  "corporate wallet": int(order_group['paymentType_corporate_wallet'].loc[index]),
                                  })
                    history[order_group['bookingDateTimestamp'].loc[index]] = value
                else:
                    history[order_group['bookingDateTimestamp'].loc[index]] = [{
                        "Vehicle Type": str(order_group['typeId'].loc[index]),
                        "card": int(order_group['paymentType_card'].loc[index]),
                        "cash": int(order_group['paymentType_cash'].loc[index]),
                        "corporate wallet": int(order_group['paymentType_corporate_wallet'].loc[index]),

                    }]
            for key, value in history.items():
                present_vehicle_type = [call['Vehicle Type'] for call in value]
                v_type = list(mapping.values())
                insert_vehicle_type = list(filter(lambda x: x not in present_vehicle_type, v_type))
                for v_type in insert_vehicle_type:
                    value.insert(0, {'Vehicle Type': v_type, 'card': 0, 'cash': 0, "corporate wallet": 0})
                history[key] = list(filter(lambda x: x['Vehicle Type'] != "nan", value))
            # Table data construction
            order = order[
                ['bookingDateTimestamp', 'paymentType_card', 'paymentType_cash', 'paymentType_corporate_wallet']]
            order_group = order.groupby(['bookingDateTimestamp']).sum().reset_index()

            if group_by == 3: order_group = Process.month_sort(data_frame=order_group,
                                                               date_column="bookingDateTimestamp")
            if group_by == 4: order_group = Process.quarter_sort(data_frame=order_group,
                                                                 date_column="bookingDateTimestamp")
            if group_by == 7: order_group = Process.day_sort(data_frame=order_group, date_column="bookingDateTimestamp")
            show_message = 0
            if export:
                pass
            else:
                if order_group.shape[0] > 1000: show_message = 1
                order_group = order_group.head(1000)

            # Graph Data Construction
            order = order_copy[['bookingDateTimestamp', 'typeId', 'paymentType']]
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
            if export:
                pass
            else:
                bookings_group_graph = bookings_group_graph.head(1000)
            if export:
                table = {
                    'totalAmount': order_group.rename(
                        columns={'bookingDateTimestamp': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                                 'paymentType_card': 'Card',
                                 'paymentType_cash': 'Cash',
                                 'paymentType_corporate_wallet': 'Corporate Wallet'}).to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history
                }
            else:
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
                        'title': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                        'categories': list(bookings_group_graph['bookingDateTimestamp'])
                    },
                    'yaxis': {
                        'title': 'Payment Count'
                    }
                }
                table = {
                    'totalAmount': order_group.rename(
                        columns={'bookingDateTimestamp': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                                 'paymentType_card': 'Card',
                                 'paymentType_cash': 'Cash',
                                 'paymentType_corporate_wallet': 'Corporate Wallet'}).to_dict(orient='list')
                }
                data = {
                    'graph': graph,
                    'table': table,
                    'history': history,
                    "show_message": show_message
                }
            response = {'message': 'success', 'data': data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def vehicle_type(self, data):
        try:
            response_data = data[["_id", "typeName"]]
            response_data["id"] = response_data["_id"].apply(lambda x: str(x))
            response_data = response_data.drop("_id", axis=1, errors="ignore")
            response_data = response_data.to_dict(orient="records")
            response_data.insert(0, {"id": "0", "typeName": {"en": "All"}})
            response = {'message': 'success', 'data': response_data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)

    def ride_status(self, order, time_zone, start_timestamp, end_timestamp, group_by, export):
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
            order_copy = order.copy()
            vehicle = order[["typeId", "typeName"]].drop_duplicates(subset=["typeId"], keep='last')
            order = order[['bookingDateTimestamp', 'typeId', 'bookingStatus']]
            payment_type = ["customer_cancelled"]  # , "driver_cancelled"]
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

            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)

            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")

            if group_by == 3: order = Process.month_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 4: order = Process.quarter_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 7: order = Process.day_sort(data_frame=order, date_column="bookingDateTimestamp")
            for col in ['bookingStatus_customer_cancelled']:  # , 'bookingStatus_driver_cancelled']:
                order[col] = order[col].astype(int)
            order_group = pd.pivot_table(order, values=['bookingStatus_customer_cancelled',
                                                        # 'bookingStatus_driver_cancelled',
                                                        ],
                                         index=['bookingDateTimestamp', 'typeId'],
                                         aggfunc=np.sum, fill_value=0)
            order_group = order_group.reset_index()

            mapping = {}
            for index in list(vehicle.index):
                mapping[vehicle["typeId"].loc[index]] = vehicle["typeName"].loc[index]
            order_group = pd.merge(order_group, vehicle, on="typeId", how="left")
            order_group = order_group.drop("typeId", axis=1)
            order_group = order_group.rename(columns={"typeName": "typeId"})
            order_group['bookingDateTimestamp'] = order_group['bookingDateTimestamp'].astype(str)

            # construction of history data
            history = {}
            for index in range(order_group.shape[0]):
                if history.get(order_group['bookingDateTimestamp'].loc[index]):
                    value = history[order_group['bookingDateTimestamp'].loc[index]]
                    value.append({"Vehicle Type": str(order_group['typeId'].loc[index]),
                                  "customer_cancelled": int(order_group['bookingStatus_customer_cancelled'].loc[index]),
                                  # "driver_cancelled": int(order_group['bookingStatus_driver_cancelled'].loc[index]),
                                  })
                    history[order_group['bookingDateTimestamp'].loc[index]] = value
                else:
                    history[order_group['bookingDateTimestamp'].loc[index]] = [{
                        "Vehicle Type": str(order_group['typeId'].loc[index]),
                        "customer_cancelled": int(order_group['bookingStatus_customer_cancelled'].loc[index]),
                        # "driver_cancelled": int(order_group['bookingStatus_driver_cancelled'].loc[index]),

                    }]
            for key, value in history.items():
                present_vehicle_type = [call['Vehicle Type'] for call in value]
                v_type = list(mapping.values())
                insert_vehicle_type = list(filter(lambda x: x not in present_vehicle_type, v_type))
                for v_type in insert_vehicle_type:
                    value.insert(0, {'Vehicle Type': v_type, 'customer_cancelled': 0})  # , 'driver_cancelled': 0})
                history[key] = list(filter(lambda x: x['Vehicle Type'] != "nan", value))

            # Table data construction
            order = order[
                ['bookingDateTimestamp', 'bookingStatus_customer_cancelled']]  # , 'bookingStatus_driver_cancelled']]
            order_group = order.groupby(['bookingDateTimestamp']).sum().reset_index()

            if group_by == 3: order_group = Process.month_sort(data_frame=order_group,
                                                               date_column="bookingDateTimestamp")
            if group_by == 4: order_group = Process.quarter_sort(data_frame=order_group,
                                                                 date_column="bookingDateTimestamp")
            if group_by == 7: order_group = Process.day_sort(data_frame=order_group, date_column="bookingDateTimestamp")
            show_message = 0
            if export:
                pass
            else:
                if order_group.shape[0] > 1000: show_message = 1
                order_group = order_group.head(1000)

            # Graph Data Construction
            order = order_copy[['bookingDateTimestamp', 'typeId', 'bookingStatus']]
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
            if export:
                pass
            else:
                bookings_group_graph = bookings_group_graph.head(1000)
            if export:
                table = {
                    'totalAmount': order_group.rename(
                        columns={'bookingDateTimestamp': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                                 'bookingStatus_customer_cancelled': 'Customer Cancelled',
                                 # 'bookingStatus_driver_cancelled': 'Driver Cancelled',
                                 }).to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history
                }
            else:
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
                        'title': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                        'categories': list(bookings_group_graph['bookingDateTimestamp'])
                    },
                    'yaxis': {
                        'title': 'Payment Count'
                    }
                }
                table = {
                    'totalAmount': order_group.rename(
                        columns={'bookingDateTimestamp': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                                 'bookingStatus_customer_cancelled': 'Customer Cancelled',
                                 # 'bookingStatus_driver_cancelled': 'Driver Cancelled',
                                 }).to_dict(orient='list')
                }
                data = {
                    'graph': graph,
                    'table': table,
                    'history': history,
                    "show_message": show_message
                }
            response = {'message': 'success', 'data': data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)

    # def location(self, coord):
    #     location = geo_locator.reverse(coord, exactly_one=True)
    #     address = location.raw['address']
    #     return address.get("suburb") if address.get("suburb") else address.get("city")

    def id_parser(self, id):
        try:
            return str(id.oid)
        except:
            return str(id)

    def top_ride(self, order, sort, top, conversion_rate, currency_symbol):
        """

        """
        try:
            order["typeId"] = order["typeId"].apply(self.id_parser)

            # --------- vehicle image integration -------
            vehicle_images = order[["typeId", "vehicleImgOff"]].to_dict(orient="records")
            vehicle_dict = {}
            for vehicle in vehicle_images:
                vehicle_dict[vehicle["typeId"]] = vehicle["vehicleImgOff"]

            # -------------------------------------------
            order["cityId"] = order["cityId"].apply(self.id_parser)
            order = order.drop(["pickup", "drop"], axis=1)
            vehicle_type = order[["typeId", "typeName"]].drop_duplicates(keep="last")
            # ----------------------------- Zone Id --------------------------
            zone_ids = list(order["pick_zone_id"].unique()) + list(order["drop_zone_id"].unique())
            _ids = []
            for id in zone_ids:
                try:
                    id = ObjectId(id)
                    _ids.append(id)
                except:
                    pass

            zones = pd.DataFrame(db["operationZones_rides"].find({"_id": {"$in": _ids}}, {"name": 1})).to_dict(
                orient="records")
            print("zones -----------_>", zones)
            zones = {str(zone["_id"]): str(zone["name"]) for zone in zones}
            order["pick_zone"] = order["pick_zone_id"].map(zones)
            order["drop_zone"] = order["drop_zone_id"].map(zones)
            # ----------------------------- Uniques --------------------------
            countries = order[["countryId", "countryName"]].drop_duplicates().to_dict(orient="records")
            countries = {str(country["countryId"]): str(country["countryName"]) for country in countries}
            cities = order[["cityId", "cityName"]].drop_duplicates().to_dict(orient="records")
            cities = {str(city["cityId"]): str(city["cityName"]) for city in cities}
            # order = order.drop(["countryName", "cityName"], axis=1)

            # order["pickup"] = order["pickup"].apply(lambda x: ", ".join([str(round(x.latitude, 4)), str(round(x.longitude, 4))]))
            # order["drop"] = order["drop"].apply(lambda x: ", ".join([str(round(x.latitude, 4)), str(round(x.longitude, 4))]))
            # pickup_unique = set(order["pickup"].unique())
            # drop_unique = set(order["drop"].unique())
            # location = list(pickup_unique.union(drop_unique))
            # print("list of location: ", len(location))
            # location_map = {}
            # for coord in location: location_map[coord] = self.location(coord=coord)
            # print("location completed")
            # order["pickup"] = order['pickup'].map(location_map)
            # order["drop"] = order["drop"].map(location_map)

            # order["pick_country"] = order["pick_zone"].map(zones)
            # order["drop_country"] = order["drop_zone"].map(zones)
            # order["pick_city"] = order["pick_zone"].map(zones)
            # order["drop_city"] = order["drop_zone"].map(zones)
            order["count"] = 1
            order["count"] = order["count"].astype(int)

            order["fare"] = order["fare"].astype(float) * conversion_rate
            pick_col = ["typeId", "countryId", "countryName", "cityId", "cityName", "pick_zone_id", "pick_zone",
                        "count", "fare"]
            pick_col_grouper = ["typeId", "countryId", "countryName", "cityId", "cityName", "pick_zone_id", "pick_zone"]
            top_pickup = order[pick_col].groupby(by=pick_col_grouper).sum().reset_index()

            top_pickup["count"] = top_pickup["count"].astype(int)
            top_pickup["fare"] = top_pickup["fare"].astype(float)
            drop_col = ["typeId", "countryId", "countryName", "cityId", "cityName", "drop_zone_id", "drop_zone",
                        "count", "fare"]
            drop_col_grouper = ["typeId", "countryId", "countryName", "cityId", "cityName", "drop_zone_id", "drop_zone"]
            top_drop = order[drop_col].groupby(by=drop_col_grouper).sum().reset_index()
            top_drop["count"] = top_drop["count"].astype(int)
            top_drop["fare"] = top_drop["fare"].astype(float)
            _col = ["typeId", "countryId", "countryName", "cityId", "cityName", "pick_zone_id", "pick_zone",
                    "drop_zone_id", "drop_zone", "count", "fare"]

            grouper_col = ["typeId", "countryId", "countryName", "cityId", "cityName", "pick_zone_id", "pick_zone",
                           "drop_zone_id", "drop_zone"]

            route = order[_col].groupby(by=grouper_col).sum().reset_index()
            route["count"] = route["count"].astype(int)
            route["fare"] = route["fare"].astype(float)
            # popular_route = route.groupby("typeId").max()
            idx = route.groupby(by='typeId')[sort].idxmax()
            popular_route = route.loc[idx,].sort_values(by=sort, ascending=False).rename(columns={
                "countryId": "topCountryId",
                "countryName": "topCountryName",
                "cityId": "topCityId",
                "cityName": "topCityName",
                "pick_zone_id": "topPickupZoneId",
                "pick_zone": "topPickupZone",
                "drop_zone_id": "topDropZoneId",
                "drop_zone": "topDropZone",
                "count": "topTripCount",
                "fare": "topTripFare",
                # "pickup": "topPickupZoneId",
                # "drop": "topDrop"
            }).to_dict(orient="records")
            response_data = []
            total_type = order[["typeId", "count", "fare"]].groupby(by=["typeId"]).sum().reset_index()
            for id in popular_route:
                id = id
                id["vehicleImgOff"] = vehicle_dict[id["typeId"]]
                id["typeName"] = str(vehicle_type[vehicle_type["typeId"] == id["typeId"]].
                                     to_dict(orient="records")[0]["typeName"])
                id["totalTrip"] = int(total_type[total_type["typeId"] == id["typeId"]]["count"])
                id["totalFare"] = float(total_type[total_type["typeId"] == id["typeId"]]["fare"])
                top_n_picks = top_pickup[top_pickup["typeId"] == id["typeId"]]. \
                    sort_values(by=sort, ascending=False).head(top)
                # ----------------------- Filler -----------------------------
                pick_series = list(top_n_picks[sort])
                pick_axis = list(top_n_picks["pick_zone"].fillna(""))
                if len(pick_series) < top:
                    difference = top - len(pick_series)
                    # pick_series.extend([0 for i in range(difference)])
                    # pick_axis.extend(["" for i in range(difference)])
                    pick_series = [0 for i in range(difference)] + pick_series
                    pick_axis = ["" for i in range(difference)] + pick_axis
                    pick_axis = list(map(lambda x: str(x).split(), pick_axis))
                    pick_axis = list(map(lambda x: [""] if x == [] else x, pick_axis))
                # ----------------------- Filler -----------------------------
                x_name = sort.capitalize() if sort != "fare" else "{sort}".format(
                    sort=sort.capitalize())
                id["topNPickups"] = {
                    "graph": {
                        "series": [{"name": x_name, "data": pick_series}],
                        # "xaxis": {"categories": list(top_n_picks["pickup"])},
                        "xaxis": {"categories": pick_axis, "title": sort},
                        "yaxis": {"title": "Location"}
                    }
                }
                top_n_drops = top_drop[top_drop["typeId"] == id["typeId"]]. \
                    sort_values(by=sort, ascending=False).head(top)
                # ----------------------- Filler -----------------------------
                drop_series = list(top_n_drops[sort])
                drop_axis = list(top_n_drops["drop_zone"].fillna(""))
                if len(drop_series) < top:
                    difference = top - len(drop_series)
                    # drop_series.extend([0 for i in range(difference)])
                    # drop_axis.extend(["" for i in range(difference)])
                    drop_series = [0 for i in range(difference)] + drop_series
                    drop_axis = ["" for i in range(difference)] + drop_axis
                    drop_axis = list(map(lambda x: str(x).split(), drop_axis))
                    drop_axis = list(map(lambda x: [""] if x == [] else x, drop_axis))
                # ----------------------- Filler -----------------------------
                id["topNDrops"] = {
                    "graph": {
                        "series": [{"name": x_name, "data": drop_series}],
                        "xaxis": {"categories": drop_axis, "title": sort},
                        # "xaxis": {"categories": list(top_n_drops["drop"])},
                        "yaxis": {"title": "Location"}
                    }
                }
                response_data.append(id)
            response = {'message': 'success', 'data': response_data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def ride_map_process(self, result_data, location):
        try:
            location_data = pd.DataFrame()
            location_data["long"] = result_data[location].apply(lambda x: x.get("longitude", 0))
            location_data["lat"] = result_data[location].apply(lambda x: x.get('latitude', 0))
            location_data["intensity"] = 1
            location_data = location_data.groupby(by=["long", "lat"]).sum().reset_index()
            centroid = {"lat": location_data["lat"].mean(), "long": location_data["long"].mean()}
            location_data = location_data.to_dict(orient="records")
            response = {"message": "success", "location": location_data, "centroid": centroid}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            traceback.print_exc()
            return RideResponses.get_status_500(ex=ex)

    def descriptive_stats(self, previous_sum, current_sum, previous_start_date, previous_end_date, today_sum):
        """
        Descriptive Stats report
        Provide data with respect to previous period and current period and current date fare summation
        """
        try:
            # ------------------ conditional response ------------------
            if (previous_sum == 0) and (current_sum == 0):
                response = {"message": "success", "percentage": 0,
                            "current_period": current_sum, "previous_period": previous_sum,
                            "data": "decrease from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y'))}
            elif previous_sum == 0:
                response = {"message": "success",
                            "percentage": 100,
                            "data": "decrease from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
            elif previous_sum > current_sum:
                decrease = previous_sum - current_sum
                percent_decrease = (decrease / previous_sum) * 100
                response = {"message": "success",
                            "percentage": -round(percent_decrease, 2),
                            "data": "decrease from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
            else:
                increase = current_sum - previous_sum
                percent_increase = (increase / previous_sum) * 100
                response = {"message": "success",
                            "percentage": round(percent_increase, 2),
                            "data": "increase from {} to {}".format(previous_start_date.strftime('%d %b, %Y'),
                                                                    previous_end_date.strftime('%d %b, %Y')),
                            "current_period": current_sum,
                            "previous_period": previous_sum}
            response["today_total"] = today_sum
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)

    def ride_count(self, order, time_zone, start_timestamp, end_timestamp, group_by, export):
        try:
            order = pd.DataFrame(order)
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            try:
                order['typeId'] = order['typeId'].apply(lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                order['typeId'] = order['typeId'].astype(str)
            order['count'] = 1
            order['count'] = order["count"].astype(int)
            order = order.drop("paymentType", axis=1)
            # order['paymentType'] = order["paymentType"].replace({1: "card", 2: "cash", 3: "corporate_wallet"})
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            order_copy = order.copy()
            vehicle = order[["typeId", "typeName"]].drop_duplicates(subset=["typeId"], keep='last')
            order = order[['bookingDateTimestamp', 'typeId', 'count']]

            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)

            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")

            if group_by == 3: order = Process.month_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 4: order = Process.quarter_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 7: order = Process.day_sort(data_frame=order, date_column="bookingDateTimestamp")
            # for col in ['paymentType_card', 'paymentType_cash', 'paymentType_corporate_wallet']:
            #     order[col] = order[col].astype(int)
            order_group = pd.pivot_table(order, values=['count'],
                                         index=['bookingDateTimestamp', 'typeId'],
                                         aggfunc=np.sum, fill_value=0)
            order_group = order_group.reset_index()

            mapping = {}
            for index in list(vehicle.index):
                mapping[vehicle["typeId"].loc[index]] = vehicle["typeName"].loc[index]
            order_group = pd.merge(order_group, vehicle, on="typeId", how="left")
            order_group = order_group.drop("typeId", axis=1)
            order_group = order_group.rename(columns={"typeName": "typeId"})
            order_group['bookingDateTimestamp'] = order_group['bookingDateTimestamp'].astype(str)

            # construction of history data
            history = {}
            for index in range(order_group.shape[0]):
                if history.get(order_group['bookingDateTimestamp'].loc[index]):
                    value = history[order_group['bookingDateTimestamp'].loc[index]]
                    value.append({"Vehicle Type": str(order_group['typeId'].loc[index]),
                                  "count": int(order_group['count'].loc[index]),
                                  # "cash": int(order_group['paymentType_cash'].loc[index]),
                                  # "corporate wallet": int(order_group['paymentType_corporate_wallet'].loc[index]),
                                  })
                    history[order_group['bookingDateTimestamp'].loc[index]] = value
                else:
                    history[order_group['bookingDateTimestamp'].loc[index]] = [{
                        "Vehicle Type": str(order_group['typeId'].loc[index]),
                        "count": int(order_group['count'].loc[index]),
                        # "cash": int(order_group['paymentType_cash'].loc[index]),
                        # "corporate wallet": int(order_group['paymentType_corporate_wallet'].loc[index]),

                    }]
            for key, value in history.items():
                present_vehicle_type = [call['Vehicle Type'] for call in value]
                v_type = list(mapping.values())
                insert_vehicle_type = list(filter(lambda x: x not in present_vehicle_type, v_type))
                for v_type in insert_vehicle_type:
                    value.insert(0, {'Vehicle Type': v_type, 'count': 0})
                history[key] = list(filter(lambda x: x['Vehicle Type'] != "nan", value))

            # Table data construction
            order = order[
                ['bookingDateTimestamp', 'count']]
            order_group = order.groupby(['bookingDateTimestamp']).sum().reset_index()

            if group_by == 3: order_group = Process.month_sort(data_frame=order_group,
                                                               date_column="bookingDateTimestamp")
            if group_by == 4: order_group = Process.quarter_sort(data_frame=order_group,
                                                                 date_column="bookingDateTimestamp")
            if group_by == 7: order_group = Process.day_sort(data_frame=order_group, date_column="bookingDateTimestamp")
            show_message = 0
            if export:
                pass
            else:
                if order_group.shape[0] > 1000: show_message = 1
                order_group = order_group.head(1000)

            if export:
                table = {
                    'totalAmount': order_group.to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history
                }
            else:
                table = {
                    'totalAmount': order_group.to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history,
                    "show_message": show_message
                }
            response = {'message': 'success', 'data': data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)

    def ride_fare(self, order, time_zone, start_timestamp, end_timestamp, group_by, export, currency,
                  conversion_rate, currency_symbol):
        try:
            order = pd.DataFrame(order)
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            try:
                order['typeId'] = order['typeId'].apply(lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                order['typeId'] = order['typeId'].astype(str)
            order['fare'] = order["fare"].astype(float) * conversion_rate
            # order['paymentType'] = order["paymentType"].replace({1: "card", 2: "cash", 3: "corporate_wallet"})
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            vehicle = order[["typeId", "typeName"]].drop_duplicates(subset=["typeId"], keep='last')

            order_copy = order.copy()
            order = order[['bookingDateTimestamp', 'typeId', 'fare']]

            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)

            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")

            if group_by == 3: order = Process.month_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 4: order = Process.quarter_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 7: order = Process.day_sort(data_frame=order, date_column="bookingDateTimestamp")
            # for col in ['paymentType_card', 'paymentType_cash', 'paymentType_corporate_wallet']:
            #     order[col] = order[col].astype(int)
            order_group = pd.pivot_table(order, values=['fare'],
                                         index=['bookingDateTimestamp', 'typeId'],
                                         aggfunc=np.sum, fill_value=0)
            order_group = order_group.reset_index()

            mapping = {}
            for index in list(vehicle.index):
                mapping[vehicle["typeId"].loc[index]] = vehicle["typeName"].loc[index]
            order_group = pd.merge(order_group, vehicle, on="typeId", how="left")
            order_group = order_group.drop("typeId", axis=1)
            order_group = order_group.rename(columns={"typeName": "typeId"})
            order_group['bookingDateTimestamp'] = order_group['bookingDateTimestamp'].astype(str)

            # construction of history data
            history = {}
            for index in range(order_group.shape[0]):
                if history.get(order_group['bookingDateTimestamp'].loc[index]):
                    value = history[order_group['bookingDateTimestamp'].loc[index]]
                    value.append({"Vehicle Type": str(order_group['typeId'].loc[index]),
                                  "fare": round(float(order_group['fare'].loc[index]), 2),
                                  # "cash": int(order_group['paymentType_cash'].loc[index]),
                                  # "corporate wallet": int(order_group['paymentType_corporate_wallet'].loc[index]),
                                  })
                    history[order_group['bookingDateTimestamp'].loc[index]] = value
                else:
                    history[order_group['bookingDateTimestamp'].loc[index]] = [{
                        "Vehicle Type": str(order_group['typeId'].loc[index]),
                        "fare": round(float(order_group['fare'].loc[index]), 2),
                        # "cash": int(order_group['paymentType_cash'].loc[index]),
                        # "corporate wallet": int(order_group['paymentType_corporate_wallet'].loc[index]),

                    }]
            for key, value in history.items():
                present_vehicle_type = [call['Vehicle Type'] for call in value]
                v_type = list(mapping.values())
                insert_vehicle_type = list(filter(lambda x: x not in present_vehicle_type, v_type))
                for v_type in insert_vehicle_type:
                    value.insert(0, {'Vehicle Type': v_type, 'fare': 0})
                history[key] = list(filter(lambda x: x['Vehicle Type'] != "nan", value))

            # Table data construction
            order = order[
                ['bookingDateTimestamp', 'fare']]
            order_group = order.groupby(['bookingDateTimestamp']).sum().reset_index()

            if group_by == 3: order_group = Process.month_sort(data_frame=order_group,
                                                               date_column="bookingDateTimestamp")
            if group_by == 4: order_group = Process.quarter_sort(data_frame=order_group,
                                                                 date_column="bookingDateTimestamp")
            if group_by == 7: order_group = Process.day_sort(data_frame=order_group, date_column="bookingDateTimestamp")
            show_message = 0
            if export:
                pass
            else:
                if order_group.shape[0] > 1000: show_message = 1
                order_group = order_group.head(1000)

            if export:
                table = {
                    'totalAmount': order_group.to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history
                }
            else:
                table = {
                    'totalAmount': order_group.to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history,
                    "show_message": show_message
                }
            response = {'message': 'success', 'data': data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)

    def surge_fare(self, order, time_zone, conversion_rate, start_timestamp, end_timestamp, group_by, export,
                   currency_symbol):
        try:
            order = pd.DataFrame(order)
            # ----------------- Columns Formatting -----------------
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            order['bookingDateTimestamp'] = order['bookingDateTimestamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            try:
                order['typeId'] = order['typeId'].apply(lambda x: x.oid if not isinstance(x, int) else str(x))
            except:
                order['typeId'] = order['typeId'].astype(str)
            order['totalFare'] = order['totalFare'].astype(float) * conversion_rate
            order['normalFare'] = order[['surgeApplied', "totalFare"]].apply(lambda x: x[1] if not x[0] else 0, axis=1)
            order['surgeFare'] = order[['surgeApplied', "totalFare"]].apply(lambda x: x[1] if x[0] else 0, axis=1)
            order_copy = order.copy()
            vehicle = order[["typeId", "typeName"]].drop_duplicates(subset=["typeId"], keep='last')
            order = order[['bookingDateTimestamp', 'typeId', 'normalFare', "surgeFare"]]
            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='bookingDateTimestamp',
                                        group_by=group_by)

            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="bookingDateTimestamp")

            if group_by == 3: order = Process.month_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 4: order = Process.quarter_sort(data_frame=order, date_column="bookingDateTimestamp")
            if group_by == 7: order = Process.day_sort(data_frame=order, date_column="bookingDateTimestamp")
            for col in ['normalFare', 'surgeFare']:
                order[col] = order[col].astype(float)
            order_group = pd.pivot_table(order, values=['normalFare', 'surgeFare'],
                                         index=['bookingDateTimestamp', 'typeId'],
                                         aggfunc=np.sum, fill_value=0)
            order_group = order_group.reset_index()

            mapping = {}
            for index in list(vehicle.index):
                mapping[vehicle["typeId"].loc[index]] = vehicle["typeName"].loc[index]
            order_group = pd.merge(order_group, vehicle, on="typeId", how="left")
            order_group = order_group.drop("typeId", axis=1)
            order_group = order_group.rename(columns={"typeName": "typeId"})
            order_group['bookingDateTimestamp'] = order_group['bookingDateTimestamp'].astype(str)

            # ----------------- construction of history data -----------------
            history = {}
            for index in range(order_group.shape[0]):
                if history.get(order_group['bookingDateTimestamp'].loc[index]):
                    value = history[order_group['bookingDateTimestamp'].loc[index]]
                    value.append({"Vehicle Type": str(order_group['typeId'].loc[index]),
                                  "normalFare": int(order_group['normalFare'].loc[index]),
                                  "surgeFare": int(order_group['surgeFare'].loc[index]),
                                  })
                    history[order_group['bookingDateTimestamp'].loc[index]] = value
                else:
                    history[order_group['bookingDateTimestamp'].loc[index]] = [{
                        "Vehicle Type": str(order_group['typeId'].loc[index]),
                        "normalFare": int(order_group['normalFare'].loc[index]),
                        "surgeFare": int(order_group['surgeFare'].loc[index]),

                    }]
            for key, value in history.items():
                present_vehicle_type = [call['Vehicle Type'] for call in value]
                v_type = list(mapping.values())
                insert_vehicle_type = list(filter(lambda x: x not in present_vehicle_type, v_type))
                for v_type in insert_vehicle_type:
                    value.insert(0, {'Vehicle Type': v_type, 'normalFare': 0, 'surgeFare': 0})
                history[key] = list(filter(lambda x: x['Vehicle Type'] != "nan", value))

            # ----------------- Table data construction -----------------
            order = order[['bookingDateTimestamp', "normalFare", "surgeFare"]]
            order_group = order.groupby(['bookingDateTimestamp']).sum().reset_index()

            if group_by == 3: order_group = Process.month_sort(data_frame=order_group,
                                                               date_column="bookingDateTimestamp")
            if group_by == 4: order_group = Process.quarter_sort(data_frame=order_group,
                                                                 date_column="bookingDateTimestamp")
            if group_by == 7: order_group = Process.day_sort(data_frame=order_group, date_column="bookingDateTimestamp")
            show_message = 0
            if export:
                pass
            else:
                if order_group.shape[0] > 1000: show_message = 1
                order_group = order_group.head(1000)

            order = order_copy[['bookingDateTimestamp', 'normalFare', "surgeFare"]]
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
            if export:
                pass
            else:
                bookings_group_graph = bookings_group_graph.head(1000)
            if export:
                table = {
                    'totalAmount': order_group.rename(
                        columns={'bookingDateTimestamp': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                                 'normalFare': 'Normal Fare',
                                 'surgeFare': 'Surge Fare',
                                 }).to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history
                }
            else:
                graph = {
                    'series': [
                        {
                            'name': 'Normal Fare',
                            'data': list(bookings_group_graph['normalFare'])
                        },
                        {
                            'name': 'Surge Fare',
                            'data': list(bookings_group_graph['surgeFare'])
                        },

                    ],
                    'xaxis': {
                        'title': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                        'categories': list(bookings_group_graph['bookingDateTimestamp'])
                    },
                    'yaxis': {'title': 'Fare({})'.format(currency_symbol)}
                }
                table = {
                    'totalAmount': order_group.rename(
                        columns={'bookingDateTimestamp': '{}'.format(GROUP_BY_SUPPORT[group_by]),
                                 'normalFare': 'Normal Fare',
                                 'surgeFare': 'Surge Fare',
                                 }).to_dict(orient='list')
                }
                data = {
                    'graph': graph,
                    'table': table,
                    'history': history,
                    "show_message": show_message
                }
            response = {'message': 'success', 'data': data}
            return RideResponses.get_status_200(response=response)
        except Exception as ex:
            return RideResponses.get_status_500(ex=ex)
