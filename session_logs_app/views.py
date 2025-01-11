from django.shortcuts import render
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from rest_framework import status
from django.http import JsonResponse
from django.utils import timezone
from rest_framework.views import APIView
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext
# from pyspark.sql import functions as F
from analytics.settings import  UTC, db
from dateutil import relativedelta
import requests
from analytics.function import Process
import ast
import json
import pytz
import os, sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()


class UserSessionLogs(APIView):
    def get(self, request):
        """
        GET API for number of active user and number of unique active user in respective time frame as per device type
        :param request: 
        :return: 
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            if 'timezone' not in request.GET:
                response = {'message': 'timezone is missing'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            if "start_timestamp" not in request.GET or "end_timestamp" not in request.GET:
                response = {"message": "mandatory field 'start_timestamp' and 'end_timestamp' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    start_timestamp = int(request.GET["start_timestamp"])
                    end_timestamp = int(request.GET["end_timestamp"])
                except:
                    response = {"message": "Incorrect timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
                # date validation
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month", 4: "quarter", 5: 'year', 6: "Hour of day",
                              7: "Day of week"}
            if "group_by" not in request.GET:
                response = {'message': 'mandatory Field group_by'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                group_by = request.GET["group_by"]
                try:
                    group_by = int(group_by)
                    if group_by not in list(group_by_value.keys()):
                        response = {
                            "message": "'group_by' value must be in range(0, {})".format(
                                max(list(group_by_value.keys())))}
                        return JsonResponse(response, safe=False, status=422)
                except:
                    return JsonResponse({"message": "'group_by' must be integer"}, safe=False, status=422)

            device_type = {0: 'all', 1: 'android', 2: 'ios', 3: "web"}
            if 'device' not in request.GET:
                response = {'message': 'mandatory Field device'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            else:
                try:
                    device = int(request.GET['device'])
                except:
                    response = {'message': 'device must be string',
                                'support': device_type}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
                if device not in list(device_type.keys()):
                    response = {'message': 'device not supported',
                                'support': device_type}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            date_range_query = "WHERE sessionStart BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            user_query = " AND usertype IN ('customer', 'user')"
            query = "SELECT sessionStart, userid from sessionLogs" + " " + date_range_query + user_query
            if device:
                device_query = " AND device=='{}'".format(device_type[device])
                query = query + device_query
            query = query.strip()
            try:
                assert False
                data = sqlContext.sql(query)
                data = data.toPandas()
            except:
                query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp},
                         "usertype": {"$in": ['customer', 'user']}}
                if device: query["device"] = device_type[device]
                params = {"sessionStart": 1, "userid": 1}
                data = pd.DataFrame(db.sessionLogs.find(query, params))

            if not data.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data['Total Count'] = 1
            data = data.rename(columns={"sessionStart": "dateTime"})

            data['dateTime'] = data['dateTime'].astype(int)
            data['dateTime'] = data['dateTime'].apply(lambda x: datetime.fromtimestamp(x, tz=time_zone))
            data["dateTime"] = data["dateTime"].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            data = data.fillna(0)

            # Date filler function called from Process Class, setting.py
            data = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                       time_zone=time_zone, data_frame=data,
                                       date_column="dateTime", group_by=group_by)

            data = Process.date_conversion(group_by=group_by, data_frame=data, date_col="dateTime")

            drop_data_frame = data.drop_duplicates(subset=["dateTime", "userid"], keep='last')
            drop_data_frame = drop_data_frame.drop("userid", axis=1)
            drop_data_frame = drop_data_frame.rename(columns={"Total Count": "Unique Count"})
            drop_data_frame = drop_data_frame.groupby(['dateTime']).sum().reset_index()
            drop_data_frame = drop_data_frame.sort_values(by='dateTime', ascending=True)
            data = data.drop("userid", axis=1)
            group_data_frame = data.groupby(['dateTime']).sum().reset_index()
            group_data_frame = group_data_frame.sort_values(by='dateTime', ascending=True)
            merge_data_frame = pd.merge(group_data_frame, drop_data_frame, on='dateTime')
            if group_by == 3: merge_data_frame = Process.month_sort(data_frame=merge_data_frame, date_column="dateTime")
            if group_by == 4: merge_data_frame = Process.quarter_sort(data_frame=merge_data_frame,
                                                                      date_column="dateTime")

            show_message = 0
            if merge_data_frame.shape[0] > 1000:
                show_message = 1
            merge_data_frame = merge_data_frame.head(1000)
            graph = {
                "series": [
                    {"name": "Total Count", "data": list(merge_data_frame['Total Count'])},
                    {"name": "Unique Count", "data": list(merge_data_frame['Unique Count'])}
                ],
                "xaxis": {"title": group_by_value[group_by], "categories": list(merge_data_frame['dateTime'])},
                "yaxis": {"title": "Number Of Session"}
            }
            name_change = {'dateTime': group_by_value[group_by].capitalize()}
            merge_data_frame = merge_data_frame.rename(columns=name_change)
            merge_data_frame = merge_data_frame.to_dict(orient='record')
            response = {'message': 'success',
                        'graph': graph,
                        'table': merge_data_frame,
                        "show_message": show_message}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class DauMau(APIView):
    def get(self, request):
        """
        GET API for daily active user, month active user and its ratio in respective time frame
        :param request: 
        :return: 
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            if 'timezone' not in request.GET:
                response = {'message': 'timezone is missing'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', 'issue': type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                start_timestamp = int(request.GET.get("start_timestamp", 0))
                end_timestamp = int(request.GET.get("end_timestamp", 0))
            except:
                response = {"message": "Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            # date validation
            if end_timestamp < start_timestamp:
                response = {"message": "end timestamp must be greater than start timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            # 30 days previous timestamp with respect to start_timestamp
            timestamp = datetime.fromtimestamp(start_timestamp, tz=time_zone) - timedelta(days=30)
            timestamp = datetime.timestamp(timestamp)

            query = "SELECT sessionStart, userid from sessionLogs"
            if start_timestamp or end_timestamp:
                date_range_query = "WHERE sessionStart BETWEEN {} AND {} AND usertype IN ('customer', 'user')".format(
                    timestamp, end_timestamp)
            query = query + " " + date_range_query
            query = query.strip()
            try:
                assert False
                data = sqlContext.sql(query)
                data_frame = data.toPandas()
            except:
                query = {"usertype": {"$in": ['customer', 'user']}}
                if start_timestamp or end_timestamp:
                    query["sessionStart"] = {"$gte": start_timestamp, "$lte": end_timestamp}
                params = {"sessionStart": 1, "userid": 1}
                data_frame = pd.DataFrame(db.sessionLogs.find(query, params))

            if not data_frame.shape[0]:
                response = {"message": "No data found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data_frame['date'] = data_frame['sessionStart'].apply(lambda x: datetime.fromtimestamp(x, tz=time_zone))
            data_frame["date"] = data_frame["date"].apply(lambda x: datetime(day=x.day, month=x.month, year=x.year))
            dau_data_frame = data_frame[['userid', 'date']]
            dau_data_frame = dau_data_frame.groupby(by='date')['userid'].nunique().reset_index()
            dau_data_frame = dau_data_frame.rename(columns={'userid': 'DAU'})
            start_date = datetime.fromtimestamp(start_timestamp, tz=time_zone)
            start_date = datetime(day=start_date.day, month=start_date.month, year=start_date.year)
            end_date = datetime.fromtimestamp(end_timestamp, tz=time_zone)
            end_date = datetime(day=end_date.day, month=end_date.month, year=end_date.year)

            dau_data_frame = dau_data_frame[(dau_data_frame.date >= start_date) & (dau_data_frame.date <= end_date)]
            dau_data_frame = dau_data_frame.sort_values(by='date', ascending=True)
            mau = []
            for index in dau_data_frame.index:
                start_date = dau_data_frame.loc[index, 'date']
                end_date = start_date - timedelta(days=30)
                count = len(
                    data_frame[(data_frame.date >= end_date) & (data_frame.date <= start_date)]["userid"].unique())
                mau.append({'date': start_date, 'MAU': count})
            mau_df = pd.DataFrame(mau)
            table = dau_data_frame.merge(mau_df, on='date')
            table['DAU/MAU'] = (table['DAU'] / table['MAU']) * 100
            graph = {
                "series": [
                    {"name": "DAU", "data": list(table['DAU'])},
                    {"name": "MAU", "data": list(table['MAU'])},
                    {"name": "DAU/MAU", "data": list(table['DAU/MAU'])},
                ],
                "xaxis": {"title": 'DATE', "categories": list(table['date'])},
                "yaxis": {"title": "Value"}
            }
            response = {'message': 'success', 'data': {"graph": graph, "table": table.to_dict(orient='records')}}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class Manufacturer(APIView):
    def get(self, request):
        """
        GET API: Device manufacture count with the number of models with respect to affiliated brands(Device Session Report)
        :param request:
                        start_timestamp(integer): epoch time in seconds
                        end_timestamp(integer)  : epoch time in seconds
        :return: tabular and graphical data with 200 response status
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {"message": "Unauthorized"}
                return JsonResponse(response_data, safe=False, status=401)

            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                export = int(request.GET.get("export", 0))
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                final_response = {"message": message, "data": []}
                return JsonResponse(final_response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            device_support = {1: "mobile", 2: "web"}
            try:
                device = int(request.GET.get("device", 1))
                device_test = device_support[device]
            except:
                response = {"message": "'device' must be integer", "support": device_support}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                export = int(request.GET.get('export', 0))
                if export not in [0, 1]:
                    response = {'message': 'export only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            date_range_query = "WHERE sessionStart BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            device_query = {1: " AND device IN ('android', 'ios')",
                            2: " AND device == 'web'"}

            query = "SELECT sessionStart, deviceId, deviceModel, make from sessionLogs" + " " + date_range_query + \
                    device_query[device]
            query = query.strip()
            try:
                assert False
                active_data = sqlContext.sql(query)
                active_data = active_data.toPandas()
            except:
                query = {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp},
                         "device": {1: {"$in": ['android', 'ios']}, 2: 'web'}[device]}
                params = {"sessionStart": 1, "deviceId": 1, "deviceModel": 1, "make": 1}
                active_data = pd.DataFrame(db.sessionLogs.find(query, params))

            if not active_data.shape[0]:
                response = {'message': "Data Not Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
            active_data_frame = pd.DataFrame(active_data)
            unique_data_frame = active_data_frame.drop_duplicates(subset='deviceId', keep="last")
            unique_data_frame = unique_data_frame[['sessionStart', 'make', 'deviceModel']]
            unique_data_frame['Unique Count'] = 1
            active_data_frame = active_data_frame[['sessionStart', 'make', 'deviceModel']]
            active_data_frame['Total Count'] = 1
            active_data_frame = active_data_frame.merge(unique_data_frame,
                                                        on=['sessionStart', 'make', 'deviceModel'],
                                                        how='left')
            device = active_data_frame.copy()
            device = device.fillna(0)
            device['Unique Count'] = device['Unique Count'].astype(int)
            device['Total Count'] = device['Total Count'].astype(int)
            device_group = device[['make', 'Total Count', 'Unique Count']].groupby(by='make').sum().reset_index()
            device_group = device_group.sort_values(by='Total Count', ascending=False)
            graph = {
                "series": [
                    {"name": "Total Count", "data": list(device_group['Total Count'])},
                    {"name": "Unique Count", "data": list(device_group['Unique Count'])},
                ],
                "labels": list(device_group['make']),
                'xaxis': "Equipment Manufacturer",
                'yaxis': 'Device Session'
            }
            table = device_group.to_dict(orient='record')
            models = {}
            for manufacturer in list(device.make.unique()):
                dummy_df = device[device.make == manufacturer][['deviceModel', 'Total Count', 'Unique Count']]
                dummy_df = dummy_df.groupby(by='deviceModel').sum().reset_index()
                dummy_df = dummy_df.sort_values(by='Total Count', ascending=False)
                models[manufacturer] = dummy_df.to_dict(orient='record')
            if export:
                response = {'message': 'success',
                            'data': {
                                'table': {"tabular_data": table, "history": models}
                            }
                            }
            else:
                response = {'message': 'success',
                            'data': {'graph': graph,
                                     'table': {"tabular_data": table, "history": models}
                                     }
                            }
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class SessionDeviceLogs(APIView):
    def get(self, request):
        """
        GET API to view the graph and table with respect to the active or inactive device os version by session in the
        respective time frame
        :param request:
                        start_timestamp(integer): epoch time in seconds
                        end_timestamp(integer)  : epoch time in seconds
                        device(integer)         : only support {1: 'ANDROID', 2: 'IOS'}
                        activity(integer)       : only support {1: 'Active', 2: 'Inactive'}
        :return: tabular and graphical data with 200 response status
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {"message": "Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            device_type = {1: 'android', 2: 'ios', 3: "web"}
            try:
                device = device_type[int(request.GET['device'])]
            except:
                response = {'message': "missing/incorrect 'device' parameter", 'support': device_type}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                export = int(request.GET.get('export', 0))
                if export not in [0, 1]:
                    response = {'message': 'export only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            activity_status = {1: 'Active', 2: 'Inactive'}
            try:
                activity = int(request.GET['activity'])
                if activity not in list(activity_status.keys()):
                    response = {'message': 'activity not supported', 'supported value': activity_status}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {'message': 'activity must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            session_activity = {1: 'sessionStart', 2: 'sessionEnd'}
            date_range_query = "WHERE {} BETWEEN {} AND {} and device == '{}'".format(
                session_activity[activity], start_timestamp, end_timestamp, device)
            query = "SELECT osVersion from sessionLogs" + " " + date_range_query
            query = query.strip()
            try:
                assert False
                query_data = sqlContext.sql(query)
                query_data = query_data.toPandas()
            except:
                query = {session_activity[activity]: {"$gte": start_timestamp, "$lte": end_timestamp},
                         "device": device
                         }
                params = {"osVersion": 1}
                query_data = pd.DataFrame(db.sessionLogs.find(query, params))

            if not query_data.shape[0]:
                response = {'message': "Data Not Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data_frame = pd.DataFrame(query_data)
            data_frame['count'] = 1
            group_data_frame = data_frame.groupby(['osVersion']).sum().reset_index()
            table = group_data_frame.rename(
                columns={'osVersion': '{} Version'.format(device.capitalize()),
                         'count': '{} User'.format(activity_status[activity])})
            table_columns = list(table.columns)
            table = group_data_frame.to_dict(orient='list')
            if export:
                data = {
                    'table': {'header': table_columns, 'data': table}
                }
            else:
                data = {
                    'graph': {
                        'categories': list(group_data_frame['osVersion']),
                        'series': [
                            {'name': '{} User'.format(activity_status[activity]),
                             'data': list(group_data_frame['count'])}
                        ]
                    },
                    'table': {'header': table_columns, 'data': table}
                }
            response = {'message': 'success', 'graph': data}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class Install(APIView):
    def get(self, request):
        """
        api to get the first session with respect to manufacturer and device(Device Type Install Report)
        :param request:
        :return:
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            try:
                export = int(request.GET.get('export', 0))
                if export not in [0, 1]:
                    response = {'message': 'export only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {"message": "Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            query = "SELECT make, deviceId, deviceModel, min(sessionStart) FROM sessionLogs WHERE sessionStart BETWEEN {} AND {} AND device IN ('android', 'ios') GROUP BY make, deviceId, deviceModel ORDER BY min(sessionStart)".format(
                start_timestamp, end_timestamp)
            query = query.strip()
            try:
                assert False
                data_frame = sqlContext.sql(query)
                data_frame = data_frame.toPandas()
            except:
                query = [
                    {"$match": {"sessionStart": {"$gte": start_timestamp, "$lte": end_timestamp},
                                "device": {"$in": ['android', 'ios']}
                                }},
                    {"$group": {'_id': {"make": '$make', "deviceId": '$deviceId', 'deviceModel': '$deviceModel'},
                                "min(sessionStart)": {"$min": "$sessionStart"}}},
                    {"$sort": {"min(sessionStart)": 1}},
                    {"$project": {
                        "make": "$_id.make",
                        "deviceId": "$_id.deviceId",
                        "deviceModel": "$_id.deviceModel",
                        "min(sessionStart)": "min(sessionStart)",
                    }}
                ]
                data_frame = pd.DataFrame(db.sessionLogs.aggregate(query))
                data_frame = data_frame.drop("_id", axis=1, errors="ignore")
            if not data_frame.shape[0]:
                response = {'message': "Data Not Found"}
                return JsonResponse(response, safe=False, status=status.HTTP_404_NOT_FOUND)
            data_frame = data_frame.rename(columns={"min(sessionStart)": "sessionStart"})
            data_frame['Count'] = 1
            device_group = data_frame[['make', 'Count']].groupby(by='make').sum().reset_index()
            device_group = device_group.sort_values(by='Count', ascending=False)
            graph = {
                "series": [
                    {"name": "Count", "data": list(device_group['Count'])},
                ],
                "labels": list(device_group['make']),
                'xaxis': "Equipment Manufacturer",
                'yaxis': 'Device Session'
            }
            table = device_group.to_dict(orient='record')
            models = {}
            for manufacturer in list(data_frame.make.unique()):
                dummy_df = data_frame[data_frame.make == manufacturer][['deviceModel', 'Count']]
                dummy_df = dummy_df.groupby(by='deviceModel').sum().reset_index()
                dummy_df = dummy_df.sort_values(by='Count', ascending=False)
                models[manufacturer] = dummy_df.to_dict(orient='record')
            if export:
                response = {'message': 'success',
                            'data': {
                                'table': {
                                    "totalAmount": table,
                                    "history": models
                                }}}
            else:
                response = {'message': 'success',
                            'data': {'graph': graph,
                                     'table': {
                                         "totalAmount": table,
                                         "history": models
                                     }}}
            return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)


class Payment(APIView):
    def get(self, request):
        """
        Order activity with respect to card and cash aggregated as per time period drill down to store categories

        :param request:
        :return:
        """
        try:

            if 'HTTP_AUTHORIZATION' not in request.META:
                response = {'message': 'AUTHORIZATION is missing in header'}
                return JsonResponse(response, safe=False, status=status.HTTP_401_UNAUTHORIZED)
            token = request.META['HTTP_AUTHORIZATION']
            if token == "":
                response_data = {
                    "message": "Unauthorized",
                }
                return JsonResponse(response_data, safe=False, status=401)

            # Store Id param check - mandatory field
            try:
                store_id = str(request.GET['store_id'])
                store_id = "" if store_id == "0" else store_id
            except:
                response = {"message": "mandatory field 'store_id' missing"}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                time_zone = pytz.timezone(str(request.GET['timezone']))
            except Exception as e:
                response = {'message': 'TIME ISSUE', "error": type(e).__name__}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            try:
                start_timestamp = int(request.GET["start_timestamp"])
                end_timestamp = int(request.GET["end_timestamp"])
                if end_timestamp < start_timestamp:
                    response = {"message": "end timestamp must be greater than start timestamp"}
                    return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)
            except:
                response = {"message": "Incorrect timestamp"}
                return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            group_by_value = {0: "Hour", 1: "Day", 2: "Week", 3: "Month",
                              4: "Quarter", 5: 'Year', 6: "Hour of Day", 7: "Day of Week"}
            try:
                group_by = int(request.GET.get("group_by", 0))
                group_by_period = group_by_value[group_by]
            except:
                return JsonResponse({"message": "'group_by' must be integer", "support": group_by_value},
                                    safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)

            try:
                export = int(request.GET.get('export', 0))
                if export not in [0, 1]:
                    response = {'message': 'export only support 0 and 1'}
                    return JsonResponse(response, safe=False, status=status.HTTP_422_UNPROCESSABLE_ENTITY)
            except:
                response = {'message': 'export must be integer'}
                return JsonResponse(response, safe=False, status=status.HTTP_400_BAD_REQUEST)

            store_categories_id = str(request.GET.get("store_categories_id", ""))
            store_categories_id = "" if store_categories_id == "0" else store_categories_id
            store_categories_query = ""
            if store_categories_id:
                store_categories_query = " AND storeCategoryId == '{}'".format(store_categories_id)

            # store query addition
            store_query = " AND storeId == '{}'".format(store_id) if store_id else ""
            ######################### createdTimeStamp ############################################
            date_range_query = "WHERE createdTimeStamp BETWEEN {} AND {}".format(start_timestamp, end_timestamp)
            query = "SELECT createdTimeStamp, storeCategoryId, storeCategory, paymentType from storeOrder" \
                    + " " + date_range_query + store_categories_query + store_query
            query = query.strip()
            try:
                assert False
                order = sqlContext.sql(query)
                order = order.toPandas()
            except:
                query = {"createdTimeStamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
                if store_categories_id != "0" and store_categories_id: query["storeCategoryId"] = store_categories_id
                if store_id: query["storeId"] = store_id
                params = {"createdTimeStamp": 1, "storeCategoryId": 1, "storeCategory": 1, "paymentType": 1}
                order = pd.DataFrame(db.storeOrder.find(query, params))

            if not order.shape[0]:
                response = {'message': 'No data found'}
                return JsonResponse(response, safe=False, status=status.HTTP_204_NO_CONTENT)
            order = pd.DataFrame(order)
            store = order[["storeCategoryId", "storeCategory"]].drop_duplicates(subset=["storeCategoryId"], keep='last')
            order['createdTimeStamp'] = order['createdTimeStamp'].apply(
                lambda x: datetime.fromtimestamp(x, tz=time_zone))
            order['paymentType'] = order["paymentType"].astype(int)
            order['paymentType'] = order["paymentType"].replace({1: "card", 2: "cash"})
            order['createdTimeStamp'] = order['createdTimeStamp'].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
            order_copy = order.copy()
            order = order[['createdTimeStamp', 'storeCategoryId', 'paymentType']]

            unique_order = list(order.paymentType.unique())
            if len(unique_order) == 1:
                if unique_order[0] == 'card':
                    order = order.rename(columns={'paymentType': 'paymentType_card'})
                    order['paymentType_card'] = 1
                    order['paymentType_cash'] = 0
                elif unique_order[0] == 'cash':
                    order = order.rename(columns={'paymentType': 'paymentType_cash'})
                    order['paymentType_card'] = 0
                    order['paymentType_cash'] = 1
                else:
                    pass
            else:
                order = pd.get_dummies(order, columns=['paymentType'])

            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='createdTimeStamp',
                                        group_by=group_by)

            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="createdTimeStamp")

            if group_by == 3: order = Process.month_sort(data_frame=order, date_column="createdTimeStamp")
            if group_by == 4: order = Process.quarter_sort(data_frame=order, date_column="createdTimeStamp")
            if group_by == 7: order = Process.day_sort(data_frame=order, date_column="createdTimeStamp")

            order_group = pd.pivot_table(order, values=['paymentType_card', 'paymentType_cash'],
                                         index=['createdTimeStamp', 'storeCategoryId'],
                                         aggfunc=np.sum, fill_value=0)
            order_group = order_group.reset_index()

            mapping = {}
            for index in list(store.index):
                mapping[store["storeCategoryId"].loc[index]] = store["storeCategory"].loc[index]
            order_group = pd.merge(order_group, store, on="storeCategoryId", how="left")
            order_group = order_group.drop("storeCategoryId", axis=1)
            order_group = order_group.rename(columns={"storeCategory": "storeCategoryId"})
            order_group['createdTimeStamp'] = order_group['createdTimeStamp'].astype(str)

            # construction of history data
            history = {}
            for index in range(order_group.shape[0]):
                if history.get(order_group['createdTimeStamp'].loc[index]):
                    value = history[order_group['createdTimeStamp'].loc[index]]
                    value.append({"Store Category": str(order_group['storeCategoryId'].loc[index]),
                                  "card": int(order_group['paymentType_card'].loc[index]),
                                  "cash": int(order_group['paymentType_cash'].loc[index]), })
                    history[order_group['createdTimeStamp'].loc[index]] = value
                else:
                    history[order_group['createdTimeStamp'].loc[index]] = [{
                        "Store Category": str(order_group['storeCategoryId'].loc[index]),
                        "card": int(order_group['paymentType_card'].loc[index]),
                        "cash": int(order_group['paymentType_cash'].loc[index]),
                    }]
            for key, value in history.items():
                present_call_type = [call['Store Category'] for call in value]
                call_type = list(mapping.values())
                insert_call_type = list(filter(lambda x: x not in present_call_type, call_type))
                for call_type in insert_call_type:
                    value.insert(0, {'Store Category': call_type, 'card': 0, 'cash': 0})
                history[key] = list(filter(lambda x: x['Store Category'] != "nan", value))

            # Table data construction
            order = order[['createdTimeStamp', 'paymentType_card', 'paymentType_cash']]
            order_group = order.groupby(['createdTimeStamp']).sum().reset_index()

            if group_by == 3: order_group = Process.month_sort(data_frame=order_group, date_column="createdTimeStamp")
            if group_by == 4: order_group = Process.quarter_sort(data_frame=order_group, date_column="createdTimeStamp")
            if group_by == 7: order_group = Process.day_sort(data_frame=order_group, date_column="createdTimeStamp")
            show_message = 0
            if export:
                pass
            else:
                if order_group.shape[0] > 1000: show_message = 1
                order_group = order_group.head(1000)

            # Graph Data Construction
            order = order_copy[['createdTimeStamp', 'storeCategoryId', 'paymentType']]
            unique_order = list(order.paymentType.unique())
            if len(unique_order) == 1:
                if unique_order[0] == 'card':
                    order = order.rename(columns={'paymentType': 'paymentType_card'})
                    order['paymentType_card'] = 1
                    order['paymentType_cash'] = 0
                elif unique_order[0] == 'cash':
                    order = order.rename(columns={'paymentType': 'paymentType_cash'})
                    order['paymentType_card'] = 0
                    order['paymentType_cash'] = 1
                else:
                    pass
            else:
                order = pd.get_dummies(order, columns=['paymentType'])

            order = order[['createdTimeStamp', 'paymentType_card', 'paymentType_cash']]
            order = Process.date_filler(start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                        time_zone=time_zone, data_frame=order, date_column='createdTimeStamp',
                                        group_by=group_by)
            order = Process.date_conversion(group_by=group_by, data_frame=order, date_col="createdTimeStamp")
            bookings_group_graph = order.groupby(['createdTimeStamp']).sum().reset_index()
            bookings_group_graph = bookings_group_graph.sort_values(by='createdTimeStamp', ascending=True)

            if group_by == 3:
                bookings_group_graph = Process.month_sort(data_frame=bookings_group_graph,
                                                          date_column="createdTimeStamp")
            if group_by == 4:
                bookings_group_graph = Process.quarter_sort(data_frame=bookings_group_graph,
                                                            date_column="createdTimeStamp")
            if group_by == 7:
                bookings_group_graph = Process.day_sort(data_frame=bookings_group_graph, date_column="createdTimeStamp")
            if export:
                pass
            else:
                bookings_group_graph = bookings_group_graph.head(1000)

            if export:
                table = {
                    'totalAmount': order_group.rename(
                        columns={'createdTimeStamp': 'Time Line',
                                 'paymentType_card': 'Card',
                                 'paymentType_cash': 'Cash'}).to_dict(orient='list')
                }
                data = {
                    'table': table,
                    'history': history
                }
                response = {'message': 'success', 'data': data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
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
                        }
                    ],
                    'xaxis': {
                        'title': 'Time Line',
                        'categories': list(bookings_group_graph['createdTimeStamp'])
                    },
                    'yaxis': {
                        'title': 'Payment Count'
                    }
                }
                table = {
                    'totalAmount': order_group.rename(
                        columns={'createdTimeStamp': 'Time Line',
                                 'paymentType_card': 'Card',
                                 'paymentType_cash': 'Cash'}).to_dict(orient='list')
                }
                data = {
                    'graph': graph,
                    'table': table,
                    'history': history,
                    "show_message": show_message
                }
                response = {'message': 'success', 'data': data}
                return JsonResponse(response, safe=False, status=status.HTTP_200_OK)
        except Exception as ex:
            traceback.print_exc()

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            finalResponse = {"message": message, "data": []}
            return JsonResponse(finalResponse, safe=False, status=500)
