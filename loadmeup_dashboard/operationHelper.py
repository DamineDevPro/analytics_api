import pandas as pd
from .dbHelper import DbHelper
from bson.objectid import ObjectId
from analytics.settings import db, UTC
from datetime import datetime
from analytics.function import Process
import asyncio

dbHelper = DbHelper()
date_format = '%m-%d-%Y'

class OperationHelper:

    @staticmethod
    async def pie_chart(pie_column, data) -> dict:

        data.sort_values(pie_column, ascending=False, inplace=True)
        summation = data[pie_column].sum()
        data[pie_column] = data[pie_column].apply(lambda x: round((x / summation) * 100, 2) if x else 0)
        pie_series = list(data[pie_column][:10])
        pie_label = list(data["Name"][:10])
        pie_label.append("other")
        pie_series.append(100 - sum(pie_series))

        return {"series": pie_series, "label": pie_label}
    
    @staticmethod
    async def bar_graph(data, s_name_1, s_name_2, x_title, y_title, x_cat) -> dict:

        return {"series": [
                    {"name": s_name_1, "data": list(data[s_name_1])},
                    {"name": s_name_2, "data": list(data[s_name_2])},
                ],
                "xaxis": {"title": x_title, "categories": list(data[x_cat])},
                "yaxis": {"title": y_title}
            }
    
    @staticmethod
    def count_driver_acceptance(x):
        data = {
            "Total Offered": x["status"].count(),
            "Total Accepted": x["status"][x["status"]==2].count(),
            "Total Cancelled": x["status"][x["status"]==5].count(),
            "Total Expired": x["status"][x["status"]==4].count(),
        }
        return pd.Series(data)
    
    @staticmethod
    def count_driver_dispatch(x):
        data = {
            "Total Offered": x["on_time"].count(),
            "Total Delayed": x["on_time"][x["on_time"]==-1].count(),
            "Total On-time": x["on_time"][x["on_time"]==0].count(),
            "Total Early": x["on_time"][x["on_time"]==1].count(),
        }
        return pd.Series(data)
    
    def driver_check(self, request):

        driver_type_value = {0: "company", 1: "independent"}
        if "driver_type" not in request.GET:
            driver_type = 0
        else:
            driver_type = request.GET["driver_type"]
            try:
                driver_type = int(driver_type)
                if driver_type not in list(driver_type_value.keys()):
                    return {"message": "'driver_type' value must be in range(0, {})".format(
                            max(list(driver_type_value.keys())))}
            except:
                return {"message": "'driver_type' must be integer"}
        return driver_type

    def group_by_check(self, request):
        group_by_value = {0: "hour", 1: "day", 2: "week", 3: "month",
                            4: "quarter", 5: 'year', 6: "hour_of_day", 7: "day_of_week"}
        if "group_by" not in request.GET:
            group_by = 0
        else:
            group_by = request.GET["group_by"]
            try:
                group_by = int(group_by)
                if group_by not in list(group_by_value.keys()):
                    return {"message": "'group_by' value must be in range(0, {})".format(
                            max(list(group_by_value.keys())))}
            except:
                return {"message": "'group_by' must be integer"}
        return group_by

    def vehicle_type(self, vehicle_type) -> pd.DataFrame:

        vehicle_type_df = dbHelper.vehicle_info(vehicle_type=vehicle_type)
        vehicle_type_df["vehicle_pvol"] = vehicle_type_df[["vehicleLength","vehicleWidth","vehicleHeight"]].astype('float').apply(
            lambda x: x["vehicleLength"]*x["vehicleWidth"]*x["vehicleHeight"], axis=1)

        return vehicle_type_df[["_id", "vehicle_pvol", "vehicle_pweight", "vehicle_length_metric"]]

    def utilization(self, data, time_zone, group_by):

        data["createdTimeStamp"] = data["createdTimeStamp"].apply(lambda x: datetime.fromtimestamp(x))
        data['createdDate'] = pd.to_datetime(data['createdTimeStamp'])
        data['createdDate'] = data['createdDate'].apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))

        # date converter as per group by
        data = Process.date_conversion(group_by=group_by, data_frame=data, date_col='createdDate')

        # data frame sorting
        data = data.sort_values(by='createdDate', ascending=True)
        if group_by == 3: data = Process.month_sort(data_frame=data, date_column="createdDate")
        if group_by == 4: data = Process.quarter_sort(data_frame=data, date_column="createdDate")
        if group_by == 7: data = Process.day_sort(data_frame=data, date_column="createdDate")

        data['createdDate'] = data['createdDate'].apply(lambda x : x.strftime(date_format))
        data["vehicle_typeId"] = data["vehicleDetails"].apply(lambda x: x["typeId"])
        data["vehicleId"] = data["vehicleDetails"].apply(lambda x: x["_id"])

        ############ To get truck VHU weight ############
        vehicle_type = list(map(lambda x: ObjectId(x), data["vehicle_typeId"].unique()))
        vehicle_type_df = self.vehicle_type(vehicle_type)

        data["packageWeight"] = data["packageBox"].apply(lambda x: x["weight"])
        data["packageWeight"] = data["packageBox"].apply(lambda x: x["weight"])
        data["packagevolume"] = data["packageBox"].apply(lambda x: int(x["lengthCapacity"])*int(x["widthCapacity"])\
            *int(x["heightCapacity"]))

        single_pickup_df = data[data["parentOrderId"]==""]
        multiple_pickup_df = data[data["parentOrderId"]!=""]

        single_pickup_df = single_pickup_df.groupby(["createdDate","vehicle_typeId","vehicleId"])["packageWeight", "packagevolume"].sum().reset_index()
        multiple_pickup_df = multiple_pickup_df.groupby(["createdDate","vehicle_typeId","vehicleId","parentOrderId"])["packageWeight", "packagevolume"].sum().reset_index()
        
        data = pd.concat([single_pickup_df, multiple_pickup_df])
        data = pd.merge(left=data, right=vehicle_type_df, left_on="vehicle_typeId", right_on="_id", how='left')
        data.dropna(subset=["vehicle_pweight","vehicle_pvol"], inplace=True)
        data[["vehicle_pweight","vehicle_pvol"]] = data[["vehicle_pweight","vehicle_pvol"]].astype('float')
        data=data.groupby(["createdDate"], as_index=False)["packageWeight", "packagevolume", "vehicle_pweight","vehicle_pvol"].sum().sort_values("createdDate")
        data["VHU Volume"] = data[["vehicle_pvol", "packagevolume"]].apply(lambda x: (int(x["packagevolume"])/int(x["vehicle_pvol"]))*100, axis=1)
        data["VHU Weight"] = data[["vehicle_pweight", "packageWeight"]].apply(lambda x: (int(x["packageWeight"])/int(x["vehicle_pweight"]))*100, axis=1)
        
        graph = OperationHelper.bar_graph(data=data, s_name_1='VHU Weight', 
            s_name_2='VHU Volume', x_title='DATE', y_title="In percentages(%)", x_cat='createdDate')
        return data, graph

    def dispatch(self, data):

        ############ On-time dispatch ############
        data["atPickup"] = data["timestamps"].apply(lambda x:x["atPickup"])
        data["time_diff"] = data["estimatedPickupTimestamp"] - data["atPickup"]
        data["on_time"] = data['time_diff'].apply(lambda x: -1 if x<0 else 0 if x==0 else 1 )

        data = data.groupby(["driverId", "Name", "email"], as_index=False).apply(OperationHelper.count_driver_dispatch)
        data.rename(columns={"email": "Email"}, inplace=True)

    def delivery(self, data):

        ############ On-time delivery ############
        pass

    async def acceptance(self, data, time_zone):

        ############ On-time acceptance ############
        data = data.explode("dispatched")
        data = data["dispatched"].apply(pd.Series)
        data["Name"] = data["firstName"] + " " + data["lastName"]
        data = data.groupby(["driverId", "Name", "email"], as_index=False).apply(OperationHelper.count_driver_acceptance)
        data.rename(columns={"email": "Email"}, inplace=True)

        task1 = asyncio.create_task(OperationHelper.bar_graph(data=data, s_name_1='Total Offered', 
            s_name_2='Total Accepted', x_title='Driver Name', y_title="Count", x_cat='Name'))
        
        pie_column = "Total Accepted"
        task2 = asyncio.create_task(OperationHelper.pie_chart(pie_column, data))

        await asyncio.gather(task1, task2)
        bar_graph = await task1
        pie_chart = await task2

        return data, bar_graph, pie_chart

    def performance(self, data):

        ############ On-time performance ############
        pass
