from datetime import datetime, timedelta
import pandas as pd
import os, sys
import requests
from .settings import CURRENCY_API, BASE_CURRENCY
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics.settings")

from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()


class Process:
    def __init__(self, group_by, data_frame, date_col, utc_start_timestamp, utc_end_timestamp, time_zone, date_column):
        self.group_by = group_by
        self.data_frame = data_frame
        self.date_col = date_col
        self.utc_start_timestamp = utc_start_timestamp
        self.utc_end_timestamp = utc_end_timestamp
        self.time_zone = time_zone
        self.data_frame = data_frame
        self.date_column = date_column

    @staticmethod
    def date_conversion(group_by, data_frame, date_col):
        """
        convert the date  column as per the group by parameter passed
        supported group by - {0: "hour", 1: "day", 2: "week", 3: "month",4: "quarter",
                              5: 'year', 6: "hour_of_day", 7: "day_of_week"}
        :param group_by:
        :param data_frame: pandas dataframe
        :param date_col: column name with datetime
        :return:  dataframe with changed datetime
        """
        if group_by == 0:
            pass
        elif group_by == 1:  # day
            data_frame[date_col] = data_frame[date_col].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year))
        elif group_by == 2:  # week
            data_frame[date_col] = data_frame[date_col].apply(
                lambda x: datetime(day=x.day, month=x.month, year=x.year) - timedelta(days=x.weekday()))
        elif group_by == 3:  # month
            data_frame[date_col] = data_frame[date_col].apply(lambda x: x.strftime('%b-%Y'))
        elif group_by == 4:  # quarter
            data_frame[date_col] = data_frame[date_col].apply(
                lambda x: "Q{} {}".format(getattr(x, "quarter"), x.year))
        elif group_by == 5:  # year
            data_frame[date_col] = data_frame[date_col].dt.year
        elif group_by == 6:  # hour_of_day
            # data_frame[date_col] = data_frame[date_col].apply(lambda x: x.strftime("%I:00 %p"))
            # changed as per UI request added date as 1/1/1980 so UI
            data_frame[date_col] = data_frame[date_col].apply(
                lambda x: datetime(day=1, month=1, year=1980, hour=x.hour))
        elif group_by == 7:  # day_of_week
            data_frame[date_col] = data_frame[date_col].apply(lambda x: x.strftime("%A"))
        else:
            pass
        return data_frame

    @staticmethod
    def date_filler(start_timestamp, end_timestamp, time_zone, data_frame, date_column, group_by):
        """
        fill out the missing date data in an data frame and merging accordingly
        :param start_timestamp: epoch time
        :param end_timestamp: epoch time
        :param time_zone: time zone (Asia/Calcutta)
        :param data_frame: pandas data frame
        :param date_column: pandas column name
        :param group_by: 0 to 1
        :return: converted data frame
        """

        if group_by == 0 or group_by == 6:
            adder = timedelta(hours=1)
        else:
            adder = timedelta(days=1)
        # min_time = datetime.utcfromtimestamp(start_timestamp)
        # min_time = min_time.replace(tzinfo=UTC).astimezone(time_zone)
        min_time = datetime.fromtimestamp(start_timestamp, tz=time_zone)
        # print("min_time", min_time, "converted:", min_time.replace(tzinfo=UTC).astimezone(time_zone))
        min_time = datetime(day=min_time.day, month=min_time.month, year=min_time.year, hour=min_time.hour)
        # max_time = datetime.utcfromtimestamp(end_timestamp)
        # max_time = max_time.replace(tzinfo=UTC).astimezone(time_zone)
        max_time = datetime.fromtimestamp(end_timestamp, tz=time_zone)
        # print("max_time", max_time, "converted:", max_time.replace(tzinfo=UTC).astimezone(time_zone))
        max_time = datetime(day=max_time.day, month=max_time.month, year=max_time.year, hour=max_time.hour)
        if group_by:
            # print(group_by)
            min_time = datetime(day=min_time.day, month=min_time.month, year=min_time.year)
            max_time = datetime(day=max_time.day, month=max_time.month, year=max_time.year)

        all_date = []
        date_time = min_time
        while date_time < max_time:
            all_date.append(date_time)
            date_time = date_time + adder
        all_date.append(max_time)
        all_date = set(all_date)
        data_frame_date = list(data_frame[date_column])
        # all_date = list(filter(lambda x: x not in data_frame_date, all_date))
        all_date.difference_update(data_frame_date)
        # print(all_date)
        append_df = pd.DataFrame()
        append_df[date_column] = list(all_date)
        for col in data_frame:
            if col != date_column:
                append_df[col] = 0
        # append_df[date_column] = append_df[date_column].apply(lambda x: x.replace(tzinfo=UTC).astimezone(time_zone))
        append_df[date_column] = append_df[date_column].apply(
            lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
        data_frame = data_frame.append(append_df, ignore_index=True)
        # data_frame = data_frame.sort_values(by=date_column).reset_index()
        # print(data_frame)
        return data_frame

    @staticmethod
    def month_sort(data_frame, date_column):
        """
        sorting the data frame as per month
        :param data_frame: pandas data frame
        :param date_column: pandas column name
        :return: return sorted data frame
        """
        month = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
        data_frame['sort_column'] = data_frame[date_column].apply(lambda x: x.split("-")[1] + month[x.split("-")[0]])
        data_frame = data_frame.sort_values(by='sort_column', ascending=True)
        data_frame = data_frame.drop("sort_column", axis=1, errors='ignore')
        return data_frame

    @staticmethod
    def quarter_sort(data_frame, date_column):
        """
        sorting the data frame as per quarter
        :param data_frame: pandas data frame
        :param date_column: pandas column name
        :return: return sorted data frame
        """

        data_frame['sort_column'] = data_frame[date_column].apply(lambda x: x.split(" ")[1] + x.split(" ")[0])
        data_frame = data_frame.sort_values(by='sort_column', ascending=True)
        data_frame = data_frame.drop("sort_column", axis=1, errors='ignore')
        return data_frame

    @staticmethod
    def day_sort(data_frame, date_column):
        """
                sorting the data frame as per day(Monday, Tuesday , etc)
                :param data_frame: pandas data frame
                :param date_column: pandas column name
                :return: return sorted data frame
                """
        day = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4, 'friday': 5, 'saturday': 6, 'sunday': 7}
        data_frame['sort_column'] = data_frame[date_column].apply(lambda x: day[x.lower()])
        data_frame = data_frame.sort_values(by='sort_column', ascending=True)
        data_frame = data_frame.drop("sort_column", axis=1, errors='ignore')
        return data_frame

    @staticmethod
    def filler(start_timestamp, end_timestamp, time_zone, data_frame, date_column, group_by):
        """
        fill out the missing date data in an data frame and merging accordingly
        :param start_timestamp: epoch time
        :param end_timestamp: epoch time
        :param time_zone: time zone (Asia/Calcutta)
        :param data_frame: pandas data frame
        :param date_column: pandas column name
        :param group_by: 0 to 1
        :return: converted data frame
        """
        if group_by == 0 or group_by == 6:
            adder = timedelta(hours=1)
        else:
            adder = timedelta(days=1)
        min_time = datetime.fromtimestamp(start_timestamp, tz=time_zone)
        min_time = datetime(day=min_time.day, month=min_time.month, year=min_time.year, hour=min_time.hour)
        max_time = datetime.fromtimestamp(end_timestamp, tz=time_zone)
        max_time = datetime(day=max_time.day, month=max_time.month, year=max_time.year, hour=max_time.hour)
        if group_by:
            min_time = datetime(day=min_time.day, month=min_time.month, year=min_time.year)
            max_time = datetime(day=max_time.day, month=max_time.month, year=max_time.year)
        all_date = []
        date_time = min_time
        while date_time < max_time:
            all_date.append(date_time)
            date_time = date_time + adder
        all_date.append(max_time)
        # all_date = set(all_date)
        # data_frame_date = list(data_frame[date_column])
        # all_date.difference_update(data_frame_date)
        append_df = pd.DataFrame()
        append_df[date_column] = list(all_date)
        for col in data_frame:
            if col != date_column:
                append_df[col] = 0
        append_df[date_column] = append_df[date_column].apply(
            lambda x: datetime(day=x.day, month=x.month, year=x.year, hour=x.hour))
        append_df = Process.date_conversion(group_by=group_by, data_frame=append_df, date_col=date_column)
        data_frame = data_frame.append(append_df, ignore_index=True)
        data_frame = data_frame.groupby([date_column]).sum().reset_index()
        return data_frame

    @staticmethod
    def restaurant_store_categories(db):
        """
        fill out the missing date data in an data frame and merging accordingly
        :param db: mongo database connection
        :return: list of restaurant categories mongo id
        """
        categories = list(db["storeCategory"].find({"type": 1}, {"_id": 1}))
        if categories: categories = [str(category["_id"]) for category in categories]
        return tuple(categories) if categories else ()

    @staticmethod
    def currency(currency):
        try:
            parameters = {"from_currency": BASE_CURRENCY, "to_currency": currency, "fetchSize": 1}
            auth = b'{"userId": "1", "userType": "admin", "metaData": {} }'
            headers = {"Authorization": auth, "lan": 'en'}
            currency_response = requests.get(CURRENCY_API, params=parameters, headers=headers)
            print("currency api status code---> ", currency_response.status_code)
            if currency_response.status_code != 200:
                response = {'message': 'Error while fetching currency rate', 'error': currency_response.content}
                return {"conversion_rate": None, "error_flag": 1,
                        "response_message": response, "response_status": currency_response.status_code}
            currency_data = json.loads(currency_response.content.decode('utf-8'))
            print(currency_data)
            if currency_data.get("data").get('data'):
                conversion_rate = float(currency_data.get("data").get('data')[0].get('exchange_rate'))
                return {"conversion_rate": conversion_rate, "error_flag": 0,
                        "response_message": None, "response_status": currency_response.status_code}
            else:
                response = {"message": "currency conversion not found"}
                return {"conversion_rate": None, "error_flag": 1,
                        "response_message": response, "response_status": 404}

        except Exception as ex:
            response = {'message': 'Internal server issue with exchange rate API', "error": type(ex).__name__}
            return {"conversion_rate": None, "error_flag": 1,
                    "response_message": response, "response_status": 500}
        
