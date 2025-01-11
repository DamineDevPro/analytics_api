from django.test import TestCase


# Create your tests here.


class TestUserSessionLogs(TestCase):
    """
    Test Case for TopWish GET API
    """

    def params_error(self):
        """
        missing timezone
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs')
        self.assertEqual(response.status_code, 400)

    def incorrect_timezone(self):
        """
        issue with timezone
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs', {"timezone": "RandomTimeZone"})
        self.assertEqual(response.status_code, 400)

    def missing_start_and_end(self):
        """
        missing start_timestamp and end_timestamp
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs', {"timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def incorrect_timestamp_error1(self):
        """
        start_timestamp and end_timestamp must be integers
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs', {"timezone": "Asia/Calcutta",
                                                                       "start_timestamp": "Must be Integer",
                                                                       "end_timestamp": "Must be Integer"})
        self.assertEqual(response.status_code, 400)

    def incorrect_timestamp_error2(self):
        """
        end_timestamp must be greater than start_timestamp
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def incorrect_timestamp_error3(self):
        """
        issue while converting the start time epoch and end time epoch in date time format
        :return:
        """
        epoch = 10 ** 20
        response = self.client.get('/python-spark/session/user-logs',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": epoch, "end_timestamp": epoch + 10})
        self.assertEqual(response.status_code, 422)

    def incorrect_groupby(self):
        """
        issue while converting the start time epoch and end time epoch in date time format
        :return:
        """
        epoch = 10 ** 20
        response = self.client.get('/python-spark/session/user-logs',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 0, "end_timestamp": 1,
                                    "group_by": 100})
        self.assertEqual(response.status_code, 422)

    def incorrect_device(self):
        """
        issue while converting the start time epoch and end time epoch in date time format
        :return:
        """
        epoch = 10 ** 20
        response = self.client.get('/python-spark/session/user-logs',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 0, "end_timestamp": 1,
                                    "group_by": 1, "device": 100})
        self.assertEqual(response.status_code, 422)

    def not_data(self):
        """
        missing data between the respective time frame
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs', {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 404)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get('/python-spark/session/user-logs',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 0, "end_timestamp": 2540536539,
                                    "group_by": 1, "device": 1})
        self.assertEqual(response.status_code, 200)


class TestDauMau(TestCase):
    def missing_timezone(self):
        """
        missing timezone
        :return:
        """
        response = self.client.get('/python-spark/session/dau-mau')
        self.assertEqual(response.status_code, 400)

    def incorrect_timezone(self):
        """
        incorrect timezone
        :return:
        """
        response = self.client.get('/python-spark/session/dau-mau', {"timezone": "RandomTimeZone"})
        self.assertEqual(response.status_code, 400)

    # def missing_time(self):
    #     """
    #     missing timezone
    #     :return:
    #     """
    #     response = self.client.get('/python-spark/session/dau-mau', {"timezone": "Asia/Calcutta"})
    #     self.assertEqual(response.status_code, 400)

    def incorrect_time1(self):
        """
        missing greater start time then end
        :return:
        """
        response = self.client.get('/python-spark/session/dau-mau',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 400)

    def incorrect_time2(self):
        """
        start_timestamp and end_timestamp must be integer
        :return:
        """
        response = self.client.get('/python-spark/session/dau-mau',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": "incorrect", "end_timestamp": 0})
        self.assertEqual(response.status_code, 400)

    def no_data(self):
        """
        no data found
        :return:
        """
        response = self.client.get('/python-spark/session/dau-mau',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get('/python-spark/session/dau-mau',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": 0, "end_timestamp": 2000000000})
        self.assertEqual(response.status_code, 400)


class TestManufacturer(TestCase):

    def incorrect_datetime(self):
        """
        start_timestamp must be smaller then end_timestamp
        :return:
        """
        response = self.client.get('/python-spark/session/manufacturer',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 400)

    def unsupport_datetime(self):
        """
        start_timestamp or end_timestamp must be integer
        :return:
        """
        response = self.client.get('/python-spark/session/manufacturer',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def invalid_device(self):
        """
        device only support {1: "mobile", 2: "web"}
        :return:
        """
        response = self.client.get('/python-spark/session/manufacturer',
                                   {"start_timestamp": 0, "end_timestamp": 1, "device": 3})
        self.assertEqual(response.status_code, 400)


    def no_data_found(self):
        """
        No data found in respective time frame
        :return:
        """
        response = self.client.get('/python-spark/session/manufacturer',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        No data found in respective time frame
        :return:
        """
        response = self.client.get('/python-spark/session/manufacturer',
                                   {"start_timestamp": 0, "end_timestamp": 1797431308})
        self.assertEqual(response.status_code, 400)