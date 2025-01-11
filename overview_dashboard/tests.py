from django.test import TestCase


# Create your tests here.


class TestTopWish(TestCase):
    """
    Test Case for TopWish GET API
    """

    def params_error(self):
        """
        missing start_timestamp and end_timestamp
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-wish')
        self.assertEqual(response.status_code, 400)

    def incorrect_timestamp_error1(self):
        """
        start_timestamp and end_timestamp must be integers
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-wish', {"start_timestamp": "Must be Integer",
                                                                        "end_timestamp": "Must be Integer"})
        self.assertEqual(response.status_code, 400)

    def incorrect_timestamp_error2(self):
        """
        end_timestamp must be greater than start_timestamp
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-wish', {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def incorrect_timestamp_error3(self):
        """
        issue while converting the start time epoch and end time epoch in date time format
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-wish', {"start_timestamp": 1000000000000000000000,
                                                                        "end_timestamp": 1000000000000000000000})
        self.assertEqual(response.status_code, 422)

    def not_data(self):
        """
        missing data between the respective time frame
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-wish', {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual((response.status_code, 404))

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-wish',
                                   {"start_timestamp": 0, "end_timestamp": 2540536539})
        self.assertEqual(response.status_code, 200)


class TestTopCart(TestCase):
    """
    Test Case for TopWish GET API
    """

    def params_error(self):
        """
        missing start_timestamp and end_timestamp
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-cart')
        self.assertEqual((response.status_code, 400))

    def incorrect_timestamp_error1(self):
        """
        start_timestamp and end_timestamp must be integers
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-cart', {"start_timestamp": "Must be Integer",
                                                                        "end_timestamp": "Must be Integer"})
        self.assertEqual(response.status_code, 400)

    def incorrect_timestamp_error2(self):
        """
        end_timestamp must be greater than start_timestamp
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-cart', {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def incorrect_timestamp_error3(self):
        """
        issue while converting the start time epoch and end time epoch in date time format
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-cart', {"start_timestamp": 1000000000000000000000,
                                                                        "end_timestamp": 1000000000000000000000})
        self.assertEqual(response.status_code, 422)

    def not_data(self):
        """
        missing data between the respective time frame
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-cart', {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 404)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get('/python-spark/dashboard/top-cart',
                                   {"start_timestamp": 0, "end_timestamp": 2540536539})
        self.assertEqual(response.status_code, 200)


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


class TestStoreCategory(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/store-category"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 200)


class TestOrderPaymentOverView(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/dashboard/order/payment"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_store(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timezone(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta_comparison(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_group_by(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta",
                                                    "start_timestamp": 1515363148,
                                                    "end_timestamp": 1615363148,
                                                    "group_by": 8})
        self.assertEqual(response.status_code, 422)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta",
                                                    "start_timestamp": 1,
                                                    "end_timestamp": 2,
                                                    })
        self.assertEqual(response.status_code, 204)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1615363148,
                                    "group_by": 4})
        self.assertEqual(response.status_code, 200)


class TestTopBrand(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/dashboard/top-brand"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_store(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timedelta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta_comparison(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_sort_by(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1,
                                                    "sort_by": 3})
        self.assertEqual(response.status_code, 422)

    def error_currency(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1, "currency": "INVALID_CURRENCY",
                                                    "sort_by": 1})
        self.assertEqual(response.status_code, 422)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1615363148,
                                    })
        self.assertEqual(response.status_code, 200)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1615363148,
                                    })
        self.assertEqual(response.status_code, 404)


class TestAverageSales(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/dashboard/order/average-sales"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_store(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timedelta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta_comparison(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_currency(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1, "currency": "INVALID_CURRENCY",
                                                    "sort_by": 1})
        self.assertEqual(response.status_code, 422)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1615363148,
                                    })
        self.assertEqual(response.status_code, 404)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1615363148,
                                    })
        self.assertEqual(response.status_code, 200)


class TestSessionConversion(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/dashboard/conversion"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_store(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timedelta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta_comparison(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1915363148,
                                    })
        self.assertEqual(response.status_code, 200)


class TestTotalOrders(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/dashboard/totalOrders"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_store(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timezone(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def error_timedelta_comparison(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_group_by(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta",
                                                    "start_timestamp": 1515363148,
                                                    "end_timestamp": 1615363148,
                                                    "group_by": 8})
        self.assertEqual(response.status_code, 422)

    def error_currency(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 2,
                                                    "end_timestamp": 1, "currency": "INVALID_CURRENCY",
                                                    "sort_by": 1})
        self.assertEqual(response.status_code, 422)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1,
                                    "end_timestamp": 2,
                                    })
        self.assertEqual(response.status_code, 404)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"store_id": "0", "timezone": "Asia/Calcutta",
                                    "start_timestamp": 1515363148,
                                    "end_timestamp": 1615363148,
                                    })
        self.assertEqual(response.status_code, 200)