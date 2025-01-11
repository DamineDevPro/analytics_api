from django.test import TestCase


# Create your tests here.
class TestFilterReport(TestCase):
    def __init__(self):
        self.end_point = "/total-order/filter-report"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_store_id(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timezone(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0"})
        self.assertEqual(response.status_code, 400)

    def error_group_by(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0",
             "time_zone": "Asia/Calcutta",
             "group_by": 50
             })
        self.assertEqual(response.status_code, 422)

    def error_columns(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0",
             "time_zone": "Asia/Calcutta",
             "group_by": 50,
             "column": [15, 16, 17, 18, 19, 21]
             })
        self.assertEqual(response.status_code, 422)


    def error_timedelta_incorrect(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0",
             "time_zone": "Asia/Calcutta",
             "group_by": 50,
             "column": [15, 16, 17, 18, 19, 21],
             "start_timestamp": 1,
             "end_timestamp": "mustBeInt"

             })
        self.assertEqual(response.status_code, 422)

    def error_skip_limit(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0",
             "time_zone": "Asia/Calcutta",
             "group_by": 50,
             "column": [15, 16, 17, 18, 19, 21],
             "start_timestamp": 1,
             "end_timestamp": 2,
             "skip": 1,
             "limit": "mustBeInt"

             })
        self.assertEqual(response.status_code, 422)

    def error_export(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0",
             "time_zone": "Asia/Calcutta",
             "group_by": 50,
             "column": [15, 16, 17, 18, 19, 21],
             "start_timestamp": 1,
             "end_timestamp": 2,
             "skip": 0,
             "limit": 10,
             "export": 10

             })
        self.assertEqual(response.status_code, 422)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"store_id": "0",
             "time_zone": "Asia/Calcutta",
             "group_by": 50,
             "column": [15, 16, 17, 18, 19, 21],
             "start_timestamp": 1,
             "end_timestamp": 2,
             "skip": 0,
             "limit": 10,
             "export": 0

             })
        self.assertEqual(response.status_code, 200)
