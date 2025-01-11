from django.test import TestCase


# Create your tests here.
class TestCart(TestCase):
    def __init__(self):
        self.end_point = "/top-cart"

    # -------------------- GET API --------------------
    def error_timedelta_missing(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timedelta_incorrect(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 2, "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_skip_limit(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point,
                                   {"start_timestamp": 1, "end_timestamp": 2, "skip": 0, "limit": "mustBeInt"})
        self.assertEqual(response.status_code, 400)

    def error_timezone(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 1, "end_timestamp": 2, "skip": 0, "limit": 10})
        self.assertEqual(response.status_code, 400)

    def error_sort(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"start_timestamp": 1, "end_timestamp": 2, "skip": 0, "limit": 10,
             "timezone": "Asia/Calcutta", "sort": 12
             })
        self.assertEqual(response.status_code, 400)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"start_timestamp": 0, "end_timestamp": 2624365484, "skip": 0, "limit": 10,
             "timezone": "Asia/Calcutta", "sort": 1
             })
        self.assertEqual(response.status_code, 200)
