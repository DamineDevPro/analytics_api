from django.test import TestCase


# Create your tests here.

class TestFunnelPlatform(TestCase):
    def __init__(self):
        self.end_point = "/funnel/platform"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_timedelta_missing(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timedelta_incorrect(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 2, "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 1, "end_timestamp": 2})
        self.assertEqual(response.status_code, 400)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 1604862743, "end_timestamp": 1714862743})
        self.assertEqual(response.status_code, 200)


class TestFunnelDevice(TestCase):
    def __init__(self):
        self.end_point = "/funnel/device"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_timedelta_missing(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_timedelta_incorrect(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 2, "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 1, "end_timestamp": 2})
        self.assertEqual(response.status_code, 400)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"start_timestamp": 1604862743, "end_timestamp": 1714862743})
        self.assertEqual(response.status_code, 200)