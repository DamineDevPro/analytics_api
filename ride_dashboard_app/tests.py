from django.test import TestCase


# Create your tests here.
class TestCountries(TestCase):
    def __init__(self):
        self.end_point = "/ride/countries"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, )
        self.assertEqual(response.status_code, 200)


class TestCities(TestCase):
    def __init__(self):
        self.end_point = "/ride/cities"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_country_id(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, )
        self.assertEqual(response.status_code, 400)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"country_id": "5df7b7218798dc2c1114e6c0"})
        self.assertEqual(response.status_code, 200)


class TestZones(TestCase):
    def __init__(self):
        self.end_point = "/ride/zones"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 200)


class TesRideRevenue(TestCase):
    def __init__(self):
        self.end_point = "/ride/zones"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_device(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"device_type": 5})
        self.assertEqual(response.status_code, 422)

    def error_booking_status(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"device_type": 0,
             "status": 50,

             })
        self.assertEqual(response.status_code, 400)

    def error_time_delta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"device_type": 0,
             "status": 1,
             "start_timestamp": 1,
             "end_timestamp": "mustBeInt"

             })
        self.assertEqual(response.status_code, 400)

    def error_time_zone(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"device_type": 0,
             "status": 1,
             "start_timestamp": 1,
             "end_timestamp": 2,
             "timezone": "Must Be 'Asia/Calcutta'"

             })
        self.assertEqual(response.status_code, 400)

    def error_group_by(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"device_type": 0,
             "status": 1,
             "start_timestamp": 1,
             "end_timestamp": 2,
             "timezone": "Asia/Calcutta",
             "group_by": 50
             })
        self.assertEqual(response.status_code, 400)

    def success(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(
            self.end_point,
            {"device_type": 0,
             "status": 1,
             "start_timestamp": 1,
             "end_timestamp": 2624365484,
             "timezone": "Asia/Calcutta",
             "group_by": 6
             })
        self.assertEqual(response.status_code, 400)
