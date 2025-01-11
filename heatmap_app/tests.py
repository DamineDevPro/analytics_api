from django.test import TestCase


# Create your tests here.
class TestZone(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/zones"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_city_id(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"city_id": "NO_CONTENT"})
        self.assertEqual(response.status_code, 204)

    def success_zone(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"city_id": "5e271475e382023b611ddf87"})
        self.assertEqual(response.status_code, 200)


class TestCities(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/cities"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def error_country_id(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 400)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"country_id": "NO_CONTENT"})
        self.assertEqual(response.status_code, 204)

    def success_cities(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"country_id": "5df7b7218798dc2c1114e6c0"})
        self.assertEqual(response.status_code, 200)


class TestCountries(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/countries"

    # -------------------- GET API --------------------
    def error_auth(self):
        response = self.client.get(self.end_point)
        self.assertEqual(response.status_code, 401)

    def success_cities(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"country_id": "5df7b7218798dc2c1114e6c0"})
        self.assertEqual(response.status_code, 200)


class TestMap(TestCase):
    def __init__(self):
        self.end_point = "/python-spark/heatmap"

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
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "INCORRECT TIME ZONE"})
        self.assertEqual(response.status_code, 400)

    def error_group_by(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "group_by": 100})
        self.assertEqual(response.status_code, 422)

    def error_timedelta(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta"})
        self.assertEqual(response.status_code, 400)

    def error_no_content(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta", "start_timestamp": 1,
                                                    "end_timestamp": 2})
        self.assertEqual(response.status_code, 404)

    def success_map(self):
        self.client.defaults['HTTP_AUTHORIZATION'] = "authorization token"
        response = self.client.get(self.end_point, {"store_id": "0", "timezone": "Asia/Calcutta",
                                                    "start_timestamp": 1604871957,
                                                    "end_timestamp": 1694871957})
        self.assertEqual(response.status_code, 404)
