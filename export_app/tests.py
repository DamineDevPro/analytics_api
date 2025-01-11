from django.test import TestCase


# Create your tests here.

class TestExport(TestCase):

    # -------------------------------------------- GET API --------------------------------------------
    def get_invalid_type_400(self):
        response = self.client.get('/export')
        self.assertEqual(response.status_code, 400)

    def get_store_id_400(self):
        response = self.client.get('/export', {"type": 1, "start_time": 1, "end_time": 2})
        self.assertEqual(response.status_code, 400)

    def get_no_content_204(self):
        response = self.client.get('/export', {"type": 1, "start_time": 1, "end_time": 2})
        self.assertEqual(response.status_code, 204)

    def get_success_200(self):
        response = self.client.get('/export', {"type": 1, "start_time": 1, "end_time": 2614760758})
        self.assertEqual(response.status_code, 200)

    # -------------------------------------------- POST API --------------------------------------------
    def post_invalid_type_400(self):
        response = self.client.post('/export')
        self.assertEqual(response.status_code, 400)

    def post_store_id_400(self):
        data = {"type": 1, "storeId": "0"}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    # -------------------------------------------- POST BUYER -------------------------------------------
    def post_buyer_missing_timedelta(self):
        data = {"type": 1, "storeId": "0"}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def post_buyer_no_content(self):
        data = {"type": 1, "storeId": "0", "start_time": 1, "end_time": 2}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 204)

    def post_buyer_success(self):
        data = {"type": 1, "storeId": "0", "start_time": 1, "end_time": 2614843897}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 204)

    # ------------------------------------------ POST INVENTORY -----------------------------------------
    def post_inventory_incorrect_service(self):
        data = {"type": 2, "storeId": "0"}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def post_inventory_no_data(self):
        data = {"type": 2, "storeId": "5c402722e05e3c7ff2362d32", "start_time": 1614847366, "end_time": 1624847366}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def post_inventory_success(self):
        data = {"type": 2, "storeId": "0", "start_time": 1, "end_time": 2}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 200)

    # -------------------------------------------- POST PROMO -------------------------------------------
    def post_promo_incorrect_service(self):
        data = {"type": 3, "storeId": "0"}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def post_promo_incorrect_timezone(self):
        data = {"type": 1, "storeId": "0"}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def post_promo_incorrect_timedelta(self):
        data = {"type": 3, "timezone": "Asia/Calcutta"}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def post_promo_incorrect_promo_service(self):
        data = {"type": 3, "timezone": "Asia/Calcutta", "service_type": 10}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def post_promo_no_content(self):
        data = {"type": 3, "type_text": "promo_logs", "service_type": "0", "storeId": "0",
                "timezone": "Asia/Calcutta", "start_time": 1, "end_time": 2}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def post_promo_success(self):
        data = {"type": 3, "type_text": "promo_logs", "service_type": "0", "storeId": "0",
                "timezone": "Asia/Calcutta", "start_time": 1, "end_time": 2612954202}
        response = self.client.post("/export", data, content_type="application/json")
        self.assertEqual(response.status_code, 200)

    # -------------------------------------------- POST ORDER -------------------------------------------
