from django.test import TestCase


# Create your tests here.


class TestTransactionData(TestCase):
    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/sales-performance/transaction-data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Transaction ID", "ascending": 1, "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def group_by_unsupported(self):
        """
        group_by must be 1, 2 or 3
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data', {"group_by": 5})
        self.assertEqual(response.status_code, 422)

    def group_by_invalid(self):
        """
        group_by must be integer
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data', {"group_by": "Unsupported_groupBy"})
        self.assertEqual(response.status_code, 404)

    def time_duration_missing(self):
        """
        start_timestamp and end_timestamp missing
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data')
        self.assertEqual(response.status_code, 404)

    def time_duration_unsupported(self):
        """
        start_timestamp and end_timestamp missing
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data',
                                   {"start_timestamp": "unsupported", "end_timestamp": "unsupported"})
        self.assertEqual(response.status_code, 404)

    def time_duration_invalid(self):
        """
        end_timestamp smaller than start_timestamp
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 404)

    def skip_limit_invalid(self):
        """
        skip and limit must be integer
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data',
                                   {"start_timestamp": 1, "end_timestamp": 0, "skip": "unsupported",
                                    "limit": "unsupported"})
        self.assertEqual(response.status_code, 422)

    def currency_invalid(self):
        """
        end_timestamp smaller than start_timestamp
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data',
                                   {"start_timestamp": 0, "end_timestamp": 1, "currency": "Unsupported"})
        self.assertEqual(response.status_code, 404)

    def sort_by_invalid(self):
        """
        unsupported sort_by options
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data',
                                   {"start_timestamp": 0, "end_timestamp": 1, "sort_by": "Unsupported"})
        self.assertEqual(response.status_code, 404)

    def no_data(self):
        """
        unsupported sort_by options
        :return:
        """
        response = self.client.get('/sales-performance/transaction-data',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 404)


class TestTransactionPercentage(TestCase):
    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/sales-performance/transaction-percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Transaction ID", "ascending": 1, "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def group_by_unsupported(self):
        """
        group_by must be 1, 2 or 3
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent', {"group_by": 5})
        self.assertEqual(response.status_code, 422)

    def group_by_invalid(self):
        """
        group_by must be integer
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent', {"group_by": "Unsupported_groupBy"})
        self.assertEqual(response.status_code, 404)

    def time_duration_missing(self):
        """
        start_timestamp and end_timestamp missing
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent')
        self.assertEqual(response.status_code, 404)

    def time_duration_unsupported(self):
        """
        start_timestamp and end_timestamp missing
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent',
                                   {"start_timestamp": "unsupported", "end_timestamp": "unsupported"})
        self.assertEqual(response.status_code, 404)

    def time_duration_invalid(self):
        """
        end_timestamp smaller than start_timestamp
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 404)

    def skip_limit_invalid(self):
        """
        skip and limit must be integer
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent',
                                   {"start_timestamp": 1, "end_timestamp": 0, "skip": "unsupported",
                                    "limit": "unsupported"})
        self.assertEqual(response.status_code, 422)

    def currency_invalid(self):
        """
        end_timestamp smaller than start_timestamp
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent',
                                   {"start_timestamp": 0, "end_timestamp": 1, "currency": "Unsupported"})
        self.assertEqual(response.status_code, 404)

    def sort_by_invalid(self):
        """
        unsupported sort_by options
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent',
                                   {"start_timestamp": 0, "end_timestamp": 1, "sort_by": "Unsupported"})
        self.assertEqual(response.status_code, 404)

    def no_data(self):
        """
        unsupported sort_by options
        :return:
        """
        response = self.client.get('/sales-performance/transaction-percent',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 404)
