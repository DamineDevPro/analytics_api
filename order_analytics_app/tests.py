from django.test import TestCase


# Create your tests here.


class TestDescriptiveSales(TestCase):
    def success(self):
        """
        success
        :return:
        """
        response = self.client.get('/python-spark/sales/descriptive-sales')
        self.assertEqual(response.status_code, 200)

    def internal_server(self):
        """
        internal server error
        :return:
        """
        response = self.client.get('/python-spark/sales/descriptive-sales')
        self.assertEqual(response.status_code, 500)
