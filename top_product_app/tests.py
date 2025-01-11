from django.test import TestCase


# Create your tests here.
class TestTopProductData(TestCase):
    """
        Test cases for Product Tabular data API for Product Performance

    """

    def date_time_missing(self):
        """
        start_timestamp and end_timestamp epoch missing
        :return:
        """
        response = self.client.get('/top_product/product_data')
        self.assertEqual(response.status_code, 400)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def skip_limit_invalid(self):
        """
        skip adn limit must be integer
        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": 0, "end_timestamp": 1, "skip": "MustBeInt",
                                    "limit": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def currency_unsupported(self):
        """

        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "currency": "RandomCurrency"})
        self.assertEqual(response.status_code, 404)

    def sort_by_unsupported(self):
        """
        unsupported sort_by
        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "UnsupportedSortBy"})
        self.assertEqual(response.status_code, 422)

    def ascending_unsupported(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "Product Revenue", "ascending": 5})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/product_data',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/product_data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def success_with_export(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/product_data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD", "export": 1})
        self.assertEqual(response.status_code, 200)


class TestTopBrandData(TestCase):
    """
    Test cases for Brand Tabular data API  for Product Performance
    """

    def date_time_missing(self):
        """
        start_timestamp and end_timestamp epoch missing
        :return:
        """
        response = self.client.get('/top_product/brand_data')
        self.assertEqual(response.status_code, 400)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def skip_limit_invalid(self):
        """
        skip adn limit must be integer
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": 0, "end_timestamp": 1, "skip": "MustBeInt",
                                    "limit": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def currency_unsupported(self):
        """
        Unsupported Currency
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "currency": "RandomCurrency"})
        self.assertEqual(response.status_code, 404)

    def sort_by_unsupported(self):
        """
        unsupported sort_by
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "UnsupportedSortBy"})
        self.assertEqual(response.status_code, 422)

    def ascending_unsupported(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "Product Revenue", "ascending": 5})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/brand_data',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/brand_data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def success_with_export(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/brand_data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD", "export": 1})
        self.assertEqual(response.status_code, 200)


class TestTopBrandPercentage(TestCase):
    """
        Test cases for Product Tabular data API for Product Performance

    """

    def date_time_missing(self):
        """
        start_timestamp and end_timestamp epoch missing
        :return:
        """
        response = self.client.get('/top_product/product_percent')
        self.assertEqual(response.status_code, 400)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def skip_limit_invalid(self):
        """
        skip adn limit must be integer
        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1, "skip": "MustBeInt",
                                    "limit": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def currency_unsupported(self):
        """

        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "currency": "RandomCurrency"})
        self.assertEqual(response.status_code, 404)

    def sort_by_unsupported(self):
        """
        unsupported sort_by
        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "UnsupportedSortBy"})
        self.assertEqual(response.status_code, 422)

    def ascending_unsupported(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "Product Revenue", "ascending": 5})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/product_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/product_percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def success_with_export(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/product_percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD", "export": 1})
        self.assertEqual(response.status_code, 200)


class TestTopBrandPercentage(TestCase):
    """
        Test cases for Brand Tabular data API for Product Performance

    """

    def date_time_missing(self):
        """
        start_timestamp and end_timestamp epoch missing
        :return:
        """
        response = self.client.get('/top_product/brand_percent')
        self.assertEqual(response.status_code, 400)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def skip_limit_invalid(self):
        """
        skip adn limit must be integer
        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1, "skip": "MustBeInt",
                                    "limit": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def currency_unsupported(self):
        """

        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "currency": "RandomCurrency"})
        self.assertEqual(response.status_code, 404)

    def sort_by_unsupported(self):
        """
        unsupported sort_by
        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "UnsupportedSortBy"})
        self.assertEqual(response.status_code, 422)

    def ascending_unsupported(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "Product Revenue", "ascending": 5})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/brand_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/brand_percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def success_with_export(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/brand_percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD", "export": 1})
        self.assertEqual(response.status_code, 200)


class TestTopCategoriesData(TestCase):
    """
        Test cases for Categories Tabular data API for Product Performance

    """

    def date_time_missing(self):
        """
        start_timestamp and end_timestamp epoch missing
        :return:
        """
        response = self.client.get('/top_product/categories_data')
        self.assertEqual(response.status_code, 400)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def skip_limit_invalid(self):
        """
        skip adn limit must be integer
        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": 0, "end_timestamp": 1, "skip": "MustBeInt",
                                    "limit": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def currency_unsupported(self):
        """

        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "currency": "RandomCurrency"})
        self.assertEqual(response.status_code, 404)

    def sort_by_unsupported(self):
        """
        unsupported sort_by
        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "UnsupportedSortBy"})
        self.assertEqual(response.status_code, 422)

    def ascending_unsupported(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "Product Revenue", "ascending": 5})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/categories_data',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/categories_data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def success_with_export(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/categories_data',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD", "export": 1})
        self.assertEqual(response.status_code, 200)


class TestTopCategoriesPercentage(TestCase):
    """
        Test cases for Brand Tabular data API for Product Performance

    """

    def date_time_missing(self):
        """
        start_timestamp and end_timestamp epoch missing
        :return:
        """
        response = self.client.get('/top_product/categories_percent')
        self.assertEqual(response.status_code, 400)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": "MustBeInt", "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def skip_limit_invalid(self):
        """
        skip adn limit must be integer
        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1, "skip": "MustBeInt",
                                    "limit": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def currency_unsupported(self):
        """

        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "currency": "RandomCurrency"})
        self.assertEqual(response.status_code, 404)

    def sort_by_unsupported(self):
        """
        unsupported sort_by
        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "UnsupportedSortBy"})
        self.assertEqual(response.status_code, 422)

    def ascending_unsupported(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1,
                                    "sort_by": "Product Revenue", "ascending": 5})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        unsupported ascending must be 0 or 1
        :return:
        """
        response = self.client.get('/top_product/categories_percent',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 400)

    def success(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/categories_percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "currency": "USD"})
        self.assertEqual(response.status_code, 200)

    def success_with_export(self):
        """
        success
        :return:
        """
        response = self.client.get(
            '/top_product/categories_percent',
            {"start_timestamp": 0, "end_timestamp": 2000000000, "skip": 0, "limit": 10,
             "sort_by": "Product Revenue", "ascending": 1, "currency": "USD", "export": 1})
        self.assertEqual(response.status_code, 200)


class TestProductGraph(TestCase):

    def date_time_missing_incorrect(self):
        """
        incorrect timezone
        :return:
        """
        response = self.client.get('/top_product/performance_graph', {"timezone": "Incorrect Timezone"})
        self.assertEqual(response.status_code, 400)

    def group_by_incorrect(self):
        """
        group_by support ranges between 0 to 7
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"timezone": "Asia/Calcutta", "group_by": "incorrect"})
        self.assertEqual(response.status_code, 400)

    def group_by_unsupport(self):
        """
        group_by support ranges between 0 to 7
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"timezone": "Asia/Calcutta", "group_by": 10})
        self.assertEqual(response.status_code, 422)

    def date_time_unsupported(self):
        """
        start_timestamp and end_timestamp epoch must be integer only
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"timezone": "Asia/Calcutta", "start_timestamp": "MustBeInt",
                                    "end_timestamp": "MustBeInt"})
        self.assertEqual(response.status_code, 422)

    def date_time_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"start_timestamp": 1, "end_timestamp": 0})
        self.assertEqual(response.status_code, 422)

    def currency_invalid(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"start_timestamp": 0, "end_timestamp": 1, "currency": "InvalidCurrency"})
        self.assertEqual(response.status_code, 422)

    def no_data_found(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"start_timestamp": 0, "end_timestamp": 1})
        self.assertEqual(response.status_code, 422)

    def success(self):
        """
        start_timestamp must be smaller then end_timestamp epoch
        :return:
        """
        response = self.client.get('/top_product/performance_graph',
                                   {"start_timestamp": 0, "end_timestamp": 20000000, "group_by": 4})
        self.assertEqual(response.status_code, 422)
