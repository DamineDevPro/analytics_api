from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^top_product/product_data', views.TopProductData.as_view(), name='TopProductData'),
    url(r'^top_product/brand_data', views.TopBrandData.as_view(), name='TopBrandData'),
    url(r'^top_product/product_percent', views.TopProductPercentage.as_view(), name='TopProductPercentage'),
    url(r'^top_product/brand_percent', views.TopBrandPercentage.as_view(), name='TopBrandPercentage'),
    url(r'^top_product/categories_data', views.TopCategoriesData.as_view(), name='TopCategoriesData'),
    url(r'^top_product/categories_percent', views.TopCategoriesPercentage.as_view(), name='TopCategoriesPercentage'),
    url(r'^top_product/performance_graph', views.ProductGraph.as_view(), name='ProductGraph'),

]
