from django.conf.urls import url
from . import views


urlpatterns = [
    # url(r'^python-spark/dashboard/totalSales', views.TotalSaleOverview.as_view(), name='TotalSaleOverview'),
    # url(r'^python-spark/dashboard/totalOrder', views.TotalOrderOverview.as_view(), name='TotalOrderOverview'),
    # url(r'^python-spark/dashboard/v2/totalSales', views.TotalSale.as_view(), name='TotalSale'),
    # url(r'^python-spark/dashboard/v2/totalOrder', views.TotalOrder.as_view(), name='TotalOrder'),
    url(r'^python-spark/dashboard/top-product', views.TopNProduct.as_view(), name='TopNProduct'),
    url(r'^python-spark/dashboard/totalSales', views.TotalSales.as_view(), name='TotalSales'),
    url(r'^python-spark/dashboard/totalOrders', views.TotalOrders.as_view(), name='TotalOrder'),
    url(r'^python-spark/dashboard/top-wish', views.TopWish.as_view(), name='TopWish'),
    url(r'^python-spark/dashboard/top-cart', views.TopCart.as_view(), name='TopCart'),
    url(r'^python-spark/dashboard/user-logs', views.UserSessionLogs.as_view(), name='UserSessionLogs'),
    url(r'^python-spark/store-category', views.StoreCategory.as_view(), name='StoreCategory'),
    url(r'^python-spark/dashboard/order/payment', views.OrderPaymentOverView.as_view(), name='OrderPaymentOverView'),
    url(r'^python-spark/dashboard/top-brand', views.TopBrand.as_view(), name='TopBrand'),
    url(r'^python-spark/dashboard/order/average-sales', views.AverageSales.as_view(), name='AverageSales'),
    url(r'^python-spark/dashboard/conversion', views.SessionConversion.as_view(), name='SessionConversion'),
]
