from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^trucker/dashboard/totalSales', views.TotalSales.as_view(), name='TotalSales'),
    url(r'^trucker/dashboard/totalOrders', views.TotalOrders.as_view(), name='TotalOrder'),
    url(r'^trucker/dashboard/user-logs', views.UserSessionLogs.as_view(), name='UserSessionLogs'),
    url(r'^trucker/store-category', views.StoreCategory.as_view(), name='StoreCategory'),
    url(r'^trucker/dashboard/order/average-sales', views.AverageSales.as_view(), name='AverageSales'),
    url(r'^trucker/dashboard/conversion', views.SessionConversion.as_view(), name='SessionConversion'),
    # -------------------------------- fare Dashboard --------------------------------
    url(r'^dashboard/trucker/revenue$', views.TruckerRevenue.as_view(), name='TruckerDashboardRevenue'),
    # -------------------------------- Count Dashboard --------------------------------
    url(r'^dashboard/trucker/count$', views.TruckerCount.as_view(), name='TruckerDashboardCount'),
    # -------------------------------- status Dashboard --------------------------------
    url(r'^dashboard/trucker/status$', views.TruckerStatus.as_view(), name='TruckerDashboardStatus'),
    # -------------------------------- payment Dashboard --------------------------------
    url(r'^dashboard/trucker/payment$', views.TruckerPayment.as_view(), name='TruckerDashboardPayment'),
    # --------------------- Support API ---------------------
    url(r'^trucker/countries$', views.Countries.as_view(), name='Countries'),
    url(r'^trucker/cities$', views.Cities.as_view(), name='Cities'),
    url(r'^trucker/zones$', views.Zones.as_view(), name='Zones'),
]
