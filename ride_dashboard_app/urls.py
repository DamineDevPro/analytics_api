from django.conf.urls import url
from . import views

urlpatterns = [
    # -------------------------------- fare Dashboard --------------------------------
    url(r'^dashboard/ride/revenue$', views.RideRevenue.as_view(), name='RideDashboardRevenue'),
    # -------------------------------- Count Dashboard --------------------------------
    url(r'^dashboard/ride/count$', views.RideCount.as_view(), name='RideDashboardCount'),
    # -------------------------------- status Dashboard --------------------------------
    url(r'^dashboard/ride/status$', views.RideStatus.as_view(), name='RideDashboardStatus'),
    # -------------------------------- payment Dashboard --------------------------------
    url(r'^dashboard/ride/payment$', views.RidePayment.as_view(), name='RideDashboardPayment'),
    # --------------------- Support API ---------------------
    url(r'^ride/countries$', views.Countries.as_view(), name='Countries'),
    url(r'^ride/cities$', views.Cities.as_view(), name='Cities'),
    url(r'^ride/zones$', views.Zones.as_view(), name='Zones'),
]
