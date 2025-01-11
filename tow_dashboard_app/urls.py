from django.conf.urls import url
from . import views

urlpatterns = [
    # -------------------------------- fare Dashboard --------------------------------
    url(r'^dashboard/tow/revenue$', views.RideRevenue.as_view(), name='RideDashboardRevenue'),
    # -------------------------------- Count Dashboard --------------------------------
    url(r'^dashboard/tow/count$', views.RideCount.as_view(), name='RideDashboardCount'),
    # -------------------------------- status Dashboard --------------------------------
    url(r'^dashboard/tow/status$', views.RideStatus.as_view(), name='RideDashboardStatus'),
    # -------------------------------- payment Dashboard --------------------------------
    url(r'^dashboard/tow/payment$', views.RidePayment.as_view(), name='RideDashboardPayment'),
    # --------------------- Support API ---------------------
    url(r'^tow/countries$', views.Countries.as_view(), name='Countries'),
    url(r'^tow/cities$', views.Cities.as_view(), name='Cities'),
    url(r'^tow/zones$', views.Zones.as_view(), name='Zones'),
]
