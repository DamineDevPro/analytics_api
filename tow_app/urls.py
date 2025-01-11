from django.conf.urls import url
from . import views

urlpatterns = [
    # --------------------- fare Dashboard ---------------------
    url(r'^tow/fare$', views.RideFare.as_view(), name='TowFare'),
    url(r'^tow/table/fare$', views.RideFareTable.as_view(), name='TowFareTable'),
    # --------------------- Count Dashboard ---------------------
    url(r'^tow/count$', views.RideCount.as_view(), name='towCount'),
    url(r'^tow/table/count$', views.RideCountTable.as_view(), name='TowCountTable'),
    # --------------------- Tow Payment Dashboard ---------------------
    url(r'^tow/payment$', views.Payment.as_view(), name='PaymentActivity'),
    # --------------------- Tow Payment Dashboard ---------------------
    url(r'^tow/status$', views.RideStatus.as_view(), name='TowStatusActivity'),
    # --------------------- Top N location Dashboard ---------------------
    url(r'^tow/top/location$', views.TopLocation.as_view(), name='TopLocation'),
    # --------------------- Heat Map Dashboard ---------------------
    url(r'^tow/heatmap$', views.Map.as_view(), name='Map'),
    # --------------------- Support API ---------------------
    url(r'^tow/vehicle-type$', views.VehicleType.as_view(), name='VehicleType'),
    url(r'^tow/sales/report$', views.DescriptiveSalesReport.as_view(), name='TowDescriptiveSalesReport'),
    # --------------------- Surge Dashboard ---------------------
    url(r'^tow/surge', views.SurgePrice.as_view(), name='SurgePrice'),
]
