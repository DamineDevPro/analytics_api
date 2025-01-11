from django.conf.urls import url
from . import views

urlpatterns = [
    # --------------------- fare Dashboard ---------------------
    url(r'^ride/fare$', views.RideFare.as_view(), name='RideFare'),
    url(r'^ride/table/fare$', views.RideFareTable.as_view(), name='RideFareTable'),
    # --------------------- Count Dashboard ---------------------
    url(r'^ride/count$', views.RideCount.as_view(), name='RideCount'),
    url(r'^ride/table/count$', views.RideCountTable.as_view(), name='RideCountTable'),
    # --------------------- Ride Payment Dashboard ---------------------
    url(r'^ride/payment$', views.Payment.as_view(), name='PaymentActivity'),
    # --------------------- Ride Payment Dashboard ---------------------
    url(r'^ride/status$', views.RideStatus.as_view(), name='RideStatusActivity'),
    # --------------------- Top N location Dashboard ---------------------
    url(r'^ride/top/location$', views.TopLocation.as_view(), name='TopLocation'),
    # --------------------- Heat Map Dashboard ---------------------
    url(r'^ride/heatmap$', views.Map.as_view(), name='Map'),
    # --------------------- Support API ---------------------
    url(r'^ride/vehicle-type$', views.VehicleType.as_view(), name='VehicleType'),
    url(r'^ride/sales/report$', views.DescriptiveSalesReport.as_view(), name='RideDescriptiveSalesReport'),
    # --------------------- Surge Dashboard ---------------------
    url(r'^ride/surge', views.SurgePrice.as_view(), name='SurgePrice'),
]
