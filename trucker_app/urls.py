from django.conf.urls import url
from . import views

urlpatterns = [
    # --------------------- fare Dashboard ---------------------
    url(r'^trucker/detailed/totalsales', views.DetailedTotalSales.as_view(), name='DetailedTotalSales'),
    url(r'^trucker/sales/report', views.DescriptiveSalesReport.as_view(), name='DescriptiveSalesReport'),
    # url(r'^trucker/detailed/totalorders', views.DetailedTotalOrders.as_view(), name='DetailedTotalOrders'),
    url(r'^trucker/orders/report', views.DescriptiveOrderReport.as_view(), name='DescriptiveOrderReport'),
    # url(r'^trucker/fare$', views.TruckerFare.as_view(), name='TruckerFare'),
    # url(r'^trucker/table/fare$', views.TruckerFareTable.as_view(), name='TruckerFareTable'),
    # # --------------------- Count Dashboard ---------------------
    # url(r'^trucker/count$', views.TruckerCount.as_view(), name='truckerCount'),
    # url(r'^trucker/table/count$', views.TruckerCountTable.as_view(), name='TruckerCountTable'),
    # # --------------------- Trucker Payment Dashboard ---------------------
    # url(r'^trucker/payment$', views.Payment.as_view(), name='PaymentActivity'),
    # # --------------------- Trucker Payment Dashboard ---------------------
    # url(r'^trucker/status$', views.TruckerStatus.as_view(), name='TruckerStatusActivity'),
    # # --------------------- Top N location Dashboard ---------------------
    # url(r'^trucker/top/location$', views.TopLocation.as_view(), name='TopLocation'),
    # # --------------------- Heat Map Dashboard ---------------------
    # url(r'^trucker/heatmap$', views.Map.as_view(), name='Map'),
    # # --------------------- Support API ---------------------
    # url(r'^trucker/vehicle-type$', views.VehicleType.as_view(), name='VehicleType'),
    # url(r'^trucker/sales/report$', views.DescriptiveSalesReport.as_view(), name='TruckerDescriptiveSalesReport'),
    # # --------------------- Surge Dashboard ---------------------
    # url(r'^trucker/surge', views.SurgePrice.as_view(), name='SurgePrice'),
]
