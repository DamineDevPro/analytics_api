from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/truck/utilisation',
         views.Utilisation.as_view(), name='TruckUtilisation'),
    path('dashboard/truck/efficiency/dispatch',
         views.Dispatch.as_view(), name='TruckUtilisation'),
    path('dashboard/truck/efficiency/delivery',
         views.Delivery.as_view(), name='TruckUtilisation'),
    path('dashboard/truck/transporter/acceptance',
         views.Acceptance.as_view(), name='TruckUtilisation'),
    path('dashboard/truck/transporter/performance',
         views.Performance.as_view(), name='TruckUtilisation'),
]
