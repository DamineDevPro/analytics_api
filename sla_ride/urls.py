from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^sla/ride/booking', views.RideSla.as_view(), name='RideSla'),
    url(r'^sla/ride/performance', views.DriverPerformance.as_view(), name='DriverPerformance'),

]