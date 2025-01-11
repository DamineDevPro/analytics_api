from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^python-spark/session/user-logs', views.UserSessionLogs.as_view(), name='UserSessionLogs'),
    url(r'^python-spark/session/dau-mau', views.DauMau.as_view(), name='DauMau'),
    url(r'^python-spark/session/manufacturer', views.Manufacturer.as_view(), name='Manufacturer'),
    url(r'^python-spark/session/device-log', views.SessionDeviceLogs.as_view(), name='SessionDeviceLogs'),
    url(r'^python-spark/session/install', views.Install.as_view(), name='Install'),
    url(r'^python-spark/order/payment', views.Payment.as_view(), name='Payment'),

]
