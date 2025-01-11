from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^tow/export$', views.RideExport.as_view(), name='RideExport'),
]
