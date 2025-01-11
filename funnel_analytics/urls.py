from django.conf.urls import url
from . import views
    
urlpatterns = [
    url(r'^funnel/platform', views.FunnelPlatform.as_view(), name='FunnelPlatform'),
    url(r'^funnel/device', views.Device.as_view(), name='Device'),
]
