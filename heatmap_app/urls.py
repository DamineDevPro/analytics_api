from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^python-spark/heatmap', views.Map.as_view(), name='Map'),
    url(r'^python-spark/countries', views.Countries.as_view(), name='Countries'),
    url(r'^python-spark/cities', views.Cities.as_view(), name='Cities'),
    url(r'^python-spark/zones', views.Zones.as_view(), name='Zones'),

]
