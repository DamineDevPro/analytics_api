from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^sla/grocery/meat', views.GroceryMeat.as_view(), name='GroceryMeat'),
    url(r'^sla/grocery/dcDemand', views.DcDemand.as_view(), name='DcDemand'),
    url(r'^sla/grocery/fcStores', views.FCStores.as_view(), name='FCStores'),
]
