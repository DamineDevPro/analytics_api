from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^sla/demand/meat', views.DemandMeat.as_view(), name='DemandMeat'),
    url(r'^sla/demand/product', views.ProductDemand.as_view(), name='ProductDemand'),
    url(r'^sla/store', views.Store.as_view(), name='Store'),

]
