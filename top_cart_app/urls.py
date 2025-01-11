from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^top-cart', views.Cart.as_view(), name='Cart'),

]