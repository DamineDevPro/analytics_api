from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^wishlist', views.WishList.as_view(), name='WishList'),

]