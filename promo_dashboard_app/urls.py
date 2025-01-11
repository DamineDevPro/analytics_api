from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^dashboard/promo/usage$', views.PromoUsage.as_view(), name='DashboardPromoUsage'),
    url(r'^dashboard/promo/top$', views.TopPromo.as_view(), name='TopPromo'),
]