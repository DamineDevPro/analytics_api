from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^promo/usage$', views.PromoUsage.as_view(), name='PromoUsage'),
    url(r'^promo/top$', views.TopPromo.as_view(), name='TopPromo'),
    url(r'^promo/analysis/count$', views.PromoCountAnalysis.as_view(), name='PromoCountAnalysis'),

]
