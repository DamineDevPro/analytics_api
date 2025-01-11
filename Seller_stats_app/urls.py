from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^sellerstats', views.SellerStats.as_view(), name='SellerStats'),
    url(r'^stats/catalog', views.CatalogStats.as_view(), name='CatalogStats'),
    url(r'^stats/store', views.Stores.as_view(), name='Stores'),
    url(r'^stats/brands', views.BrandStats.as_view(), name='BrandStats'),
    url(r'^stats/leafcategory', views.LeafCategory.as_view(), name='LeafCategory'),
]
