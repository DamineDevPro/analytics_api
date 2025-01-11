from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^export', views.Export.as_view(), name='Export'),
]
