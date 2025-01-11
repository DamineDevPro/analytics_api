from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^allproexport', views.Export.as_view(), name='AllProExport'),
    url(r'^completedloads', views.Loads.as_view(), name='AllProExportLoads'),
]
