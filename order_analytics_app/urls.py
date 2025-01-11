from django.conf.urls import url
from . import views

urlpatterns = [
    # url(r'^python-spark/current-report', views.CurrentReport.as_view(), name='current-report'),
    # url(r'^python-spark/historical-report', views.HistoricalReport.as_view(), name='historical-report'),
    # url(r'^python-spark/merge-report', views.MergeReport.as_view(), name='merge-report'),
    url(r'^python-spark/filter-report', views.FilterReport.as_view(), name='filtering-report'),
    # url(r'^python-spark/sales/descriptive-sales', views.DescriptiveSales.as_view(), name='DescriptiveSales'),
    url(r'^python-spark/sales/report', views.DescriptiveSalesReport.as_view(), name='DescriptiveSalesReport'),
    url(r'^python-spark/column-name', views.Columns.as_view(), name='columns'),
]
