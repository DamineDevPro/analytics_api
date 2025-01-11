from django.conf.urls import url
from . import views

urlpatterns = [
    # url(r'^total-order/current-report', views.CurrentReport.as_view(), name='current-report'),
    # url(r'^total-order/historical-report', views.HistoricalReport.as_view(), name='historical-report'),
    # url(r'^total-order/merge-report', views.MergeReport.as_view(), name='merge-report'),
    url(r'^total-order/filter-report', views.FilterReport.as_view(), name='filter-report'),
]
