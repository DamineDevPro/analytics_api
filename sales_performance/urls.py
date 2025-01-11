from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^sales-performance/transaction-data', views.TransactionData.as_view(), name='TransactionData'),
    url(r'^sales-performance/transaction-percent', views.TransactionPercentage.as_view(), name='TransactionPercentage'),

]
