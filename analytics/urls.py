"""analytics URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include
from django.conf.urls import url

urlpatterns = [
    url('admin/', admin.site.urls),
    url(r'^', include('order_analytics_app.urls')),     # TOTAL SALES API(MONGO DB)
    url(r'^', include('overview_dashboard.urls')),      # OVER-VIEW DASHBOARD API(MONGO DB)
    url(r'^', include('total_order_app.urls')),         # TOTAL ORDER'S API(MONGO DB)
    url(r'^', include('top_product_app.urls')),         # TOP N PRODUCTS API/PRODUCT PERFORMANCE(MONGO DB)
    url(r'^', include('top_wishlist_app.urls')),        # TOP N PRODUCTS IN WISH LIST API(CASSANDRA)
    url(r'^', include('top_cart_app.urls')),            # TOP N PRODUCTS IN CART API(CASSANDRA)
    url(r'^', include('session_logs_app.urls')),        # SESSION LOGS REPORT WITH RESPECT TO CUSTOMER(MONGO DB)
    url(r'^', include('sales_performance.urls')),       # SALES PERFORMANCE REPORT(MONGO DB)
    url(r'^', include('funnel_analytics.urls')),        # FUNNEL ANALYTICS (MONGO DB)
    url(r'^', include('heatmap_app.urls')),             # GRAPH HEATMAP (MONGO DB)
    url(r'^', include('sla_grocery.urls')),             # SLA GROCERY MEAT (MONGO DB)
    url(r'^', include('sla_ride.urls')),                # SLA RIDE (MONGO DB)
    url(r'^', include('promo_app.urls')),               # PROMO ANALYTICS
    url(r'^', include('demand_app.urls')),              # DEMAND ANALYTICS (MONGO DB)
    url(r'^', include('ride_app.urls')),                # RIDE ANALYTICS (MONGO DB)
    url(r'^', include('promo_dashboard_app.urls')),     # PROMO OVERVIEW DASHBOARD ANALYTICS (MONGO DB)
    url(r'^', include('export_app.urls')),              # EXPORT FUNCTIONALITY (MONGO DB)
    url(r'^', include('ride_dashboard_app.urls')),      # RIDE DASHBOARD ANALYTICS (MONGO DB)
    url(r'^', include('export_ride_app.urls')),         # EXPORT FUNCTIONALITY (MONGO DB)
    url(r'^', include('Seller_stats_app.urls')),        # Seller Stats API
    url(r'^', include('export_allpro.urls')),           # All-Pro Export API
    url(r'^', include('tow_app.urls')),                # tow ANALYTICS (MONGO DB)
    url(r'^', include('tow_dashboard_app.urls')),      # tow DASHBOARD ANALYTICS (MONGO DB)
    url(r'^', include('export_tow_app.urls')),         # EXPORT FUNCTIONALITY tow (MONGO DB)
    url(r'^', include('trucker_app.urls')),                # trucker ANALYTICS (MONGO DB)
    url(r'^', include('trucker_dashboard_app.urls')),      # trucker DASHBOARD ANALYTICS (MONGO DB)
    url(r'^', include('export_trucker_app.urls')),         # EXPORT FUNCTIONALITY trucker (MONGO DB)
    url(r'^', include('loadmeup_dashboard.urls')),         # LoadMeUp trucker
]
