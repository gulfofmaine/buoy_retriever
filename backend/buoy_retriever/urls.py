"""
URL configuration for buoy_retriever project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
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
from django.urls import include, path

from .api import api

prefix = "backend/"

urlpatterns = [
    path(f"{prefix}admin/", admin.site.urls),
    path(f"{prefix}api/", api.urls),
    path(f"{prefix}health/", include("health_check.urls")),  # health check endpoints
    path(prefix, include("django.contrib.auth.urls")),
]
