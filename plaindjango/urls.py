"""plaindjango URL Configuration

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
from django.urls import path
from plaindjango import views
from django.conf.urls import url
urlpatterns = [
    path('admin/', admin.site.urls),
    path('followers/<str:user_name>/', views.api.get_followers_by_name),
    path('directmessage/', views.api.send_direct_messages),
    path('crm/', views.api.crm_manager),
    path('humanize/', views.api.humanize),
    path('getid/<str:user_name>/', views.api.get_id_by_name),
    path('refresh/', views.api.refresh_api),
    path('seedusers/<str:key_word>/', views.api.get_seed_users_by_key),

]
