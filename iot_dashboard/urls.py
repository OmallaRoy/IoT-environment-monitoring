from django.contrib import admin
from django.urls import path, include          # include is needed
from dashboard import views                    # your charts view
from django.conf import settings
from django.conf.urls.static import static
from django.shortcuts import redirect
from .views import landing
# for root redirect

# Optional: redirect root '/' to charts
def home(request):
    return redirect('charts')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', landing, name='landing'),                               # redirect root to charts
    # path('charts/', views.charts, name='charts'),   # charts page
    path('charts/', include('dashboard.urls')),
    path('ml/', include('ml.urls')),               
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
