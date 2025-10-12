from django.urls import path
# from .views import predict_next, upload_model
from .views import auto_predict, prediction_api

urlpatterns = [
    # path("predict/", predict_next, name="predict_next"),
    #  path("upload/", upload_model, name="upload_model"),
    
    #  path("upload/", upload_model, name="upload_model"),
    path("auto_predict/", auto_predict, name="auto_predict"),
    path("api/predict/", prediction_api, name="prediction_api"),
    
]
