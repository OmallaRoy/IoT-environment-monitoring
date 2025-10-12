# from django.shortcuts import render, redirect
# from .forms import SensorHistoryForm, ModelUploadForm
# from .models import UploadedModel
# from .model_store import rf
# import joblib
# import pandas as pd

# # -----------------------------
# # Upload ML model
# # -----------------------------
# def upload_model(request):
#     """
#     Upload a new Random Forest model.
#     The model is saved to ml/models/ and immediately loaded for predictions.
#     """
#     if request.method == "POST":
#         form = ModelUploadForm(request.POST, request.FILES)
#         if form.is_valid():
#             uploaded_model = form.save()  # saves the file
#             # Load the model into memory immediately
#             model_path = uploaded_model.model_file.path
#             model = joblib.load(model_path)

#             # Save it to the global model_store
#             from . import model_store
#             model_store.rf = model

#             return redirect("predict_next")
#     else:
#         form = ModelUploadForm()

#     return render(request, "ml/upload.html", {"form": form})

# # -----------------------------
# # Predict next readings
# # -----------------------------
# def predict_next(request):
#     """
#     Use the latest model to predict next temperature and humidity
#     based on last 3 readings submitted via form.
#     """
#     prediction = None

#     if request.method == "POST":
#         form = SensorHistoryForm(request.POST)
#         if form.is_valid():
#             cd = form.cleaned_data

#             # Prepare features in the same lag order as training
#             latest_features = pd.DataFrame({
#                 "temp_lag1": [cd["temp3"]],
#                 "temp_lag2": [cd["temp2"]],
#                 "temp_lag3": [cd["temp1"]],
#                 "hum_lag1":  [cd["hum3"]],
#                 "hum_lag2":  [cd["hum2"]],
#                 "hum_lag3":  [cd["hum1"]],
#             })

#             # Make sure a model is loaded
#             if rf is None:
#                 prediction = {"error": "No model loaded. Please upload a model first."}
#             else:
#                 # Predict using the latest model
#                 pred = rf.predict(latest_features)[0]
#                 prediction = {
#                     "temp": round(pred[0], 2),
#                     "hum":  round(pred[1], 2),
#                 }
#     else:
#         form = SensorHistoryForm()

#     return render(request, "ml/predict.html", {
#         "form": form,
#         "prediction": prediction
#     })



#Made changes

from django.shortcuts import render
from .model_store import rf
import pandas as pd
import requests
from datetime import datetime
from django.http import JsonResponse

# -----------------------------
# Auto-predict with latest data from ThingSpeak
# -----------------------------
def auto_predict(request):
    """
    Automatically fetch latest data from ThingSpeak and make predictions
    """
    prediction = None
    error = None
    
    try:
        # Fetch latest data from ThingSpeak
        THINGSPEAK_API_URL = "https://api.thingspeak.com/channels/3077306/feeds.json?api_key=RJKY2M6KAC4APH45&results=10"
        response = requests.get(THINGSPEAK_API_URL)
        data = response.json()
        feeds = data['feeds']
        
        if len(feeds) >= 3:
            # Get the last 3 readings (most recent first)
            last_three = feeds[-3:]
            
            # Extract temperature and humidity 
            # Based on your ThingSpeak setup:
            # field1 = battery, field2 = humidity, field3 = motion, field4 = temperature
            temps = [float(feed.get('field4', 0)) for feed in last_three]
            hums = [float(feed.get('field2', 0)) for feed in last_three]
            
            # Make sure a model is loaded
            if rf is None:
                error = "Model not loaded. Please ensure the model is properly configured."
            else:
                # Prepare features for prediction
                # Note: Order is important - we need oldest to newest for lag features
                latest_features = pd.DataFrame({
                    "temp_lag1": [temps[2]],  # latest (most recent)
                    "temp_lag2": [temps[1]],  # middle
                    "temp_lag3": [temps[0]],  # oldest
                    "hum_lag1":  [hums[2]],   # latest (most recent)
                    "hum_lag2":  [hums[1]],   # middle
                    "hum_lag3":  [hums[0]],   # oldest
                })
                
                # Predict using the model
                pred = rf.predict(latest_features)[0]
                prediction = {
                    "temp": round(pred[0], 2),
                    "hum":  round(pred[1], 2),
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        else:
            error = "Not enough data available. Need at least 3 readings from ThingSpeak."
            
    except Exception as e:
        error = f"Error fetching data from ThingSpeak: {str(e)}"
    
    context = {
        "prediction": prediction,
        "error": error
    }
    
    return render(request, "ml/auto_predict.html", context)

# -----------------------------
# API endpoint for predictions (optional)
# -----------------------------
def prediction_api(request):
    """
    JSON API endpoint for predictions
    """
    try:
        # Fetch latest data from ThingSpeak
        THINGSPEAK_API_URL = "https://api.thingspeak.com/channels/3077306/feeds.json?api_key=RJKY2M6KAC4APH45&results=3"
        response = requests.get(THINGSPEAK_API_URL)
        data = response.json()
        feeds = data['feeds']
        
        if len(feeds) >= 3 and rf is not None:
            # Get the last 3 readings
            last_three = feeds[-3:]
            temps = [float(feed.get('field4', 0)) for feed in last_three]
            hums = [float(feed.get('field2', 0)) for feed in last_three]
            
            # Prepare features for prediction
            latest_features = pd.DataFrame({
                "temp_lag1": [temps[2]],
                "temp_lag2": [temps[1]],
                "temp_lag3": [temps[0]],
                "hum_lag1":  [hums[2]],
                "hum_lag2":  [hums[1]],
                "hum_lag3":  [hums[0]],
            })
            
            # Make prediction
            pred = rf.predict(latest_features)[0]
            
            return JsonResponse({
                "success": True,
                "prediction": {
                    "temperature": round(pred[0], 2),
                    "humidity": round(pred[1], 2),
                    "timestamp": datetime.now().isoformat()
                }
            })
        else:
            return JsonResponse({
                "success": False,
                "error": "Insufficient data or model not loaded"
            })
            
    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": str(e)
        })