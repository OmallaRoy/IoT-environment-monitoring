from django.shortcuts import render
import requests
import json
from datetime import datetime

 # Also made changes here and replaced with mine
 
def charts(request):
    THINGSPEAK_API_URL = "https://api.thingspeak.com/channels/3091064/feeds.json?api_key=RIJH231PEMRU55M3" # used the read API

    response = requests.get(THINGSPEAK_API_URL)
    data = response.json()
    feeds = data['feeds']

    latest_per_day = {}

    for feed in feeds:
        ts = datetime.strptime(feed['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        day_str = ts.strftime('%d %b')  # "17 Sep", "18 Sep", ...

        # Keep only the latest feed for that day
        if day_str not in latest_per_day or ts > datetime.strptime(latest_per_day[day_str]['created_at'], '%Y-%m-%dT%H:%M:%SZ'):
            latest_per_day[day_str] = feed

    # Sort days chronologically
    sorted_days = sorted(
        latest_per_day.keys(),
        key=lambda d: datetime.strptime(d, '%d %b')
    )

    # Prepare lists for Chart.js
    labels = []
    Temperature, Humidity, Battery, Motion = [], [], [], []

    for day in sorted_days:
        labels.append(day)
        f = latest_per_day[day]
        Temperature.append(float(f['field1'] or 0))
        Humidity.append(float(f['field2'] or 0))
        Battery.append(float(f['field3'] or 0))
        Motion.append(float(f['field4'] or 0))

    context = {
        "timestamps_json": json.dumps(labels),   # <-- use the same name as in your template
        "Temperature_json": json.dumps(Temperature),
        "Humidity_json": json.dumps(Humidity),
        "Battery_json": json.dumps(Battery),
        "Motion_json": json.dumps(Motion),
    }

    return render(request, "dashboard/charts.html", context)
