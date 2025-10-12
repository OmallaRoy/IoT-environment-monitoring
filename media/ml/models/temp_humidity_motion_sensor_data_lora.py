# import paho.mqtt.client as mqtt
# import json
# from datetime import datetime, timedelta
# import time
# import os
# import requests
# import schedule
# import threading
# import pandas as pd
# from dotenv import load_dotenv


# load_dotenv()

# broker = "eu1.cloud.thethings.network"
# port = 1883
# username = os.getenv("TTN_USERNAME", "bd-test-app2@ttn")
# password = os.getenv("TTN_API_KEY", "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA")
# device_id = "lht65n-01-temp-humidity-sensor"

# thingspeak_channel_id = os.getenv("THINGSPEAK_CHANNEL_ID", "3077306")
# thingspeak_write_api_key = os.getenv("THINGSPEAK_WRITE_API_KEY", "JTOW7IQ7BDQBUGR6")
# thingspeak_read_api_key = os.getenv("THINGSPEAK_READ_API_KEY", "RJKY2M6KAC4APH45")
# csv_file = "sensor_data.csv"


# def update_csv_from_thingspeak():
#     start_time = time.time()
#     print("Updating CSV from ThingSpeak (last 48h)…")

   
#     start_utc = datetime.utcnow() - timedelta(hours=48)
#     url = (
#         f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.json"
#         f"?api_key={thingspeak_read_api_key}&results=2000"
#         f"&start={start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
#     )

#     try:
#         response = requests.get(url, timeout=10)
#         response.raise_for_status()
#     except requests.RequestException as e:
#         print(f"Error fetching from ThingSpeak: {e}")
#         return

#     feeds = response.json().get('feeds', [])
#     csv_data_list = []

#     for feed in feeds:
#         if feed.get('field4'):  # Ensure temperature exists
#             try:
#                 dt_utc = datetime.strptime(feed['created_at'], "%Y-%m-%dT%H:%M:%SZ")
#                 dt_uganda = dt_utc + timedelta(hours=3)
#                 timestamp = dt_uganda.isoformat() + '+03:00'
#                 csv_data = {
#                     "timestamp": timestamp,
#                     "battery": float(feed.get('field1', None)),
#                     "humidity": float(feed.get('field2', None)),
#                     "motion": int(feed.get('field3', None)) if feed.get('field3') else None,
#                     "temperature": float(feed.get('field4', None))
#                 }
#                 csv_data_list.append(csv_data)
#             except ValueError as e:
#                 print(f"Error parsing feed: {e}")
#                 continue

#     if csv_data_list:
#         df_new = pd.DataFrame(csv_data_list, columns=['timestamp', 'battery', 'humidity', 'motion', 'temperature'])
#         if os.path.exists(csv_file):
#             df_existing = pd.read_csv(csv_file)
#             df_new = df_new[~df_new['timestamp'].isin(df_existing['timestamp'])]  # Remove duplicates
#             df_new = pd.concat([df_existing, df_new], ignore_index=True)
#         df_new.to_csv(csv_file, mode='w', index=False)
#         print(f"Batch saved {len(csv_data_list)} entries to {csv_file}")

#     print(f"CSV update completed in {time.time() - start_time:.2f} seconds.")


# def get_historical_and_upload():
#     start_time = time.time()
#     print("Starting historical data fetch (last 48h)…")

#     app_id = username.split('@')[0]
#     api_key = password
#     url = f"https://{broker}/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"

#     # ► changed to 48 hours
#     params = {"last": "48h", "limit": 500}
#     headers = {"Authorization": f"Bearer {api_key}"}

#     try:
#         response = requests.get(url, headers=headers, params=params, timeout=10)
#         response.raise_for_status()
#     except requests.RequestException as e:
#         print(f"Error fetching from TTN: {e}")
#         return

#     updates = []
#     for line in response.text.splitlines():
#         if line.strip():
#             try:
#                 data = json.loads(line)
#                 result = data.get('result')
#                 if result:
#                     received_at_utc = result.get('received_at')
#                     if received_at_utc:
#                         dt_utc = datetime.fromisoformat(received_at_utc.rstrip('Z'))
#                         dt_uganda = dt_utc + timedelta(hours=3)
#                         created_at = dt_uganda.isoformat() + '+03:00'

#                         decoded = result.get('uplink_message', {}).get('decoded_payload', {})
#                         batt = decoded.get('field1')
#                         hum = decoded.get('field3')
#                         motion = decoded.get('field4')
#                         temp = decoded.get('field5')

#                         if any([batt, hum, motion, temp]):
#                             update = {"created_at": created_at}
#                             if batt is not None:
#                                 update["field1"] = batt
#                             if hum is not None:
#                                 update["field2"] = hum
#                             if motion is not None:
#                                 update["field3"] = motion
#                             if temp is not None:
#                                 update["field4"] = temp
#                             updates.append(update)
#             except json.JSONDecodeError as e:
#                 print(f"Error parsing JSON line: {e}")
#                 continue

#     if updates:
#         bulk_url = f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/bulk_update.json"
#         bulk_data = {"write_api_key": thingspeak_write_api_key, "updates": updates[:100]}
#         try:
#             bulk_response = requests.post(bulk_url, json=bulk_data, timeout=10)
#             if bulk_response.status_code == 202:
#                 print("Historical data uploaded to ThingSpeak successfully!")
#                 update_csv_from_thingspeak()
#             else:
#                 print("Error uploading to ThingSpeak:", bulk_response.status_code, bulk_response.text)
#         except requests.RequestException as e:
#             print(f"Error uploading to ThingSpeak: {e}")

#     print(f"Historical fetch completed in {time.time() - start_time:.2f} seconds.")


# def on_connect(client, userdata, flags, reason_code, properties=None):
#     if reason_code == 0:
#         print("Connected to TTN MQTT broker!")
#         client.subscribe(f"v3/{username}/devices/{device_id}/up")
#     else:
#         print(f"Failed to connect, reason code {reason_code}")


# def on_message(client, userdata, msg):
#     try:
#         payload = json.loads(msg.payload.decode())
#         uplink_message = payload.get('uplink_message', {})
#         decoded_payload = uplink_message.get('decoded_payload', {})
#         if not decoded_payload:
#             print("No decoded payload in message.")
#             return

#         batt = decoded_payload.get('field1')
#         hum = decoded_payload.get('field3')
#         motion = decoded_payload.get('field4')
#         temp = decoded_payload.get('field5')

#         received_at_utc = payload.get('received_at')
#         if received_at_utc:
#             dt_utc = datetime.fromisoformat(received_at_utc.rstrip('Z'))
#             dt_uganda = dt_utc + timedelta(hours=3)
#             timestamp = dt_uganda.isoformat() + '+03:00'
#         else:
#             timestamp = (datetime.utcnow() + timedelta(hours=3)).isoformat() + '+03:00'

#         # Upload to ThingSpeak
#         update_url = "https://api.thingspeak.com/update"
#         params = {"api_key": thingspeak_write_api_key}
#         if batt is not None:
#             params["field1"] = batt
#         if hum is not None:
#             params["field2"] = hum
#         if motion is not None:
#             params["field3"] = motion
#         if temp is not None:
#             params["field4"] = temp

#         if len(params) > 1:
#             response = requests.post(update_url, params=params, timeout=10)
#             print(f"ThingSpeak real-time upload response: {response.status_code}, {response.text}")
#             if response.status_code == 200 and int(response.text) > 0:
#                 print("Real-time data uploaded to ThingSpeak.")
#                 update_csv_from_thingspeak()
#             else:
#                 print("Error uploading real-time data:", response.text)
#     except Exception as e:
#         print(f"Error in on_message: {e}")


# # ── MQTT client and threaded runner ────────────────────────────
# client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
# client.username_pw_set(username, password)
# client.reconnect_delay_set(min_delay=1, max_delay=300)
# client.on_connect = on_connect
# client.on_message = on_message


# def run_mqtt_with_restart():
#     while True:
#         try:
#             client.connect(broker, port, 60)
#             client.loop_forever()
#         except Exception as e:
#             print(f"MQTT thread crashed: {e}. Restarting in 10 seconds…")
#             time.sleep(10)

# if __name__ == "__main__":
#     mqtt_thread = threading.Thread(target=run_mqtt_with_restart)
#     mqtt_thread.start()

#     # Schedule tasks
#     schedule.every().day.at("15:09").do(get_historical_and_upload)
#     schedule.every().day.at("15:09").do(update_csv_from_thingspeak)
    

#     # Run missed tasks if within 30 minutes of schedule
#     current_time = datetime.now().time()
#     scheduled_time = datetime.strptime("15:10", "%H:%M").time()
#     if scheduled_time <= current_time <= datetime.strptime("15:10", "%H:%M").time():
#         print("Running missed historical fetch and CSV update…")
#         get_historical_and_upload()
#         update_csv_from_thingspeak()

#     while True:
#         schedule.run_pending()
#         time.sleep(1)
