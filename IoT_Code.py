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

# # ------------------- Configuration -------------------
# load_dotenv(".env")

# broker = "eu1.cloud.thethings.network"
# port = 1883
# username = os.getenv("TTN_USERNAME", "bd-test-app2@ttn")
# password = os.getenv("TTN_API_KEY", "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA")
# device_id = "lht65n-01-temp-humidity-sensor"

# thingspeak_channel_id = os.getenv("THINGSPEAK_CHANNEL_ID", "3091064")
# thingspeak_write_api_key = os.getenv("THINGSPEAK_WRITE_API_KEY", "7LD0JKR5AZLO896Y")
# thingspeak_read_api_key = os.getenv("THINGSPEAK_READ_API_KEY", "RIJH231PEMRU55M3")
# thingspeak_bulk_url = f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/bulk_update.json"
# thingspeak_update_url = "https://api.thingspeak.com/update"
# csv_file = "sensor_data.csv"

# # ------------------- Helper Functions -------------------
# def parse_ttn_timestamp(timestamp_str):
#     """Parse TTN timestamp that may have more than 6 decimal places"""
#     try:
#         timestamp_str = timestamp_str.rstrip("Z")
#         if '.' in timestamp_str:
#             main_part, fractional = timestamp_str.split('.')
#             fractional = fractional[:6].ljust(6, '0')
#             timestamp_str = f"{main_part}.{fractional}"
#         return datetime.fromisoformat(timestamp_str)
#     except Exception as e:
#         print(f"❌ Error parsing timestamp '{timestamp_str}': {e}")
#         return datetime.utcnow()

# def initialize_csv():
#     """Create CSV file with headers if it doesn't exist"""
#     print("📁 Initializing CSV file...")
#     if not os.path.exists(csv_file):
#         df = pd.DataFrame(columns=['timestamp', 'Battery', 'Humidity', 'Motion', 'Temperature'])
#         df.to_csv(csv_file, index=False)
#         print(f"✅ Created new CSV file: {csv_file}")
#     else:
#         df = pd.read_csv(csv_file)
#         print(f"✅ CSV file already exists: {csv_file} with {len(df)} rows")
#     print("")

# def save_to_csv(data_list):
#     """Save data to CSV, avoiding duplicates"""
#     if not data_list:
#         return
    
#     df_new = pd.DataFrame(data_list)
    
#     if os.path.exists(csv_file):
#         df_existing = pd.read_csv(csv_file)
#         mask = ~df_new['timestamp'].isin(df_existing['timestamp'])
#         df_new_filtered = df_new[mask]
        
#         if len(df_new_filtered) > 0:
#             df_combined = pd.concat([df_existing, df_new_filtered], ignore_index=True)
#             df_combined.to_csv(csv_file, index=False)
#             print(f"💾 CSV: {len(df_new_filtered)} new rows, total {len(df_combined)}")
#         else:
#             print("💾 No new data (duplicates)")
#     else:
#         df_new.to_csv(csv_file, index=False)
#         print(f"💾 Created CSV with {len(df_new)} rows")

# def upload_data_to_thingspeak(data_list):
#     """Upload data to ThingSpeak"""
#     if not data_list:
#         return
    
#     print(f"☁️ Uploading {len(data_list)} entries to ThingSpeak...")
#     successful_uploads = 0
    
#     for data in data_list:
#         try:
#             params = {"api_key": thingspeak_write_api_key}
            
#             if data.get('Battery') is not None:
#                 params["field1"] = data['Battery']
#             if data.get('Humidity') is not None:
#                 params["field2"] = data['Humidity']
#             if data.get('Motion') is not None:
#                 params["field3"] = data['Motion']
#             if data.get('Temperature') is not None:
#                 params["field4"] = data['Temperature']
            
#             # Add timestamp if available
#             if data.get('timestamp'):
#                 # Convert timestamp to ThingSpeak format
#                 try:
#                     ts = data['timestamp'].replace('+03:00', '')
#                     dt = datetime.fromisoformat(ts)
#                     params["created_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
#                 except:
#                     pass
            
#             response = requests.post(thingspeak_update_url, params=params, timeout=5)
#             if response.status_code == 200:
#                 successful_uploads += 1
#             else:
#                 print(f"❌ ThingSpeak upload failed for timestamp {data.get('timestamp')}: {response.text}")
            
#             # Respect ThingSpeak rate limit (15 sec between updates)
#             time.sleep(15)
            
#         except Exception as e:
#             print(f"❌ Error uploading to ThingSpeak: {e}")
    
#     print(f"✅ Successfully uploaded {successful_uploads}/{len(data_list)} entries to ThingSpeak")

# def sync_local_csv_to_thingspeak():
#     """Sync entire local CSV to ThingSpeak - ensures all local data is on ThingSpeak"""
#     print("🔄 Syncing local CSV to ThingSpeak...")
    
#     if not os.path.exists(csv_file):
#         print("❌ No local CSV file found")
#         return
    
#     try:
#         df = pd.read_csv(csv_file)
#         if len(df) == 0:
#             print("📊 Local CSV is empty")
#             return
        
#         print(f"📊 Found {len(df)} records in local CSV")
        
#         # Convert DataFrame to list of dictionaries for upload
#         data_list = []
#         for _, row in df.iterrows():
#             data = {
#                 'timestamp': row['timestamp'],
#                 'Battery': row['Battery'] if pd.notna(row['Battery']) else None,
#                 'Humidity': row['Humidity'] if pd.notna(row['Humidity']) else None,
#                 'Motion': row['Motion'] if pd.notna(row['Motion']) else None,
#                 'Temperature': row['Temperature'] if pd.notna(row['Temperature']) else None
#             }
#             data_list.append(data)
        
#         # Upload to ThingSpeak
#         upload_data_to_thingspeak(data_list)
        
#     except Exception as e:
#         print(f"❌ Error syncing local CSV to ThingSpeak: {e}")

# # ------------------- ThingSpeak to CSV Sync -------------------
# def sync_thingspeak_to_csv():
#     """Sync data from ThingSpeak to CSV - This works reliably"""
#     print("🔄 Syncing ThingSpeak data to CSV...")
    
#     # Get last 48 hours of data from ThingSpeak
#     start_utc = datetime.utcnow() - timedelta(hours=48)
#     url = (
#         f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.json"
#         f"?api_key={thingspeak_read_api_key}&results=8000"
#         f"&start={start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
#     )

#     try:
#         response = requests.get(url, timeout=10)
#         feeds = response.json().get('feeds', [])
#         print(f"📊 Found {len(feeds)} feeds in ThingSpeak")
        
#         csv_data_list = []
#         for feed in feeds:
#             try:
#                 dt_utc = datetime.strptime(feed['created_at'], "%Y-%m-%dT%H:%M:%SZ")
#                 dt_uganda = dt_utc + timedelta(hours=3)
#                 timestamp = dt_uganda.isoformat() + '+03:00'
                
#                 csv_data = {
#                     "timestamp": timestamp,
#                     "Battery": float(feed.get('field1')) if feed.get('field1') else None,
#                     "Humidity": float(feed.get('field2')) if feed.get('field2') else None,
#                     "Motion": int(feed.get('field3')) if feed.get('field3') else None,
#                     "Temperature": float(feed.get('field4')) if feed.get('field4') else None
#                 }
#                 csv_data_list.append(csv_data)
#             except (ValueError, TypeError):
#                 continue

#         if csv_data_list:
#             save_to_csv(csv_data_list)
#             print(f"✅ Synced {len(csv_data_list)} entries from ThingSpeak")
#         else:
#             print("❌ No data from ThingSpeak")
            
#     except Exception as e:
#         print(f"❌ ThingSpeak sync error: {e}")

# # ------------------- Fixed Historical Data Fetch -------------------
# def get_historical_and_upload():
#     """Fixed historical data fetch with correct field mapping"""
#     print("🔄 Starting historical data fetch from TTN...")
#     start_time = time.time()

#     app_id = username.split('@')[0]
#     url = f"https://{broker}/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"
#     headers = {"Authorization": f"Bearer {password}"}
#     params = {"last": "48h", "limit": 100}

#     try:
#         response = requests.get(url, headers=headers, params=params, timeout=10)
#         response.raise_for_status()
#         print("✅ TTN historical data fetched successfully")
        
#     except requests.RequestException as e:
#         print(f"❌ Error fetching from TTN: {e}")
#         return

#     updates = []
#     historical_data_list = []
#     line_count = 0
#     valid_data_count = 0
    
#     print("🔍 Processing historical data with CORRECT field mapping...")
#     for line in response.text.splitlines():
#         if not line.strip():
#             continue
#         line_count += 1
#         try:
#             data = json.loads(line)
            
#             # Extract from the correct structure - TTN storage API uses "result" -> "uplink_message"
#             result = data.get("result", {})
#             uplink = result.get("uplink_message", {})
#             decoded = uplink.get("decoded_payload", {})
            
#             if not decoded:
#                 continue

#             # CORRECT FIELD MAPPING - SAME AS REAL-TIME DATA!
#             Battery = decoded.get("field1")      # 3.066
#             Humidity = decoded.get("field3")     # 63.4  
#             Motion = decoded.get("field4")       # 1
#             Temperature = decoded.get("field5")  # 26.51

#             # Debug: Print first few entries to verify
#             if line_count <= 3:
#                 print(f"📋 Sample historical data (line {line_count}):")
#                 print(f"  Battery (field1): {Battery}")
#                 print(f"  Humidity (field3): {Humidity}")
#                 print(f"  Motion (field4): {Motion}")
#                 print(f"  Temperature (field5): {Temperature}")
#                 print(f"  All fields: {list(decoded.keys())}")

#             received_at_utc = result.get("received_at")
#             if received_at_utc:
#                 dt_utc = parse_ttn_timestamp(received_at_utc)
#                 dt_uganda = dt_utc + timedelta(hours=3)
#                 created_at = dt_uganda.isoformat() + '+03:00'
#             else:
#                 created_at = datetime.utcnow().isoformat() + "Z"

#             # Only add if we have valid sensor data
#             if Battery is not None or Humidity is not None or Motion is not None or Temperature is not None:
#                 update = {"created_at": created_at}
#                 if Battery is not None: update["field1"] = Battery
#                 if Humidity is not None: update["field2"] = Humidity
#                 if Motion is not None: update["field3"] = Motion
#                 if Temperature is not None: update["field4"] = Temperature
#                 updates.append(update)

#                 historical_data_list.append({
#                     "timestamp": created_at,
#                     "Battery": Battery,
#                     "Humidity": Humidity,
#                     "Motion": Motion,
#                     "Temperature": Temperature
#                 })
#                 valid_data_count += 1

#         except Exception as e:
#             print(f"⚠️ Error on line {line_count}: {e}")
#             continue

#     print(f"📊 Processed {line_count} lines, found {valid_data_count} valid data entries")

#     # Save to CSV and upload to ThingSpeak if we have data
#     if historical_data_list:
#         print(f"💾 Saving {len(historical_data_list)} historical entries to CSV...")
#         save_to_csv(historical_data_list)
        
#         print(f"☁️ Uploading {len(historical_data_list)} historical entries to ThingSpeak...")
#         upload_data_to_thingspeak(historical_data_list)
#     else:
#         print("❌ No historical data found - check field mapping")

#     print(f"✅ Historical fetch completed in {time.time() - start_time:.2f} seconds.")

# # ------------------- MQTT Callbacks -------------------
# def on_connect(client, userdata, flags, reason_code, properties=None):
#     if reason_code == 0:
#         print("✅ Connected to TTN MQTT broker!")
#         client.subscribe(f"v3/{username}/devices/{device_id}/up")
#         print("📡 Listening for real-time sensor data...")
#     else:
#         print(f"❌ Connection failed: {reason_code}")

# def on_message(client, userdata, msg):
#     """Handle real-time MQTT messages - THIS WORKS CORRECTLY"""
#     try:
#         payload = json.loads(msg.payload.decode())
#         uplink = payload.get("uplink_message", {})
#         decoded = uplink.get("decoded_payload", {})
        
#         if not decoded:
#             return

#         print(f"📨 Real-time data: {decoded}")
        
#         # Real-time data uses field1, field3, field4, field5
#         Battery = decoded.get("field1")
#         Humidity = decoded.get("field3")  
#         Motion = decoded.get("field4")    
#         Temperature = decoded.get("field5")

#         received_at_utc = payload.get("received_at")
#         if received_at_utc:
#             dt_utc = parse_ttn_timestamp(received_at_utc)
#             dt_uganda = dt_utc + timedelta(hours=3)
#             timestamp = dt_uganda.isoformat() + "+03:00"
#         else:
#             timestamp = (datetime.utcnow() + timedelta(hours=3)).isoformat() + "+03:00"

#         # Prepare data for both CSV and ThingSpeak
#         realtime_data = [{
#             "timestamp": timestamp,
#             "Battery": Battery,
#             "Humidity": Humidity,
#             "Motion": Motion,
#             "Temperature": Temperature
#         }]
        
#         # Save to CSV immediately
#         save_to_csv(realtime_data)
        
#         # Send to ThingSpeak immediately
#         upload_data_to_thingspeak(realtime_data)
        
#     except Exception as e:
#         print(f"❌ MQTT error: {e}")

# # ------------------- MQTT Setup -------------------
# client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
# client.username_pw_set(username, password)
# client.reconnect_delay_set(min_delay=1, max_delay=300)
# client.on_connect = on_connect
# client.on_message = on_message

# def run_mqtt():
#     while True:
#         try:
#             client.connect(broker, port, 60)
#             client.loop_forever()
#         except Exception as e:
#             print(f"❌ MQTT disconnected: {e}")
#             time.sleep(10)

# # ------------------- Main -------------------
# if __name__ == "__main__":
#     print("=" * 50)
#     print("🚀 IoT Data Collection System")
#     print("=" * 50)
    
#     # Initialize CSV
#     initialize_csv()
    
#     # Step 1: Sync local CSV to ThingSpeak (ensure all existing data is uploaded)
#     print("\n📊 Step 1: Syncing local CSV to ThingSpeak")
#     print("-" * 40)
#     sync_local_csv_to_thingspeak()
    
#     # Step 2: Get historical data from ThingSpeak (this works reliably)
#     print("\n📊 Step 2: Getting historical data from ThingSpeak")
#     print("-" * 40)
#     sync_thingspeak_to_csv()
    
#     # Step 3: Try to get additional historical data from TTN with correct field mapping
#     print("\n📊 Step 3: Getting additional historical data from TTN") 
#     print("-" * 40)
#     get_historical_and_upload()
    
#     # Step 4: Start real-time MQTT listener
#     print("\n📊 Step 4: Starting real-time data collection")
#     print("-" * 40)
#     mqtt_thread = threading.Thread(target=run_mqtt, daemon=True)
#     mqtt_thread.start()
    
#     # Give MQTT time to connect
#     time.sleep(3)

#     # Step 5: Schedule regular updates
#     print("\n📊 Step 5: Setting up scheduled updates")
#     print("-" * 40)
    
#     # Schedule ThingSpeak sync every 6 hours (reliable backup)
#     schedule.every(6).hours.do(sync_thingspeak_to_csv)
    
#     # Schedule TTN historical fetch once per day
#     schedule.every().day.at("12:40").do(get_historical_and_upload)
    
#     # Schedule full local CSV sync to ThingSpeak once per day (backup)
#     schedule.every().day.at("06:00").do(sync_local_csv_to_thingspeak)
    
#     print("✅ ThingSpeak sync: Every 6 hours")
#     print("✅ TTN historical: Daily at 20:23")
#     print("✅ Full CSV backup: Daily at 06:00")
#     print("✅ Real-time: Continuous")

#     # Final status
#     print("\n" + "=" * 50)
#     print("✅ System initialization completed!")
#     print(f"📁 CSV file: {os.path.abspath(csv_file)}")
    
#     if os.path.exists(csv_file):
#         df = pd.read_csv(csv_file)
#         print(f"📊 Current data: {len(df)} rows in CSV")
    
#     print("📡 Real-time monitoring: ACTIVE")
#     print("⏰ Scheduled tasks: ACTIVE")
#     print("🔄 Data sync: CSV ↔ ThingSpeak")
#     print("=" * 50)
#     print("\n🔄 System is running. Press Ctrl+C to stop.")
    
#     # Main loop
#     try:
#         while True:
#             schedule.run_pending()
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("\n🛑 Script stopped by user")

import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import time
import os
import requests
import schedule
import threading
import pandas as pd
from dotenv import load_dotenv

# ------------------- Configuration -------------------
load_dotenv(".env")

broker = "eu1.cloud.thethings.network"
port = 1883
username = os.getenv("TTN_USERNAME", "bd-test-app2@ttn")
password = os.getenv("TTN_API_KEY", "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA")
device_id = "lht65n-01-temp-humidity-sensor"

thingspeak_channel_id = os.getenv("THINGSPEAK_CHANNEL_ID", "3091064")
thingspeak_write_api_key = os.getenv("THINGSPEAK_WRITE_API_KEY", "7LD0JKR5AZLO896Y")
thingspeak_read_api_key = os.getenv("THINGSPEAK_READ_API_KEY", "RIJH231PEMRU55M3")
thingspeak_bulk_url = f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/bulk_update.json"
thingspeak_update_url = "https://api.thingspeak.com/update"
csv_file = "sensor_data.csv"

# ------------------- Helper Functions -------------------
def parse_ttn_timestamp(timestamp_str):
    """Parse TTN timestamp that may have more than 6 decimal places"""
    try:
        timestamp_str = timestamp_str.rstrip("Z")
        if '.' in timestamp_str:
            main_part, fractional = timestamp_str.split('.')
            fractional = fractional[:6].ljust(6, '0')
            timestamp_str = f"{main_part}.{fractional}"
        return datetime.fromisoformat(timestamp_str)
    except Exception as e:
        print(f"❌ Error parsing timestamp '{timestamp_str}': {e}")
        return datetime.utcnow()

def initialize_csv():
    """Create CSV file with headers if it doesn't exist"""
    print("📁 Initializing CSV file...")
    if not os.path.exists(csv_file):
        df = pd.DataFrame(columns=['timestamp', 'Battery', 'Humidity', 'Motion', 'Temperature'])
        df.to_csv(csv_file, index=False)
        print(f"✅ Created new CSV file: {csv_file}")
    else:
        df = pd.read_csv(csv_file)
        print(f"✅ CSV file already exists: {csv_file} with {len(df)} rows")
    print("")

def save_to_csv(data_list):
    """Save data to CSV, avoiding duplicates"""
    if not data_list:
        return
    
    df_new = pd.DataFrame(data_list)
    
    if os.path.exists(csv_file):
        df_existing = pd.read_csv(csv_file)
        mask = ~df_new['timestamp'].isin(df_existing['timestamp'])
        df_new_filtered = df_new[mask]
        
        if len(df_new_filtered) > 0:
            df_combined = pd.concat([df_existing, df_new_filtered], ignore_index=True)
            df_combined.to_csv(csv_file, index=False)
            print(f"💾 CSV: {len(df_new_filtered)} new rows, total {len(df_combined)}")
            return len(df_new_filtered)
        else:
            print("💾 No new data (duplicates)")
            return 0
    else:
        df_new.to_csv(csv_file, index=False)
        print(f"💾 Created CSV with {len(df_new)} rows")
        return len(df_new)

# ------------------- OPTIMIZED ThingSpeak Functions -------------------
def upload_bulk_to_thingspeak_smart(data_list, max_retries=3):
    """Smart bulk upload with rate limit handling and retries"""
    if not data_list:
        return True
    
    print(f"⚡ Smart uploading {len(data_list)} entries to ThingSpeak...")
    
    # Prepare bulk update data
    updates = []
    for data in data_list:
        update = {}
        
        if data.get('timestamp'):
            try:
                ts = data['timestamp'].replace('+03:00', '')
                dt = datetime.fromisoformat(ts)
                update["created_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                update["created_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        if data.get('Battery') is not None:
            update["field1"] = data['Battery']
        if data.get('Humidity') is not None:
            update["field2"] = data['Humidity']
        if data.get('Motion') is not None:
            update["field3"] = data['Motion']
        if data.get('Temperature') is not None:
            update["field4"] = data['Temperature']
        
        updates.append(update)
    
    bulk_data = {
        "write_api_key": thingspeak_write_api_key, 
        "updates": updates
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(thingspeak_bulk_url, json=bulk_data, timeout=30)
            
            if response.status_code == 202:
                print(f"✅ Bulk upload successful! {len(data_list)} entries sent")
                return True
            elif response.status_code == 429:
                wait_time = (attempt + 1) * 60  # Wait 1, 2, 3 minutes
                print(f"⏳ Rate limited. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ Bulk upload failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Bulk upload error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(30)
    
    print(f"❌ Failed to upload after {max_retries} attempts")
    return False

def upload_single_to_thingspeak_smart(data):
    """Smart single upload for real-time data with rate limit handling"""
    params = {"api_key": thingspeak_write_api_key}
    
    if data.get('Battery') is not None:
        params["field1"] = data['Battery']
    if data.get('Humidity') is not None:
        params["field2"] = data['Humidity']
    if data.get('Motion') is not None:
        params["field3"] = data['Motion']
    if data.get('Temperature') is not None:
        params["field4"] = data['Temperature']
    
    if data.get('timestamp'):
        try:
            ts = data['timestamp'].replace('+03:00', '')
            dt = datetime.fromisoformat(ts)
            params["created_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
    
    try:
        response = requests.post(thingspeak_update_url, params=params, timeout=10)
        if response.status_code == 200:
            print("✅ Real-time data sent to ThingSpeak")
            return True
        elif response.status_code == 429:
            print("⏳ Real-time: Rate limited, will retry later")
            return False
        else:
            print(f"❌ ThingSpeak upload failed: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error uploading to ThingSpeak: {e}")
        return False

def get_complete_thingspeak_timestamps():
    """Get ALL timestamps from ThingSpeak to avoid any duplicates"""
    print("🔍 Getting complete ThingSpeak timestamp list...")
    all_timestamps = set()
    
    # ThingSpeak returns max 8000 entries per request
    url = f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.json"
    params = {
        'api_key': thingspeak_read_api_key,
        'results': 8000
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            feeds = response.json().get('feeds', [])
            for feed in feeds:
                try:
                    dt_utc = datetime.strptime(feed['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                    dt_uganda = dt_utc + timedelta(hours=3)
                    timestamp = dt_uganda.isoformat() + '+03:00'
                    all_timestamps.add(timestamp)
                except (ValueError, TypeError):
                    continue
            print(f"📥 Found {len(all_timestamps)} existing records in ThingSpeak")
        else:
            print(f"❌ Error fetching ThingSpeak data: {response.status_code}")
    except Exception as e:
        print(f"❌ Error fetching existing timestamps: {e}")
    
    return all_timestamps

def sync_local_csv_to_thingspeak_smart():
    """Smart sync that respects rate limits and avoids duplicates"""
    print("🧠 Smart syncing local CSV to ThingSpeak...")
    
    if not os.path.exists(csv_file):
        print("❌ No local CSV file found")
        return
    
    try:
        df = pd.read_csv(csv_file)
        if len(df) == 0:
            print("📊 Local CSV is empty")
            return
        
        print(f"📊 Found {len(df)} records in local CSV")
        
        # Get ALL existing ThingSpeak data to avoid duplicates
        existing_timestamps = get_complete_thingspeak_timestamps()
        
        # Convert DataFrame to list of dictionaries
        data_list = []
        new_records_count = 0
        
        for _, row in df.iterrows():
            data = {
                'timestamp': row['timestamp'],
                'Battery': row['Battery'] if pd.notna(row['Battery']) else None,
                'Humidity': row['Humidity'] if pd.notna(row['Humidity']) else None,
                'Motion': row['Motion'] if pd.notna(row['Motion']) else None,
                'Temperature': row['Temperature'] if pd.notna(row['Temperature']) else None
            }
            
            # Only add if not already in ThingSpeak
            if data['timestamp'] not in existing_timestamps:
                data_list.append(data)
                new_records_count += 1
        
        print(f"📤 Found {new_records_count} new records to upload")
        
        if new_records_count == 0:
            print("✅ Local CSV already synced with ThingSpeak")
            return True
        
        # Upload in smaller chunks with proper rate limiting
        chunk_size = 50  # Smaller chunks to avoid rate limits
        total_chunks = (len(data_list) - 1) // chunk_size + 1
        successful_uploads = 0
        
        for i in range(0, len(data_list), chunk_size):
            chunk = data_list[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            print(f"📦 Uploading chunk {chunk_num}/{total_chunks} ({len(chunk)} records)...")
            
            success = upload_bulk_to_thingspeak_smart(chunk)
            if success:
                successful_uploads += len(chunk)
            
            # Respectful delay between chunks regardless of success
            if chunk_num < total_chunks:
                print("⏳ Waiting 30 seconds before next chunk...")
                time.sleep(30)
        
        print(f"🎉 Smart sync completed! {successful_uploads}/{new_records_count} records uploaded")
        return successful_uploads == new_records_count
            
    except Exception as e:
        print(f"❌ Error in smart sync: {e}")
        return False

def download_thingspeak_complete_data(filename="thingspeak_complete_data.csv"):
    """Download complete data from ThingSpeak for MATLAB training"""
    print(f"📥 Downloading complete ThingSpeak data to {filename}...")
    
    url = f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.csv"
    params = {
        'api_key': thingspeak_read_api_key,
        'results': 8000  # Maximum results
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                f.write(response.text)
            
            # Process the downloaded CSV to match our local format
            df = pd.read_csv(filename)
            print(f"✅ Downloaded {len(df)} records from ThingSpeak")
            
            # Convert ThingSpeak format to our local format
            if len(df) > 0:
                processed_data = []
                for _, row in df.iterrows():
                    try:
                        dt_utc = datetime.strptime(row['created_at'], "%Y-%m-%d %H:%M:%S UTC")
                        dt_uganda = dt_utc + timedelta(hours=3)
                        timestamp = dt_uganda.isoformat() + '+03:00'
                        
                        processed_data.append({
                            "timestamp": timestamp,
                            "Battery": row['field1'] if pd.notna(row['field1']) else None,
                            "Humidity": row['field2'] if pd.notna(row['field2']) else None,
                            "Motion": row['field3'] if pd.notna(row['field3']) else None,
                            "Temperature": row['field4'] if pd.notna(row['field4']) else None
                        })
                    except (ValueError, KeyError):
                        continue
                
                # Save processed data
                processed_df = pd.DataFrame(processed_data)
                processed_df.to_csv(filename, index=False)
                print(f"🔄 Processed {len(processed_df)} records for MATLAB compatibility")
            
            return True
        else:
            print(f"❌ Failed to download: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Download error: {e}")
        return False

def sync_thingspeak_to_csv_smart():
    """Smart sync from ThingSpeak to CSV - avoids duplicates and handles errors"""
    print("🔄 Smart syncing ThingSpeak data to CSV...")
    
    try:
        # Get complete data from ThingSpeak
        url = f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.json"
        params = {
            'api_key': thingspeak_read_api_key,
            'results': 8000
        }
        
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            print(f"❌ Failed to fetch ThingSpeak data: {response.status_code}")
            return
        
        feeds = response.json().get('feeds', [])
        print(f"📊 Found {len(feeds)} feeds in ThingSpeak")
        
        if not feeds:
            print("❌ No data available in ThingSpeak")
            return
        
        csv_data_list = []
        for feed in feeds:
            try:
                dt_utc = datetime.strptime(feed['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                dt_uganda = dt_utc + timedelta(hours=3)
                timestamp = dt_uganda.isoformat() + '+03:00'
                
                csv_data = {
                    "timestamp": timestamp,
                    "Battery": float(feed.get('field1')) if feed.get('field1') not in [None, ''] else None,
                    "Humidity": float(feed.get('field2')) if feed.get('field2') not in [None, ''] else None,
                    "Motion": int(feed.get('field3')) if feed.get('field3') not in [None, ''] else None,
                    "Temperature": float(feed.get('field4')) if feed.get('field4') not in [None, ''] else None
                }
                csv_data_list.append(csv_data)
            except (ValueError, TypeError, KeyError) as e:
                continue

        if csv_data_list:
            new_rows = save_to_csv(csv_data_list)
            print(f"✅ Smart sync: {new_rows} new entries added from ThingSpeak")
        else:
            print("💾 No new data from ThingSpeak (all duplicates)")
            
    except Exception as e:
        print(f"❌ ThingSpeak sync error: {e}")

# ------------------- Fixed Historical Data Fetch -------------------
def get_historical_and_upload_smart():
    """Smart historical data fetch with better error handling"""
    print("🔄 Starting smart historical data fetch from TTN...")
    start_time = time.time()

    app_id = username.split('@')[0]
    url = f"https://{broker}/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"
    headers = {"Authorization": f"Bearer {password}"}
    params = {"last": "48h", "limit": 100}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        print("✅ TTN historical data fetched successfully")
        
    except requests.RequestException as e:
        print(f"❌ Error fetching from TTN: {e}")
        return

    historical_data_list = []
    line_count = 0
    valid_data_count = 0
    
    print("🔍 Processing historical data...")
    for line in response.text.splitlines():
        if not line.strip():
            continue
        line_count += 1
        try:
            data = json.loads(line)
            
            result = data.get("result", {})
            uplink = result.get("uplink_message", {})
            decoded = uplink.get("decoded_payload", {})
            
            if not decoded:
                continue

            # Field mapping
            Battery = decoded.get("field1")
            Humidity = decoded.get("field3")  
            Motion = decoded.get("field4")    
            Temperature = decoded.get("field5")

            received_at_utc = result.get("received_at")
            if received_at_utc:
                dt_utc = parse_ttn_timestamp(received_at_utc)
                dt_uganda = dt_utc + timedelta(hours=3)
                created_at = dt_uganda.isoformat() + '+03:00'
            else:
                created_at = datetime.utcnow().isoformat() + "Z"

            # Only add if we have valid sensor data
            if Battery is not None or Humidity is not None or Motion is not None or Temperature is not None:
                historical_data_list.append({
                    "timestamp": created_at,
                    "Battery": Battery,
                    "Humidity": Humidity,
                    "Motion": Motion,
                    "Temperature": Temperature
                })
                valid_data_count += 1

        except Exception as e:
            continue

    print(f"📊 Processed {line_count} lines, found {valid_data_count} valid data entries")

    # Save to CSV and upload to ThingSpeak if we have data
    if historical_data_list:
        new_rows = save_to_csv(historical_data_list)
        print(f"💾 Saved {new_rows} new historical entries to CSV")
        
        if new_rows > 0:
            print(f"⚡ Uploading {new_rows} new historical entries to ThingSpeak...")
            # Only upload the new data that was actually saved
            upload_bulk_to_thingspeak_smart(historical_data_list[-new_rows:])
        else:
            print("💾 No new historical data to upload (all duplicates)")
    else:
        print("❌ No historical data found")

    print(f"✅ Historical fetch completed in {time.time() - start_time:.2f} seconds.")

# ------------------- MQTT Callbacks -------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("✅ Connected to TTN MQTT broker!")
        client.subscribe(f"v3/{username}/devices/{device_id}/up")
        print("📡 Listening for real-time sensor data...")
    else:
        print(f"❌ Connection failed: {reason_code}")

def on_message(client, userdata, msg):
    """Handle real-time MQTT messages"""
    try:
        payload = json.loads(msg.payload.decode())
        uplink = payload.get("uplink_message", {})
        decoded = uplink.get("decoded_payload", {})
        
        if not decoded:
            return

        print(f"📨 Real-time data: {decoded}")
        
        Battery = decoded.get("field1")
        Humidity = decoded.get("field3")  
        Motion = decoded.get("field4")    
        Temperature = decoded.get("field5")

        received_at_utc = payload.get("received_at")
        if received_at_utc:
            dt_utc = parse_ttn_timestamp(received_at_utc)
            dt_uganda = dt_utc + timedelta(hours=3)
            timestamp = dt_uganda.isoformat() + "+03:00"
        else:
            timestamp = (datetime.utcnow() + timedelta(hours=3)).isoformat() + "+03:00"

        realtime_data = {
            "timestamp": timestamp,
            "Battery": Battery,
            "Humidity": Humidity,
            "Motion": Motion,
            "Temperature": Temperature
        }
        
        # Save to CSV immediately
        save_to_csv([realtime_data])
        
        # Send to ThingSpeak with smart handling
        upload_single_to_thingspeak_smart(realtime_data)
        
    except Exception as e:
        print(f"❌ MQTT error: {e}")

# ------------------- MQTT Setup -------------------
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(username, password)
client.reconnect_delay_set(min_delay=1, max_delay=300)
client.on_connect = on_connect
client.on_message = on_message

def run_mqtt():
    while True:
        try:
            client.connect(broker, port, 60)
            client.loop_forever()
        except Exception as e:
            print(f"❌ MQTT disconnected: {e}")
            time.sleep(10)

# ------------------- MATLAB Integration -------------------
def generate_matlab_access_code():
    """Generate MATLAB code for accessing ThingSpeak data"""
    print("\n🔗 MATLAB Access Code:")
    print("=" * 60)
    print("% Connect to your ThingSpeak channel")
    print(f"channelID = {thingspeak_channel_id};")
    print(f"readAPIKey = '{thingspeak_read_api_key}';")
    print("\n% Read all data fields")
    print("data = thingSpeakRead(channelID, 'ReadKey', readAPIKey, 'NumPoints', 8000);")
    print("\n% Or read specific fields:")
    print("% Temperature (Field 4)")
    print("temperatureData = thingSpeakRead(channelID, 'Fields', 4, 'ReadKey', readAPIKey);")
    print("\n% Download CSV directly:")
    print(f"csvURL = 'https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.csv?api_key={thingspeak_read_api_key}';")
    print("websave('thingspeak_data.csv', csvURL);")
    print("=" * 60)

# ------------------- Main -------------------
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 OPTIMIZED IoT Data Collection System")
    print("=" * 60)
    
    # Initialize CSV
    initialize_csv()
    
    # Step 1: Smart sync local CSV to ThingSpeak (with rate limit handling)
    print("\n📊 Step 1: Smart syncing local CSV to ThingSpeak")
    print("-" * 40)
    sync_local_csv_to_thingspeak_smart()
    
    # Step 2: Download complete ThingSpeak data for MATLAB
    print("\n📊 Step 2: Downloading ThingSpeak data for MATLAB")
    print("-" * 40)
    download_thingspeak_complete_data("thingspeak_training_data.csv")
    
    # Step 3: Smart sync from ThingSpeak to CSV
    print("\n📊 Step 3: Smart syncing from ThingSpeak to CSV")
    print("-" * 40)
    sync_thingspeak_to_csv_smart()
    
    # Step 4: Generate MATLAB access code
    print("\n📊 Step 4: Generating MATLAB Access Information")
    print("-" * 40)
    generate_matlab_access_code()
    
    # Step 5: Get additional historical data
    print("\n📊 Step 5: Getting additional historical data from TTN") 
    print("-" * 40)
    get_historical_and_upload_smart()
    
    # Step 6: Start real-time MQTT listener
    print("\n📊 Step 6: Starting real-time data collection")
    print("-" * 40)
    mqtt_thread = threading.Thread(target=run_mqtt, daemon=True)
    mqtt_thread.start()
    
    time.sleep(3)

    # Step 7: Schedule optimized updates
    print("\n📊 Step 7: Setting up optimized schedules")
    print("-" * 40)
    
    # Reduced frequency to avoid rate limits
    schedule.every(12).hours.do(sync_thingspeak_to_csv_smart)  # Less frequent
    schedule.every().day.at("13:02").do(get_historical_and_upload_smart)
    schedule.every().day.at("03:00").do(sync_local_csv_to_thingspeak_smart)  # Off-peak
    schedule.every().day.at("07:00").do(lambda: download_thingspeak_complete_data())
    
    print("✅ ThingSpeak sync: Every 12 hours (reduced frequency)")
    print("✅ TTN historical: Daily at 13:02")
    print("✅ Smart CSV sync: Daily at 03:00 (off-peak)")
    print("✅ Training data: Daily at 07:00")
    print("✅ Real-time: Continuous with rate limit handling")

    # Final status
    print("\n" + "=" * 60)
    print("✅ System initialization completed!")
    print(f"📁 Local CSV: {os.path.abspath(csv_file)}")
    print(f"📁 MATLAB CSV: thingspeak_training_data.csv")
    
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        print(f"📊 Current data: {len(df)} rows in local CSV")
    
    print("📡 Real-time monitoring: ACTIVE")
    print("⏰ Scheduled tasks: ACTIVE")
    print("⚡ Data sync: SMART rate limit handling")
    print("🔗 MATLAB access: Ready (see above for code)")
    print("=" * 60)
    print("\n🔄 System is running. Press Ctrl+C to stop.")
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Script stopped by user")