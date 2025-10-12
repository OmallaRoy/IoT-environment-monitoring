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
        else:
            print("💾 No new data (duplicates)")
    else:
        df_new.to_csv(csv_file, index=False)
        print(f"💾 Created CSV with {len(df_new)} rows")

# ------------------- ThingSpeak to CSV Sync -------------------
def sync_thingspeak_to_csv():
    """Sync data from ThingSpeak to CSV - This works reliably"""
    print("🔄 Syncing ThingSpeak data to CSV...")
    
    # Get last 48 hours of data from ThingSpeak
    start_utc = datetime.utcnow() - timedelta(hours=48)
    url = (
        f"https://api.thingspeak.com/channels/{thingspeak_channel_id}/feeds.json"
        f"?api_key={thingspeak_read_api_key}&results=8000"
        f"&start={start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )

    try:
        response = requests.get(url, timeout=10)
        feeds = response.json().get('feeds', [])
        print(f"📊 Found {len(feeds)} feeds in ThingSpeak")
        
        csv_data_list = []
        for feed in feeds:
            try:
                dt_utc = datetime.strptime(feed['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                dt_uganda = dt_utc + timedelta(hours=3)
                timestamp = dt_uganda.isoformat() + '+03:00'
                
                csv_data = {
                    "timestamp": timestamp,
                    "Battery": float(feed.get('field1')) if feed.get('field1') else None,
                    "Humidity": float(feed.get('field2')) if feed.get('field2') else None,
                    "Motion": int(feed.get('field3')) if feed.get('field3') else None,
                    "Temperature": float(feed.get('field4')) if feed.get('field4') else None
                }
                csv_data_list.append(csv_data)
            except (ValueError, TypeError):
                continue

        if csv_data_list:
            save_to_csv(csv_data_list)
            print(f"✅ Synced {len(csv_data_list)} entries from ThingSpeak")
        else:
            print("❌ No data from ThingSpeak")
            
    except Exception as e:
        print(f"❌ ThingSpeak sync error: {e}")

# ------------------- Fixed Historical Data Fetch -------------------
def get_historical_and_upload():
    """Fixed historical data fetch with correct field mapping"""
    print("🔄 Starting historical data fetch from TTN...")
    start_time = time.time()

    app_id = username.split('@')[0]
    url = f"https://{broker}/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"
    headers = {"Authorization": f"Bearer {password}"}
    params = {"last": "48h", "limit": 100}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        print("✅ TTN historical data fetched successfully")
        
    except requests.RequestException as e:
        print(f"❌ Error fetching from TTN: {e}")
        return

    updates = []
    historical_data_list = []
    line_count = 0
    valid_data_count = 0
    
    print("🔍 Processing historical data with CORRECT field mapping...")
    for line in response.text.splitlines():
        if not line.strip():
            continue
        line_count += 1
        try:
            data = json.loads(line)
            
            # Extract from the correct structure - TTN storage API uses "result" -> "uplink_message"
            result = data.get("result", {})
            uplink = result.get("uplink_message", {})
            decoded = uplink.get("decoded_payload", {})
            
            if not decoded:
                continue

            # CORRECT FIELD MAPPING - SAME AS REAL-TIME DATA!
            # Based on your debug output, historical data uses the SAME fields as real-time:
            Battery = decoded.get("field1")      # 3.066
            Humidity = decoded.get("field3")     # 63.4  
            Motion = decoded.get("field4")       # 1
            Temperature = decoded.get("field5")  # 26.51

            # Debug: Print first few entries to verify
            if line_count <= 3:
                print(f"📋 Sample historical data (line {line_count}):")
                print(f"  Battery (field1): {Battery}")
                print(f"  Humidity (field3): {Humidity}")
                print(f"  Motion (field4): {Motion}")
                print(f"  Temperature (field5): {Temperature}")
                print(f"  All fields: {list(decoded.keys())}")

            received_at_utc = result.get("received_at")
            if received_at_utc:
                dt_utc = parse_ttn_timestamp(received_at_utc)
                dt_uganda = dt_utc + timedelta(hours=3)
                created_at = dt_uganda.isoformat() + '+03:00'
            else:
                created_at = datetime.utcnow().isoformat() + "Z"

            # Only add if we have valid sensor data
            if Battery is not None or Humidity is not None or Motion is not None or Temperature is not None:
                update = {"created_at": created_at}
                if Battery is not None: update["field1"] = Battery
                if Humidity is not None: update["field2"] = Humidity
                if Motion is not None: update["field3"] = Motion
                if Temperature is not None: update["field4"] = Temperature
                updates.append(update)

                historical_data_list.append({
                    "timestamp": created_at,
                    "Battery": Battery,
                    "Humidity": Humidity,
                    "Motion": Motion,
                    "Temperature": Temperature
                })
                valid_data_count += 1

        except Exception as e:
            print(f"⚠️ Error on line {line_count}: {e}")
            continue

    print(f"📊 Processed {line_count} lines, found {valid_data_count} valid data entries")

    # Upload to ThingSpeak if we have data
    if updates:
        print(f"☁️ Uploading {len(updates)} historical entries to ThingSpeak...")
        bulk_data = {"write_api_key": thingspeak_write_api_key, "updates": updates}
        try:
            r = requests.post(thingspeak_bulk_url, json=bulk_data, timeout=10)
            print(f"📊 Bulk update response: {r.status_code}")
            if r.status_code == 202:
                print("✅ Historical data uploaded to ThingSpeak!")
                save_to_csv(historical_data_list)
            else:
                print(f"❌ ThingSpeak upload failed: {r.text}")
        except Exception as e:
            print(f"❌ Upload error: {e}")
    else:
        print("❌ No historical data found - check field mapping")

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
    """Handle real-time MQTT messages - THIS WORKS CORRECTLY"""
    try:
        payload = json.loads(msg.payload.decode())
        uplink = payload.get("uplink_message", {})
        decoded = uplink.get("decoded_payload", {})
        
        if not decoded:
            return

        print(f"📨 Real-time data: {decoded}")
        
        # Real-time data uses field1, field3, field4, field5
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

        # Save to CSV immediately
        realtime_data = [{
            "timestamp": timestamp,
            "Battery": Battery,
            "Humidity": Humidity,
            "Motion": Motion,
            "Temperature": Temperature
        }]
        
        save_to_csv(realtime_data)
        
        # Send to ThingSpeak
        params = {"api_key": thingspeak_write_api_key}
        if Battery is not None: params["field1"] = Battery
        if Humidity is not None: params["field2"] = Humidity
        if Motion is not None: params["field3"] = Motion
        if Temperature is not None: params["field4"] = Temperature

        response = requests.post(thingspeak_update_url, params=params, timeout=5)
        if response.status_code == 200:
            print("✅ Sent to ThingSpeak")
        
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

# ------------------- Main -------------------
if __name__ == "__main__":
    print("=" * 50)
    print("🚀 IoT Data Collection System")
    print("=" * 50)
    
    # Initialize CSV
    initialize_csv()
    
    # Step 1: Get historical data from ThingSpeak (this works reliably)
    print("\n📊 Step 1: Getting historical data from ThingSpeak")
    print("-" * 40)
    sync_thingspeak_to_csv()
    
    # Step 2: Try to get additional historical data from TTN with correct field mapping
    print("\n📊 Step 2: Getting additional historical data from TTN") 
    print("-" * 40)
    get_historical_and_upload()
    
    # Step 3: Start real-time MQTT listener
    print("\n📊 Step 3: Starting real-time data collection")
    print("-" * 40)
    mqtt_thread = threading.Thread(target=run_mqtt, daemon=True)
    mqtt_thread.start()
    
    # Give MQTT time to connect
    time.sleep(3)

    # Step 4: Schedule regular updates
    print("\n📊 Step 4: Setting up scheduled updates")
    print("-" * 40)
    
    # Schedule ThingSpeak sync every 6 hours (reliable backup)
    schedule.every(6).hours.do(sync_thingspeak_to_csv)
    
    # Schedule TTN historical fetch once per day
    #22:39
    schedule.every().day.at("20:23").do(get_historical_and_upload)
    
    print("✅ ThingSpeak sync: Every 6 hours")
    print("✅ TTN historical: Daily at 21:50")
    print("✅ Real-time: Continuous")

    # Final status
    print("\n" + "=" * 50)
    print("✅ System initialization completed!")
    print(f"📁 CSV file: {os.path.abspath(csv_file)}")
    
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        print(f"📊 Current data: {len(df)} rows in CSV")
    
    print("📡 Real-time monitoring: ACTIVE")
    print("⏰ Scheduled tasks: ACTIVE")
    print("=" * 50)
    print("\n🔄 System is running. Press Ctrl+C to stop.")
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Script stopped by user")