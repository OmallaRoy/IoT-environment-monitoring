import csv
from django.core.management.base import BaseCommand
from django.utils.dateparse import parse_datetime
from dashboard.models import SensorReading
from django.conf import settings
import os

# CSV path relative to project root
CSV_PATH = os.path.join(settings.BASE_DIR, "sensor_data.csv")

class Command(BaseCommand):
    help = "Import sensor_data.csv into SensorReading (only new rows)"

    def handle(self, *args, **kwargs):
        if not os.path.exists(CSV_PATH):
            self.stdout.write(self.style.ERROR(f"No CSV at {CSV_PATH}"))
            return

        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                ts = parse_datetime(row.get("timestamp"))
                if not ts:
                    continue
                # skip if exists
                if SensorReading.objects.filter(timestamp=ts).exists():
                    continue
                sr = SensorReading(
                    timestamp=ts,
                    Battery=row.get("Battery") or None,# replaced these with mine
                    Humidity=row.get("Humidity") or None,
                    Motion=row.get("Motion") or None,
                    Temperature=row.get("Temperature") or None,
                )
                sr.save()
                count += 1

        self.stdout.write(self.style.SUCCESS(f"Imported {count} rows"))
