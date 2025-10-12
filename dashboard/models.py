from django.db import models

# made changes 
class SensorReading(models.Model):
    timestamp = models.DateTimeField(db_index=True)
    Battery = models.FloatField(null=True, blank=True)
    Humidity = models.FloatField(null=True, blank=True)
    Motion = models.IntegerField(null=True, blank=True)
    Temperature = models.FloatField(null=True, blank=True)

    class Meta:
        ordering = ["-timestamp"]

    def __str__(self):
        return f"{self.timestamp} T={self.Temperature} H={self.Humidity}"
