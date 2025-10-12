from django.apps import AppConfig
import joblib
import os

class MlConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'ml'

    def ready(self):
        """
        Load the trained RandomForest model into memory at startup.
        """
        from django.conf import settings
        from . import model_store

        model_path = os.path.join(settings.BASE_DIR, 'ml', 'models', 'temp_hum_model.joblib')
        model_store.rf = joblib.load(model_path)
