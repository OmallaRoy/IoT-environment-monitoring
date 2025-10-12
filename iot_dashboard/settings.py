# # iot_dashboard/settings.py
# from pathlib import Path
# import os
# from dotenv import load_dotenv

# # Load environment variables from a .env file (install python-dotenv)
# load_dotenv()

# # Base directory
# BASE_DIR = Path(__file__).resolve().parent.parent

# # SECURITY
# # Put sensitive values in a .env file (see README below).
# SECRET_KEY = os.getenv(
#     "DJANGO_SECRET_KEY",
#     "django-insecure-replace-this-in-production"  # fallback for dev only
# )

# DEBUG = os.getenv("DJANGO_DEBUG", "True").lower() in ("1", "true", "yes")

# # Comma-separated, e.g. "127.0.0.1,localhost,mydomain.com"
# ALLOWED_HOSTS = [h.strip() for h in os.getenv("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost").split(",") if h.strip()]

# # Application definition
# INSTALLED_APPS = [
#     "django.contrib.admin",
#     "django.contrib.auth",
#     "django.contrib.contenttypes",
#     "django.contrib.sessions",
#     "django.contrib.messages",
#     "django.contrib.staticfiles",

#     # 3rd-party
#     "rest_framework",

#     # Local apps
#     "dashboard",
#     "ml",
# ]

# MIDDLEWARE = [
#     # If you want to serve static files in production directly from Django,
#     # consider using WhiteNoise. Uncomment below after installing whitenoise.
#     # "whitenoise.middleware.WhiteNoiseMiddleware",

#     "django.middleware.security.SecurityMiddleware",
#     "django.contrib.sessions.middleware.SessionMiddleware",
#     "django.middleware.common.CommonMiddleware",
#     "django.middleware.csrf.CsrfViewMiddleware",
#     "django.contrib.auth.middleware.AuthenticationMiddleware",
#     "django.contrib.messages.middleware.MessageMiddleware",
#     "django.middleware.clickjacking.XFrameOptionsMiddleware",
# ]

# ROOT_URLCONF = "iot_dashboard.urls"

# TEMPLATES = [
#     {
#         "BACKEND": "django.template.backends.django.DjangoTemplates",
#         # Put project-level templates in BASE_DIR / "templates"
#         "DIRS": [BASE_DIR / "templates"],
#         "APP_DIRS": True,
#         "OPTIONS": {
#             "context_processors": [
#                 "django.template.context_processors.debug",
#                 "django.template.context_processors.request",
#                 "django.template.context_processors.static",
#                 "django.template.context_processors.media",
#                 "django.contrib.auth.context_processors.auth",
#                 "django.contrib.messages.context_processors.messages",
#             ],
#         },
#     },
# ]

# WSGI_APPLICATION = "iot_dashboard.wsgi.application"

# # Database (simple sqlite for development)
# DATABASES = {
#     "default": {
#         "ENGINE": "django.db.backends.sqlite3",
#         "NAME": BASE_DIR / "db.sqlite3",
#     }
# }

# # Password validation (default Django validators)
# AUTH_PASSWORD_VALIDATORS = [
#     {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",},
#     {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",},
#     {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",},
#     {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",},
# ]

# # Internationalization & timezone
# LANGUAGE_CODE = "en-us"

# # Use the user's timezone (Uganda). Django stores datetimes in UTC in DB by default
# # when USE_TZ = True; TIME_ZONE affects display/interpretation in templates/admin.
# TIME_ZONE = "Africa/Kampala"

# USE_I18N = True
# USE_TZ = True

# # Static files & media (uploads)
# STATIC_URL = "/static/"
# STATIC_ROOT = BASE_DIR / "staticfiles"           # collectstatic target
# STATICFILES_DIRS = [BASE_DIR / "static"]         # dev static files

# MEDIA_URL = "/media/"
# MEDIA_ROOT = BASE_DIR / "media"

# # REST framework basics
# REST_FRAMEWORK = {
#     # In development you might allow browsable API; lock down in production.
#     "DEFAULT_PERMISSION_CLASSES": [
#         "rest_framework.permissions.AllowAny",
#     ],
#     "DEFAULT_RENDERER_CLASSES": [
#         "rest_framework.renderers.JSONRenderer",
#     ],
# }

# # Security-related toggles (simple)
# if not DEBUG:
#     # In production set these env vars appropriately.
#     SECURE_SSL_REDIRECT = os.getenv("DJANGO_SECURE_SSL_REDIRECT", "True").lower() in ("1","true","yes")
#     SESSION_COOKIE_SECURE = True
#     CSRF_COOKIE_SECURE = True

# # Default primary key field type
# DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


# iot_dashboard/settings.py
from pathlib import Path
import os
# from dotenv import load_dotenv

# Load environment variables from a .env file (install python-dotenv)
# load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY
# Put sensitive values in a .env file (see README below).
SECRET_KEY = os.getenv(
    "DJANGO_SECRET_KEY",
    "django-insecure-replace-this-in-production"  # fallback for dev only
)

# Detect Railway environment
RAILWAY_ENV = 'RAILWAY' in os.environ

# Set DEBUG based on environment
DEBUG = os.getenv("DJANGO_DEBUG", "True").lower() in ("1", "true", "yes") and not RAILWAY_ENV

# Comma-separated, e.g. "127.0.0.1,localhost,mydomain.com"
ALLOWED_HOSTS = [h.strip() for h in os.getenv("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost").split(",") if h.strip()]

# Add Railway domain if running on Railway
if RAILWAY_ENV:
    ALLOWED_HOSTS.extend(['.railway.app', 'localhost', '127.0.0.1'])

# Application definition
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",

    # 3rd-party
    "rest_framework",

    # Local apps
    "dashboard",
    "ml",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  # ← ADD THIS LINE (UNCOMMENTED)
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "iot_dashboard.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        # Put project-level templates in BASE_DIR / "templates"
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.template.context_processors.static",
                "django.template.context_processors.media",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "iot_dashboard.wsgi.application"

# Database (simple sqlite for development)
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

# Password validation (default Django validators)
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",},
]

# Internationalization & timezone
LANGUAGE_CODE = "en-us"

# Use the user's timezone (Uganda). Django stores datetimes in UTC in DB by default
# when USE_TZ = True; TIME_ZONE affects display/interpretation in templates/admin.
TIME_ZONE = "Africa/Kampala"

USE_I18N = True
USE_TZ = True

# Static files & media (uploads)
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"           # collectstatic target
STATICFILES_DIRS = [BASE_DIR / "static"]         # dev static files

# Add WhiteNoise static files configuration
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

# REST framework basics
REST_FRAMEWORK = {
    # In development you might allow browsable API; lock down in production.
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.AllowAny",
    ],
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
}

# Security-related toggles (simple)
if not DEBUG:
    # In production set these env vars appropriately.
    SECURE_SSL_REDIRECT = os.getenv("DJANGO_SECURE_SSL_REDIRECT", "True").lower() in ("1","true","yes")
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Railway-specific settings
if RAILWAY_ENV:
    # Ensure DEBUG is False on Railway
    DEBUG = False
    # Security settings for production
    SECURE_SSL_REDIRECT = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True