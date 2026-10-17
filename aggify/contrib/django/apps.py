from django.apps import AppConfig


class AggifyConfig(AppConfig):
    name = "aggify.contrib.django"
    label = "aggify"
    verbose_name = "Aggify (MongoDB)"
    default_auto_field = "django.db.models.AutoField"

    def ready(self) -> None:
        from aggify.contrib.django.conf import setup_from_settings

        setup_from_settings()
