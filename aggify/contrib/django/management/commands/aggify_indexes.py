from django.core.management.base import BaseCommand

from aggify import registry
from aggify.documents import Document


class Command(BaseCommand):
    help = "Create the indexes declared on Aggify documents (Meta.indexes, Field(index=True/unique=True))."

    def add_arguments(self, parser):
        parser.add_argument("models", nargs="*", help="Model names or app_label.Model (default: all)")

    def handle(self, *args, **options):
        wanted = set(options["models"])
        done = 0
        seen = set()
        for bucket in registry._models.values():
            for model in bucket:
                if model in seen or not issubclass(model, Document) or model._meta.abstract:
                    continue
                seen.add(model)
                if wanted and model.__name__ not in wanted and model._meta.label not in wanted:
                    continue
                names = model.ensure_indexes()
                self.stdout.write(f"{model._meta.label}: {len(names)} index(es)")
                done += 1
        self.stdout.write(self.style.SUCCESS(f"Done ({done} model(s))."))
