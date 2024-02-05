# Generated by Django 3.2.23 on 2024-02-06 16:50

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("posthog", "0388_add_schema_to_batch_exports"),
    ]

    operations = [
        migrations.RunSQL(
            "ALTER TABLE posthog_persondistinctid ALTER COLUMN person_id DROP NOT NULL",
            state_operations=[
                migrations.AlterField(
                    model_name="persondistinctid",
                    name="person",
                    field=models.ForeignKey(
                        null=True, on_delete=django.db.models.deletion.CASCADE, to="posthog.person"
                    ),
                ),
            ],
        )
    ]
