# Generated by Django 3.2.16 on 2023-04-24 11:34

import django.contrib.postgres.fields
from django.db import migrations, models
import django.db.models.deletion
import posthog.models.utils


class Migration(migrations.Migration):

    dependencies = [
        ("posthog", "0311_dashboard_template_scope"),
    ]

    operations = [
        migrations.CreateModel(
            name="ExportDestination",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=posthog.models.utils.UUIDT, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("name", models.TextField()),
                ("type", models.CharField(choices=[("S3", "S3")], max_length=64)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("last_updated_at", models.DateTimeField(auto_now_add=True)),
                ("config", models.JSONField(blank=True, default=dict)),
                ("team", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="posthog.team")),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="ExportSchedule",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=posthog.models.utils.UUIDT, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("last_updated_at", models.DateTimeField(auto_now_add=True)),
                ("paused_at", models.DateTimeField(null=True)),
                ("unpaused_at", models.DateTimeField(null=True)),
                ("start_at", models.DateTimeField(null=True)),
                ("end_at", models.DateTimeField(null=True)),
                ("name", models.CharField(max_length=256)),
                (
                    "calendars",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.JSONField(), blank=True, default=list, size=None
                    ),
                ),
                (
                    "intervals",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.JSONField(), blank=True, default=list, size=None
                    ),
                ),
                (
                    "cron_expressions",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.TextField(), blank=True, default=list, size=None
                    ),
                ),
                (
                    "skip",
                    django.contrib.postgres.fields.ArrayField(base_field=models.JSONField(), default=list, size=None),
                ),
                ("jitter", models.DurationField(null=True)),
                ("time_zone_name", models.CharField(default="Etc/UTC", max_length=64, null=True)),
                (
                    "destination",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="schedules",
                        to="posthog.exportdestination",
                    ),
                ),
                ("team", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="posthog.team")),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="ExportRun",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=posthog.models.utils.UUIDT, editable=False, primary_key=True, serialize=False
                    ),
                ),
                ("run_id", models.TextField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("Running", "Running"),
                            ("Cancelled", "Cancelled"),
                            ("Completed", "Completed"),
                            ("ContinuedAsNew", "Continuedasnew"),
                            ("Failed", "Failed"),
                            ("Terminated", "Terminated"),
                            ("TimedOut", "Timedout"),
                        ],
                        max_length=64,
                    ),
                ),
                ("opened_at", models.DateTimeField(null=True)),
                ("closed_at", models.DateTimeField(null=True)),
                ("data_interval_start", models.DateTimeField()),
                ("data_interval_end", models.DateTimeField()),
                (
                    "schedule",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="posthog.exportschedule"),
                ),
            ],
            options={
                "abstract": False,
            },
        ),
    ]
