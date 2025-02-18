from django.db import models


class Setting(models.Model):
    key = models.CharField(max_length=255)
    value = models.JSONField(null=True)
