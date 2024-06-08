class BadShipTarget(Exception):
    def __init__(self, message):
        self.name = message


class MissingRequiredEnvVar(Exception):
    def __init__(self, message):
        self.name = message


class BadRequiredEnvVar(Exception):
    def __init__(self, message):
        self.name = message


class FailedToUploadPayload(Exception):
    def __init__(self, message):
        self.name = message


class NoAnalyticsCollected(Exception):
    def __init__(self, message):
        self.name = message

class UnparsableParameter(Exception):
    def __init__(self, message):
        self.name = message
