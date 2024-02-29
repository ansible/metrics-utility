class BadShipTarget(Exception):
    def __init__(self, message):
        self.name = message


class MissingRequiredEnvVar(Exception):
    def __init__(self, message):
        self.name = message