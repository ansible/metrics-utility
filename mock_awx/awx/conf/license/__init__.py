class Unlicense:
    def validate(self):
        return {
            'license_type': 'UNLICENSED',
            'product_name': 'AWX',
            'subscription_name': None,
            'valid_key': False,
        }


def get_license():
    return Unlicense().validate()
