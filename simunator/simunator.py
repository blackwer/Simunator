import itertools


class Simunator:
    def __init__(self):
        self._params = {}

    def add_param(self, key: str, shortname: str, description="", bounds=(0.0, 1.0)):
        self._params[key] = {
            "shortname": shortname,
            "description": description,
            "bounds": bounds,
        }

    def print_combos(self):
        for keys in itertools.product(list(self._params.keys())):
            print(keys)
