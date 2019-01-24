import json
from os import path


class Database:

    def __init__(self, dump_path):
        self.data = {}
        self.dump_path = dump_path

        if path.isfile(dump_path):
            with open(dump_path, 'r') as dump_file:
                self.data = json.load(dump_file)

    def get(self, id_):
        return self.data.get(id_)

    def put(self, id_, value):
        self.data[id_] = value
        self.dump()

    def delete(self, id_):
        try:
            self.data.pop(id_)
            self.dump()
            return True
        except KeyError:
            return False

    def dump(self):
        with open(self.dump_path, 'w') as dump_file:
            json.dump(self.data, dump_file)
