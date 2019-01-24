import json
from multiprocessing import Process
from os import environ, path

import requests
from flask import Flask, request, abort

from db import Database

app = Flask(__name__)
dump_path = environ.get("DUMP_PATH", default='dump.json')

db = Database(dump_path)

follower_file = environ.get("FOLLOWER_FILE", default=None)

if follower_file:
    with open(follower_file) as file:
        follower_urls = [line.strip() for line in file]
else:
    follower_urls = []

sync = environ.get("SYNC", default=False)


def replicate(request_func, id_, data=None):
    for url in follower_urls:
        request_func(path.join(url, id_), data=data)


@app.route('/<id_>', methods=['GET'])
def get(id_):
    return db.get(id_) or abort(404)


@app.route('/all', methods=['GET'])
def get_all():
    return json.dumps(list(db.data.keys()))


@app.route('/<id_>', methods=['PUT'])
def put(id_):
    db.put(id_, request.data.decode())
    process = Process(target=replicate, args=(requests.put, id_, request.data.decode()))
    process.start()
    if sync:
        process.join()
    return 'success'


@app.route('/<id_>', methods=['DELETE'])
def delete(id_):
    result = db.delete(id_)
    process = Process(target=replicate, args=(requests.delete, id_))
    process.start()
    if sync:
        process.join()
    return 'success' if result else abort(404)


if __name__ == '__main__':
    app.run()
