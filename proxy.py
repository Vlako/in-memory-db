from multiprocessing import Process
from os import environ, path

import requests
from flask import Flask, redirect

app = Flask(__name__)

shardfile = environ.get("SHARD_FILE", default='shards1')
with open(shardfile) as file:
    shard_urls = [line.strip() for line in file]


def reshard():
    for i, url in enumerate(shard_urls):
        response = requests.get(path.join(url, 'all')).json()
        for key in response:
            key_hash = hash(key) % len(shard_urls)
            if key_hash != i:
                val = requests.get(path.join(url, key)).text
                requests.put(path.join(shard_urls[key_hash], key), data=val)
                requests.delete(path.join(url, key))


process = Process(target=reshard)
process.start()


@app.route('/<id_>', methods=['GET', 'PUT', 'DELETE'])
def get(id_):
    url = shard_urls[hash(id_) % len(shard_urls)]
    return redirect(path.join(url, id_), 307)


if __name__ == '__main__':
    app.run()
