import os
from flask import Flask, url_for, request
import requests
import json
from google.cloud import pubsub_v1
import time
from datetime import datetime
from google.cloud import secretmanager
from google.cloud import storage
import base64
from yadt import scan_and_apply_tz


# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "climacell_data"
bucket = storage_client.bucket('climacell_data')


project_id = os.environ['PROJECT_ID']
topic_id = os.environ['TOPIC_ID']

app = Flask(__name__)


def create_file(payload, filename):
    """Create a file.

  The retry_params specified in the open call will override the default
  retry params for this particular file handle.

  Args:
    filename: filename.
  """
    blob = bucket.blob(filename)

    blob.upload_from_string(data=payload,
                            content_type='text/plain')


def access_secret_version(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8").strip()
    return payload


querystring = {
    "lat":
    os.environ['LAT'],
    "lon":
    os.environ['LON'],
    "unit_system":
    "si",
    "fields":
    "temp,feels_like,dewpoint,humidity,wind_speed,wind_direction,wind_gust,baro_pressure,precipitation,precipitation_type,sunrise,sunset,visibility,cloud_cover,cloud_base,cloud_ceiling,surface_shortwave_radiation,moon_phase,weather_code",
}

querystring_hourly = {
    "start_time": "now",
    "fields": querystring["fields"] + "precipitation_probability"
}

querystring_hourly = {**querystring_hourly, **querystring}

request_headers = {
    "content-type": "application/json",
    "apikey": access_secret_version(project_id, "CLIMACELL_API_KEY","latest")
}

climacell_url_realtime = "https://api.climacell.co/v3/weather/realtime"
climacell_url_hourly = "https://api.climacell.co/v3/weather/forecast/hourly"

def pubsub(json_payload,mode_data):
    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                print(f.result())
                futures.pop(data)
            except:  # noqa
                print("Please handle {}.".format(f.exception()))

        return callback

    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True,
            ),
        )
    topic_path = publisher.topic_path(project_id, topic_id)
    #futures.update({data: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, json_payload.encode("utf-8"), ordering_key='mode', mode=mode_data)
    futures[json_payload] = future
    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, json_payload))

    print(futures.items)

    # Wait for all the publish futures to resolve before exiting.
    while futures:
        time.sleep(1)


def get_metric_list_from_bucket(pre):
    blobs = list(storage_client.list_blobs(bucket, prefix=pre))
    metric_list = []
    for _b in blobs:
        datestr = ' '.join(_b.name.rsplit('-', 2)[1:3])
        try:
            #dateobj = datetime.strptime(filename, "%Y%m%d %H%M%S")
            item = {"name": _b.name, "dateobj": datestr}
            metric_list.append(item)
        except ValueError:
            pass

    return metric_list


@app.route('/realtime/', methods=['GET'])
def realtime():

    response_realtime = requests.get(climacell_url_realtime,
                                params=querystring,
                                headers=request_headers)

    print(response_realtime.status_code)
    # print(json.dumps(response_realtime.json(), indent=4, sort_keys=True))

    _json = scan_and_apply_tz(response_realtime.json())

    pubsub(json.dumps(_json),'realtime')

    return json.dumps(response_realtime.json(), indent=4, sort_keys=True)


@app.route('/hourly/', methods=['GET'])
def hourly():

    response_hourly = requests.get(climacell_url_hourly,
                                   params=querystring_hourly,
                                   headers=request_headers)

    print(response_hourly.status_code)
    # print(json.dumps(response_hourly.json(), indent=4, sort_keys=True))

    _json = scan_and_apply_tz(response_hourly.json())

    pubsub(json.dumps(_json),'hourly')

    return json.dumps(response_hourly.json(), indent=4, sort_keys=True)

def last_range(last):
    if last != 1:
        last = range(-1, 0 - int(last) - 1, -1)
    else:
        last = range(-1, -2, -1)

    return last

def last_json(last,blobs):
    last_json = []
    for i in last:
        j = json.loads(blobs[i].download_as_string())
        j = scan_and_apply_tz(j)
        if isinstance(j, list):
            # For hourly forecast, one blob contain several days
            # We need to split them in order to replicate the
            # same structure we have with realtime.
            for item in j:
                item['name'] = blobs[i].name
                last_json.append(item)
        else:
            # For realtime data, one day is already in a single object
            j['name'] = blobs[i].name
            last_json.append(j)
    return last_json

def range_start_end(blobs, file_start, file_end):
    x_start = None
    x_end = None

    x = -1
    reverse = blobs[::-1]
    for _b in reverse:
        if _b.name == file_start:
            x_start = x
        if _b.name == file_end:
            x_end = x

        x = x - 1

        if x_start != None and x_end != None:
            break

    if x_start == x_end:
        x_end = x_end - 1

    last = range(x_start, x_end, -1)

    return last


@app.route('/store/realtime/', methods=['GET'])
def store_realtime_get():
    last = request.args.get('last', 1)
    file_start = request.args.get('start', None)
    file_end = request.args.get('end', None)

    blobs = list(storage_client.list_blobs(bucket, prefix='realtime'))
    if file_start != None and file_end != None:
        print("Get from {} to {}.".format(file_start,file_end))
        last = range_start_end(blobs, file_start, file_end)

    else:
        last = last_range(last)

    _json = last_json(last,blobs)
    return json.dumps(_json)


@app.route('/store/list/realtime/', methods=['GET'])
def store_list_realtime_get():
    realtime_list = get_metric_list_from_bucket("realtime")
    return json.dumps(realtime_list)


@app.route('/store/realtime/', methods=['POST'])
def store_realtime():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    payload = ''
    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        payload = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()

    filename = "realtime-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    create_file(payload, filename)

    return ('', 204)


@app.route('/store/hourly/', methods=['GET'])
def store_hourly_get():
    file_start = request.args.get('start', None)
    file_end = request.args.get('end', None)
    last = request.args.get('last', 1)

    blobs = list(storage_client.list_blobs(bucket, prefix='hourly'))
    if file_start != None and file_end != None:
        print("Get from {} to {}.".format(file_start, file_end))
        last = range_start_end(blobs, file_start, file_end)
    else:
        last = last_range(last)


    _json = last_json(last, blobs)
    return json.dumps(_json)


@app.route('/store/list/hourly/', methods=['GET'])
def store_list_hourly_get():
    hourly_list = get_metric_list_from_bucket("hourly")
    return json.dumps(hourly_list)


@app.route('/store/hourly/', methods=['POST'])
def store_hourly():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    payload = ''
    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        payload = base64.b64decode(
            pubsub_message['data']).decode('utf-8').strip()

    filename = "hourly-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    create_file(payload, filename)

    return ('', 204)


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


@app.route("/site-map")
def site_map():
    links = []
    for rule in app.url_map.iter_rules():
        # Filter out rules we can't navigate to in a browser
        # and rules that require parameters
        if ("GET" in rule.methods or "POST" in rule.methods) and has_no_empty_params(rule):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            links.append((url, rule.endpoint))
    # links is now a list of url, endpoint tuples
    print(links)
    return ('', 200)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))