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

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "climacell_data"
bucket = storage_client.bucket('climacell_data')


project_id = os.environ['PROJECT_ID']
topic_id = os.environ['TOPIC_ID']

app = Flask(__name__)
app.config["DEBUG"] = True


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

@app.route('/realtime/', methods=['GET'])
def realtime():

    response_realtime = requests.get(climacell_url_realtime,
                                params=querystring,
                                headers=request_headers)

    print(response_realtime.status_code)
    # print(json.dumps(response_realtime.json(), indent=4, sort_keys=True))

    pubsub(json.dumps(response_realtime.json()),'realtime')

    return json.dumps(response_realtime.json(), indent=4, sort_keys=True)


@app.route('/hourly/', methods=['GET'])
def hourly():

    response_hourly = requests.get(climacell_url_hourly,
                                   params=querystring_hourly,
                                   headers=request_headers)

    print(response_hourly.status_code)
    # print(json.dumps(response_hourly.json(), indent=4, sort_keys=True))

    pubsub(json.dumps(response_hourly.json()),'hourly')

    return json.dumps(response_hourly.json(), indent=4, sort_keys=True)


@app.route('/store/realtime/', methods=['GET'])
def store_realtime_get():
    blobs = list(storage_client.list_blobs(bucket, prefix='realtime'))
    print ("GET latest climacell realtime : "+str(blobs[-1]))
    json = blobs[-1].download_as_string()
    return json


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
    blobs = list(storage_client.list_blobs(bucket, prefix='hourly'))
    print("GET latest climacell hourly : " + str(blobs[-1]))
    json = blobs[-1].download_as_string()
    return json


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