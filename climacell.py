import os
from flask import Flask
import requests
import json
from google.cloud import pubsub_v1
import time
from google.cloud import secretmanager


project_id = os.environ['PROJECT_ID']
topic_id = os.environ['TOPIC_ID']

app = Flask(__name__)
app.config["DEBUG"] = True


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

print(request_headers)

climacell_url_realtime = "https://api.climacell.co/v3/weather/realtime"
climacell_url_hourly = "https://api.climacell.co/v3/weather/forecast/hourly"


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
    payload = response.payload.data.decode("UTF-8")
    return payload

def pubsub(json_payload):
    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                print(f.result())
                futures.pop(data)
            except:  # noqa
                print("Please handle {}.".format(f.exception()))

        return callback

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    #futures.update({data: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, json_payload.encode("utf-8"))
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

    pubsub(json.dumps(response_realtime.json(), indent=4, sort_keys=True))

    return json.dumps(response_realtime.json(), indent=4, sort_keys=True)


@app.route('/hourly/', methods=['GET'])
def hourly():

    response_hourly = requests.get(climacell_url_hourly,
                                   params=querystring_hourly,
                                   headers=request_headers)

    print(response_hourly.status_code)
    # print(json.dumps(response_hourly.json(), indent=4, sort_keys=True))

    pubsub(json.dumps(response_hourly.json(), indent=4, sort_keys=True))

    return json.dumps(response_hourly.json(), indent=4, sort_keys=True)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))