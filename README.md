## GCP resources

### Create storage bucket
```
gsutil mb -p $env:PROJECT_ID -c STANDARD -l $env:GCP_LOCATION -b on gs://climacell_data
gsutil label ch -l project:thermostat gs://climacell_data
```

### PubSub subscriptions to store weather data in storage bucket
```
gcloud beta pubsub subscriptions create $env:SUBSCRIPTION_CLIMACELL_STORE_REALTIME2 --topic $env:TOPIC_ID --push-endpoint=$endpoint --push-auth-service-account=$env:CLIMACELL_AGENT_SERVICE_ACCOUNT --message-filter='attribute.mode:realtime' --enable-message-ordering

gcloud beta pubsub subscriptions create $env:SUBSCRIPTION_CLIMACELL_STORE_HOURLY --topic $env:TOPIC_ID --push-endpoint=$endpoint_hourly --push-auth-service-account=$env:CLIMACELL_AGENT_SERVICE_ACCOUNT --message-filter='attributes.mode = \"hourly\"' --enable-message-ordering
```