### Import Pipeline using Dataflow.

This Import piece is part of the PoC focused on handling the schema mutation in Gaming Industry. This is a sample dataflow streaming pipeline that reads user events from GCS buckets and publishes to PubSub with the attribute as the Event Type. For example: event_type=game_crash.

### Before Run

1. Create a PubSub Topic
2. Create a GCS bucket and upload your .json event files. The file name format currently accepted is: <enter_your_event_name>_event.json. The file can have single/multiple json messages.
All the files in the aforementioned file format will be picked up and the code will poll for newer files.


### How to Create & Execute pipeline using dataflow template?

Please update the arguments as required.

To create a DataFlow template:

```
gradle run -DmainClass=com.google.swarm.importpipeline.GCSImportPipeline -Pargs=" --streaming --project=<project_id> --runner=DataflowRunner --tempLocation=gs://df-temp-import/temp --templateLocation=gs://<bucket>/template --inputFile=gs://game-event-data/*_event.json --topic=projects/<project_id>/topics/<id> --numWorkers=1 --workerMachineType=n1-standard-4 --maxNumWorkers=2 --autoscalingAlgorithm=THROUGHPUT_BASED"

```
To execute:

```
gcloud dataflow jobs run import-pipeline --gcs-location gs://<bucket>/template

```


### Local Build & Run

To run using direct runner

```
gradle run -DmainClass=com.google.swarm.importpipeline.GCSImportPipeline -Pargs=" --streaming --project=<project_id> --runner=DirectRunner --tempLocation=gs://df-temp-import/temp --inputFile=gs://game-event-data/*_event.json --topic=projects/<project_id>/topics/<id>"

```

Sample JSON file in GCS bucket - game_crash_event.json

```

{ "TitleId": "prod",  "EventName": "game_crash",  "EventId": "04da62c4-80c4-40d4-8e9d-ad4eb0b7a065",  "Timestamp": "2019-02-11T18:25:50Z",  "ServerTimestamp": "2019-02-11T18:25:52Z",  "CrashGuid": "UE4CC-Windows-820EF2F0482B0B67045CE98FA7686D2C_0000",  "GameChangelist": "113030",  "ErrorMessage": ""}
{ "TitleId": "prod",  "EventName": "game_crash",  "EventId": "04da62c4-80c4-40d4-8e9d-ad4eb0b7a066",  "Timestamp": "2019-02-12T18:25:50Z",  "ServerTimestamp": "2019-02-12T18:25:52Z",  "CrashGuid": "UE4CC-Windows-820EF2F0482B0B67045CE98FA7686D2C_0000",  "GameChangelist": "113030",  "ErrorMessage": ""}
{ "TitleId": "prod",  "EventName": "game_crash",  "EventId": "04da62c4-80c4-40d4-8e9d-ad4eb0b7a067",  "Timestamp": "2019-02-13T18:25:50Z",  "ServerTimestamp": "2019-02-13T18:25:52Z",  "CrashGuid": "UE4CC-Windows-820EF2F0482B0B67045CE98FA7686D2C_0000",  "GameChangelist": "113030",  "ErrorMessage": ""}

```

Screenshot in dataflow/ Beam 2.10

![dataflow_dag](import_pipeline_screen_shot.jpeg)



``` 

