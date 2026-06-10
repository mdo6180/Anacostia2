### Test Objective:
- Show the simplest pipeline possible.
- Show the node can receive artifact bundles from one consumer. 
- Show the stream can detect new artifacts arriving in the directory it is monitoring. 

### Pipeline Configuration:
Resources: a folder on the local filesystem `./testing_artifacts/incoming1`. 

Streams: 
- `odd_folder` stream monitors `./testing_artifacts/incoming1`, extracts the content of the files

Consumers: 
- `stream_consumer_odd` takes in `odd_folder` stream, bundle size 1.

Nodes:
- `TestNode` pulls artifact bundles from `stream_consumer_odd`, and prints out the bundle (the content of one file).

Producers: None

Transports: None

### Test setup:
Run `python mid_stream_stop.py`.

### Pipeline trigger:
Files will be created and dumped into the `./testing_artifacts/incoming1` folder. Consumer will bundle the artifact and hand it to the node. Node will trigger once it receives a bundle from the consumer. 

### Instructions to run test:
Open up another terminal and run `python trigger_pipeline.py`