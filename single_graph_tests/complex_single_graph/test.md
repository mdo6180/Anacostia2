### Test Objective:
- Show a complex pipeline with one node operating.
- Show the node can receive artifact bundles from two different consumers (each sonsumer polls a stream). 
- Show the streams can detect new artifacts arriving in the directory it is monitoring. 

### Pipeline Configuration:
Resources: two folders on the local filesystem `./testing_artifacts/incoming1` and `./testing_artifacts/incoming2`. 

Streams: 
- `odd_folder` stream monitors `./testing_artifacts/incoming1`, extracts the content of the files
- `even_folder` stream monitors `./testing_artifacts/incoming2`, extracts the content of the files

Consumers: 
- `stream_consumer_odd` takes in `odd_folder` stream, bundle size 2, filters out all artifacts execpt for the artifacts where its last character is an odd number.
- `stream_consumer_even` takes in `even_folder` stream, bundle size 2, filters out all artifacts execpt for the artifacts where its last character is an even number.

Nodes:
- `TestNode` pulls artifact bundles from both consumers, writes the odd file contents to a file `processed_odd_<run_id>.txt`, writes the even file contents to a file `processed_evn_<run_id>.txt`, and then writes the contents of both even and odd files to a file `processed_combined_<run_id>.txt`. It then uses the following producers to record these produced artifacts into the pipeline database. It then uses `combined_transport` to move the `processed_combined_<run_id>.txt` files from one folder on the filesystem to another. 

Producers:
- `odd_producer` records `processed_odd_<run_id>.txt` files into the DB. 
- `even_producer` records `processed_even_<run_id>.txt` files into the DB. 
- `combined_producer` records `processed_combined_<run_id>.txt` files into the DB. 

Transports:
- `combined_transport` moves `processed_combined_<run_id>.txt` to `./testing_artifacts/transport_receiver` folder.

### Test setup:
Run `python mid_stream_stop.py`.

### Pipeline trigger:
Files will be created and dumped into the `./testing_artifacts/incoming1` and `./testing_artifacts/incoming2` folders. Consumers will filter out all artifacts, upon seeing two artifacts that pass the filter consumer will bundle those two artifacts up and hand it to the node. Node will trigger once it receives a bundle from each consumer. 

### Instructions to run test:
Open up another terminal and run `python trigger_pipeline.py`