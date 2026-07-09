## Tips on building your own plugins for Anacostia

### Stream
1. Inherit the base `Stream` class.
2. Implement the `__iter__()` method. Your implementation of the `__iter__()` method will be used to poll a resource for new artifacts. It will likely be similar to the following pseudocode:
```python
def __iter__(self) -> Iterator[Artifact]:
    while True:
        # discover new artifacts (see example in anacostia/streams/filesystem.py)
        current_artifacts_locations = { locations of artifacts in resource }
        registered_artifacts_locations = {locations for location in self.get_all_artifact_locations()}
        new_artifact_locations = current_artifacts_locations - registered_artifacts_locations

        # sort new artifacts by the order in which they appeared in the resource (oldest to newest)
        for artifact_location in sorted(new_artifact_locations):

            # hash the artifact using SHA256 (you will have to implement your own hashing function using hashlib.sha256() and then returning the hex digest)
            # note: if the artifact is large, you might want to load it in chunks
            sha256 = hashlib.sha256()
            sha256.update(artifact_content)
            artifact_hash = sha256.hexdigest()

            artifact = Artifact(location=artifact_location, hash=artifact_hash)
            self.register_artifact(artifact)    # register the artifact into the local database
            yield artifact                      # yield an Artifact object to the consumer
                
        # IMPORTANT: prevent polling from blocking main thread
        time.sleep(self.poll_interval)
```

## Tips for developing your own distributed pipeline and run tests:
1. Install `mkcert`
```
brew install mkcert
mkcert -install
```