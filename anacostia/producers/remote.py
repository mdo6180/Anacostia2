import json
from typing import Any

from anacostia.producers.base import Producer
from anacostia.utils.types import Artifact, JsonDict



class RemoteProducer(Producer):
    def upload_artifact(self, artifact: Artifact, metadata: JsonDict = None, content: Any = None):
        """
        Upload an artifact to a remote server.
        Implement the logic to upload the artifact to a remote server.
        This could involve making an HTTP request, using an SDK, etc.
        
        Recommendation: call the `register_artifact` method after a successful upload to register the artifact in the local and global database tables.

        :param artifact: The artifact to upload.
        :param metadata: Optional metadata associated with the artifact.
        :param content: Optional content associated with the artifact.
        
        **Usage**:
        ```
        class MyRemoteProducer(RemoteProducer):
            def upload_artifact(self, artifact: Artifact, metadata: JsonDict = None, content: Any = None):
                
                # Upload the file_data to the remote server along with metadata
                with open(artifact.location['path'], 'rb') as f:
                    file_data = f.read()
                    response = requests.post(remote_url, files={'file': file_data}, data=metadata)
                
                # Register the artifact in the local and global database tables
                self.register_artifact(artifact, metadata, content)
        ```
        """
        raise NotImplementedError("The upload_artifact method must be implemented in a subclass of RemoteProducer.")