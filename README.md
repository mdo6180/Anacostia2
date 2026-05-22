# Anacostia
Welcome to Anacostia. Anacostia is a framework for creating machine learning operations (MLOps) pipelines. I believe the process of creating MLOps pipelines today are too difficult; thus, this is my attempt at simplifying the entire process, especially for people who work in classified environments and people who build cross-domain pipelines that span classification boundaries. 

## Notes for contributors and developers
If you are interested in contributing to Anacostia, please see CONTRIBUTORS.md. 
If you are interested in building your own plugins for Anacostia and contributing to the Anacostia ecosystem, please see DEVELOPERS.md. 

## Basic Anacostia Concepts & Terminology:
Basic terminology:
- Resource: A *resource* is the underlying data source or system being observed—such as a directory, message queue, database, or API from which data originates.
- Artifact: An *artifact* is a discrete unit of data produced or consumed by the system—such as a file, message, or dataset—that flows through the pipeline.
- A *pipeline* is a directed sequence of nodes that process and transform artifacts as they flow from upstream resources to downstream outputs, defining the end-to-end execution of a workflow. In Anacostia, pipelines are represented as directed acyclic graphs (DAGs), where each node is a component and each edge represents the flow of artifacts between components. Pipelines may span multiple environments—including fully air-gapped systems—with each environment executing a partition of the overall graph. Artifacts move between environments via file-based transfers, ranging from manual handoffs (e.g., removable media) in air-gapped settings to network-based protocols such as HTTPS or SFTP.
- An *environment* is an isolated compute domain—such as a machine, network, or security boundary (including air-gapped systems)—that executes a partition of a pipeline and governs how artifacts are accessed, processed, and transferred.
- Runs: a *run* is a single execution instance of a Node, triggered when it receives a complete batch of artifacts from all of its Consumers. During a run, the Node processes the input bundle, executes its logic, and produces output artifacts before completing.


An Anacostia pipeline is defined as a directed acyclic graph (DAG). There are five components to an Anacostia pipeline: streams, consumers, nodes, producers, and transports.
- **Streams**: a Stream is an iterator that continuously monitors a resource for new artifacts via polling. Streams detect artifacts as they are emitted from a resource, then loads the artifact into memory, hash it, and validate it to ensure consistent provenance, traceability, and auditability across the pipeline.
- **Consumers**: a Consumer is an iterator responsible for pulling artifacts from a stream, filtering the artifacts, batching those artifacts into a bundle, and then yielding the bundle to the node.
- **Nodes**: a Node consumes bundles of artifacts via its Consumers, executes user-defined logic to process or transform them, and produces outputs that flow downstream to other components in the pipeline. A Node executes only when it receives a complete batch of artifacts from all of its Consumers.
- **Producers**: a Producer is responsible for creating and emitting new artifacts as outputs of a Node’s run. It manages the staging, registration, and finalization of these artifacts before making them available to downstream components in the pipeline.
- **Transports**: a Transport is responsible for packaging and transferring artifacts between environments or pipeline partitions. It stages artifacts, bundles associated data and metadata, and moves them to a destination where they can be ingested and processed by downstream components.

Basic code structure:
1. **Configure the components of the pipeline.** In the following code snippet, we see two DirectoryStreams used to monitor two different folders for incoming files. Those two streams are then fed into two different Consumers and each Consumer object uses a different filter function to filter out unwanted artifacts. There are also three Producers being defined to register three different types of output artifacts. We then define one transport to move artifacts to another folder in the filesystem; and then lastly, we register all of these components with the Node object.
```python
def filter_odd(content: str) -> bool:
    return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

def filter_even(content: str) -> bool:
    return int(content[-1]) % 2 == 0    # Keep only artifacts with last character as even number

stream_consumer_odd = Consumer(
    name="stream_consumer_odd", 
    stream=DirectoryStream(name="odd_folder", directory="/path/to/odd", logger=logger), 
    bundle_size=2, filter_func=filter_odd, logger=logger
)
stream_consumer_even = Consumer(
    name="stream_consumer_even", 
    stream=DirectoryStream(name="even_folder", directory="/path/to/even", logger=logger), 
    bundle_size=2, filter_func=filter_even, logger=logger
)
odd_producer = Producer(name="odd_producer", directory=output_path1, logger=logger)   # example producer
even_producer = Producer(name="even_producer", directory=output_path2, logger=logger)   # example producer
combined_producer = Producer(name="combined_producer", directory=output_combined_path, logger=logger)   # example producer

combined_transport = FileSystemTransport(name="combined_transport", packages_directory=transport_package_dir, logger=logger)

node = Node(
    name="TestNode", 
    consumers=[stream_consumer_odd, stream_consumer_even], 
    producers=[odd_producer, even_producer, combined_producer], 
    transports=[combined_transport],
    logger=logger
)
```

2. **Implement the entrypoint function.** Once the Node object is created and all components are registered in the Node object, we can use the *entrypoint* decorator to define the work we want the node to execute. Depending on how we want to use the bundle, we might opt for an entry point function with a structure similar to the code in *Snippet 1* if we want to extract artifacts out of the bundle, or *Snippet 2* if we want to just use the whole bundle. If we have multiple Consumers, we could use the *zip* function to wait for bundles from both Consumers before executing like in *Snippet 3*.

Snippet 1
```python
@node.entrypoint
def entrypoint():
    # startup logic

    for bundle in consumer:
        # pre-run logic

        with node.stage_run():
            # run-start logic

            for item in bundle:
                # extract items from the bundle
                # per-item logic

            # post-run, pre-exit logic

        # cleanup logic
```

Snippet 2:
```python
@node.entrypoint
def entrypoint():
    # startup logic

    for bundle in consumer:
        # pre-run logic

        with node.stage_run():
            # run-start logic

            use_bundle(bundle)    # use the entire bundle

            # post-run, pre-exit logic

        # cleanup logic
```

Snippet 3:
```python
@node.entrypoint
def entrypoint():
    # startup logic

    for bundle1, bundle2 in zip(consumer1, consumer2):
        # pre-run logic

        with node.stage_run():
            # run-start logic

            for item1 in bundle1:
                # per item logic
            
            use_bundle2(bundle2)    # use the entire bundle

            # post-run, pre-exit logic

        # cleanup logic
```
