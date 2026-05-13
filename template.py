from venv import logger


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


@node.entrypoint
def entrypoint():
    # startup logic

    for bundle in consumer:
        # pre-run logic

        with node.stage_run():
            # run-start logic

            # use the entire bundle

            # post-run, pre-exit logic

        # cleanup logic


combined_consumer = Consumer(
    name="combined_consumer", 
    stream=CombinedStream(name="combined_folder", directory=transport_dest_path, logger=logger), 
    logger=logger
)
model_registry_producer = Producer(name="model_registry_producer", directory=model_registry_path, logger=logger)
isafe_transport = iSafeTransport(name="isafe_transport", directory=isafe_transport_path, logger=logger)

node2 = ModelRetrainingNode(name="ModelRetrainingNode", consumers=[combined_consumer], producers=[model_registry_producer], logger=logger)

@node2.entrypoint
def node2_func():
    # user code executed on startup of node (e.g., restart logic)

    logger.info(f"Node {node2.name} starting entrypoint function for run {node2.run_id}")
    model_registry_staging_path = model_registry_producer.get_staging_directory()
    model_registry_final_path = model_registry_producer.directory

    for bundle in combined_consumer:
        # user code executed before run starts 
        # (e.g., loading in metrics for comparison from previous runs, loading model from previous run, etc)
        # note: at this point, the run id hasn't incremented yet because the new run hasn't begun

        with node2.stage_run():
            # user code executed right after run starts, before unpacking bundle
            metrics = []
            
            subdir = model_registry_staging_path / f"model_dir_{node2.run_id}"
            os.makedirs(subdir, exist_ok=False)

            for i, item in enumerate(bundle):
                # user code executed for each item in the bundle (e.g., training logic)
                model_path = subdir / f"model_{node2.run_id}.txt"

                with open(model_path, "a") as file:
                    file.write(f"Model retrained with:\n")
                logger.info(f"ModelRetrainingNode writing: 'Model retrained with:' to {model_path} in run {node2.run_id}")

                time.sleep(1)   # checkpoint

                with open(model_path, "a") as file:
                    file.write(f"{item}\n")
                logger.info(f"ModelRetrainingNode writing: '{item}' to {model_path} in run {node2.run_id}")

                time.sleep(1)   # checkpoint

                node2.metrics.append({x: i, y: 10.0})    # simulate metrics generation during run

            # user code executed once per run after unpacking bundle (e.g., recording metrics into DB, creating model cards)

            # hash the model and commit it. user can choose which model gets committed here.
            # commit_artifact() allows a user to select which artifact to be moved to the final directory and register it in the DB
            # all uncommited artifacts will be discarded after the run
            final_model_path, model_hash = model_registry_producer.commit_artifact(artifact_path=model_path)

            # record metrics after model has been commited
            avg_metric = node2.compute_avg_metric(metrics=metrics)
            node2.log_model_metrics(model_hash=model_hash, avg_metric=avg_metric)   # permanently log metrics

            model_card_path = model_registry_staging_path / f"model_dir_{node2.run_id}" / f"model_card_{node2.run_id}.txt"
            with open(model_card_path, "w") as file:
                file.write(f"model card for {final_model_path}, hash: {model_hash}")

            final_model_card_path, model_card_hash = model_registry_producer.commit_artifact(artifact_path=model_card_path)

            # add artifacts to the transport's staging area to prepare for packaging
            isafe_transport.stage_artifact(artifact_path=final_model_card_path, artifact_hash=model_card_hash)
            isafe_transport.stage_artifact(artifact_path=final_model_path, artifact_hash=model_hash)

            # package the artifacts into one artifact to prepare to be sent via Intelink iSafe
            package_path, package_hash = isafe_transport.package()
            isafe_transport.send(package_path=package_path, package_hash=package_hash)

        # user code executed after run ends (e.g., cleanup)