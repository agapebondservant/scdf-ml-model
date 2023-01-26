# ML Model Processor for Spring Cloud Data Flow

SCDF processor abstraction for executing a Python step in a machine learning pipeline.
<br/><br/>
<a href="https://youtu.be/51VT-d5lYaA" target="_blank">Link to recorded demo</a>
<br/><br/>

## Disclaimer
**NOTE**: This is NOT production-ready code. It is only implemented as a prototype.

<img src="End-to-end Machine and Deep Learning with MLFlow and Spring.jpg"
     alt="Markdown Monster icon"
     style="float: left; margin-right: 10px;" />

## Current Limitations
* Currently supports Python 3.
* (Other - to be documented)

## Parameters

_**model_entry**_: _(Required)_

Name of the MLProject **entry point** which will be invoked on load, ex. _**app.main**_

_**git_sync_repo**_: _(Required)_

git clone address (https) of the git repository hosting the MLFlow MLProject.

_**monitor_app**_: _(Optional, default: false)_

Whether this is a monitoring application for an **mlmodel**.

_**monitor_sliding_window_size**_: _(Optional, int)_ 

The size of the sliding window to use for monitoring.

_**monitor_schema_path**_: _(Optional, default: data/schema.csv)_

The location of the CSV schema file to use for data drift detection.

## How It Works
**mlmodel** runs a <a target="_blank" href="https://dataflow.spring.io/docs/recipes/polyglot/processor/">Python processor application in a Spring Cloud Data Flow streaming pipeline</a>
as an **MLFlow** <a target="_blank" href="https://mlflow.org/docs/2.0.1/projects.html">MLProject</a>. Using environment variables injected by Spring Cloud Data Flow,
(either out-of-the-box or by injecting a ConfigMap as a pipeline property), it prepares the following ports:

* **Data ports**, which are connectors for integrating with services on ETL, training & tuning clusters
  * These ports are dynamically injected and auto-discoverable. To use, a 
    <a target="_blank" href="https://docs.spring.io/spring-cloud-dataflow/docs/current/reference/htmlsingle/#_configmap_references"><b>ConfigMap</b></a>
    is mounted to the SCDF pipeline with environment variables 
    whose names follow a given convention, allowing the ports to be accessible for reading and writing via 
    <a target="_blank" href="https://en.wikipedia.org/wiki/Convention_over_configuration"><b>Convention over configuration</b></a>. This convention will 
    support almost any kind of port (the current version demonstrates RabbitMQ Messaging and Streaming ports.)
    <br/>
    For example: <br/>
    Use **ports.get_rabbitmq_streams_port('abc', flow_type=FlowType.INBOUND, _YOUR_KWARG_OVERRIDES_)**  <br/>
    to initialize an inbound connection to a <a target="_blank" href="https://www.rabbitmq.com/streams.html"><b>RabbitMQ Streams</b></a> broker,
    using environment variables **ABC_SCDF_ML_MODEL_RABBITMQ_HOST**, **ABC_SCDF_ML_MODEL_RABBITMQ_PORT**, 
    **ABC_SCDF_ML_MODEL_RABBITMQ_USERNAME** etc as initialization parameters.
    <br/><br/>
    
* **Control port** for driving the main flow, which is to execute the **mlmodel** application as an MLFlow **run**
  * This port is automatically instantiated to handle the flow of parameters between the current step 
    (a <a target="_blank" href="https://dataflow.spring.io/docs/concepts/architecture/"><b>processor</b></a>), 
    the prior <a target="_blank" href="https://dataflow.spring.io/docs/concepts/architecture/"><b>processor</b></a> or
    <a target="_blank" href="https://dataflow.spring.io/docs/concepts/architecture/"><b>sink</b></a>, 
    and the following <a target="_blank" href="https://dataflow.spring.io/docs/concepts/architecture/"><b>processor</b></a>
    or <a target="_blank" href="https://dataflow.spring.io/docs/concepts/architecture/"><b>source</b></a>. It sets up an 
    <a target="_blank" href="https://mlflow.org/docs/latest/python_api/mlflow.entities.html?highlight=run#mlflow.entities.Run"><b>MlFlow run</b></a>
    for this step with various parameters. It also enables integration with endpoints exposed via environment variables injected 
    into the <a target="_blank" href="https://docs.spring.io/spring-cloud-dataflow/docs/current/reference/htmlsingle/#_configmap_references"><b>ConfigMap</b></a> above, ex. 
    **RAY_ADDRESS** for <a href="https://www.ray.io/" target="_blank">Ray</a>, or **MLFLOW_TRACKING_URI** for <a href="https://mlflow.org" target="_blank">MlFlow</a>.
    <br/><br/>
  
* **Monitoring port** for exporting ML, data and resource metrics to <a target="_blank" href="https://dataflow.spring.io/docs/feature-guides/streams/monitoring/">SCDF's integrated Prometheus</a>
  * This port is automatically instantiated to handle scraping a Prometheus endpoint,**/actuator/prometheus**, which is exposed by default by the processor. 
    A <a target="_blank" href="https://github.com/agapebondservant/ml-metrics-accelerator">helper library</a> (beta) was developed which supports exporting metrics to **/actuator/prometheus**.
    <br/><br/>

In order to inject the ports into the MLProject code automatically, Python functions must be annotated with the **@scdf_adapter** decorator.
This enables access to in-built adapters for accessing the ports, injects parameters from the SCDF pipeline into the MLProject, 
and also automatically enables the control port.


## Other
### (Coming soon: Full instructions for testing the ML Model Processor in a full-fledged pipeline. The guidance below provides a partial subset of instructions.)

* To build and run ML Model Processor docker image locally:
```
docker build -t scdf-ml-model:0.0.1 .
docker push scdf-ml-model:0.0.1 .
docker run -v $(pwd)/app:/parent/app scdf-ml-model:0.0.1
```

* To register custom ML processor:
```
java -jar spring-cloud-dataflow-shell-2.9.2.jar
dataflow config server --uri http://scdf.tanzudatatap.ml
app register --type processor --name ml-model --uri docker://<your-docker-registry>/scdf-ml-model:1.0.7
```

* To unregister previously deployed processor:
```
app unregister --type processor --name ml-model
```

* To deploy a streaming pipeline:
```
stream create --name MyPipeline --definition '<your source> | <your processor> | <your processor> | ... | <your sink>'
```
For example:
```
stream create --name anomaly-detection-training --definition 'http | extract-features: ml-model model_entry=app.main git_sync_repo=https://github.com/<your-repo>/sample-ml-step.git|  log'
```

* To deploy a streaming pipeline:
```
stream deploy --name MyPipeline --properties 'deployer.ml-model.kubernetes.enviroment-variables=ENV1=VAL1,ENV2=VAL2,...'
```

For example:
```
stream deploy --name anomaly-detection-training --properties 'deployer.extract-features.kubernetes.enviroment-variables=GIT_SYNC_REPO=https://github.com/agapebondservant/sample-ml-step.git,MODEL_ENTRY=app.main,deployer.build-arima-model.kubernetes.enviroment-variables=GIT_SYNC_REPO=https://github.com/agapebondservant/sample-ml-step.git,MODEL_ENTRY=app.main'
```

* To test the example:
    * Deploy the configmap dependency:
```
cd </path/to/sample/ml/project> (from https://github.com/agapebondservant/sample-ml-step)
kubectl delete configmap test-ml-model || true
kubectl create configmap test-ml-model --from-env-file=.env
cd -
kubectl rollout restart deployment/skipper
kubectl rollout restart deployment/scdf-server
```

  * Prepare the environment for the pipelines:
```
scripts/prepare-pipeline-environment.sh
```

  * In GitHub, create Secrets under Settings -> Secrets -> Actions:
    * Create a Secret named CREATE_PIPELINE_CMD with the content of scripts/commands/create-scdf-ml-pipeline.txt
    * Create a Secret named UPDATE_PIPELINE_CMD with the content of scripts/commands/update-scdf-ml-pipeline.txt
    * Create a Secret named SCDF_ML_KUBECONFIG with the cluster kubeconfig
    * Create a Secret named DEMO_REGISTRY_USERNAME with the container registry username
    * Create a Secret named DEMO_REGISTRY_PASSWORD with the container registry token/password
    * Create a Secret named PRODUCER_SCDF_ML_MODEL_RABBITMQ_VIRTUAL_HOST with the virtual host to use for the pipelines (/ for default)
