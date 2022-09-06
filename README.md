# ML Model Processor for Spring Cloud Data Flow

SCDF processor abstraction for executing a step in a machine learning pipeline.
Currently supports Python 3.

## Parameters

_**MODEL_ENTRY**_:

Fully qualified Python module which invokes the ML model on load, ex. _**app.main**_

_**GIT_SYNC_REPO**_:

clone address (https) of the git repository for the Python module which invokes the ML model described above

## Other

* To build and run ML Model Processor docker image:
```
docker build -t ml-model:0.0.1 .
docker run -v $(pwd)/app:/parent/app ml-model:0.0.1
```

* To register custom processor:
```
java -jar spring-cloud-dataflow-shell-2.9.2.jar
dataflow config server --uri http://scdf.tanzudatatap.ml
app register --type processor --name ml-model --uri docker://oawofolu/scdf-ml-model:1.0.7
```

* To unregister previously deployed processor:
```
app unregister --type processor --name ml-model
```

* To deploy a streaming pipeline:
```
stream create --name MyPipeline --definition '<your source> | <your processor> | <your processor> | ... | <your sink>'
```
Sample:
```
stream create --name anomaly_detection_training --definition 'http | extract-features: ml-model | build-arima-model: ml-model | log'
```

* To deploy a streaming pipeline:
```
stream deploy --name MyPipeline --properties 'deployer.ml-model.kubernetes.enviromentVariables=ENV1=VAL1,ENV2=VAL2,...'
```

Sample:
```
stream deploy --name anomaly_detection_training --properties 'deployer.extract-features.kubernetes.enviromentVariables=GIT_SYNC_REPO=github.com/agapebondservant/sample-ml-step.git,MODEL_ENTRY=app.main,deployer.build-arima-model.kubernetes.enviromentVariables=GIT_SYNC_REPO=github.com/agapebondservant/sample-ml-step.git,MODEL_ENTRY=app.main'
```