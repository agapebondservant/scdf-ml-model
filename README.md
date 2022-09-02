# ML Model Processor for Spring Cloud Data Flow
* Build and run ML Model Processor docker image:
```
docker build -t ml-model:0.0.1 .
docker run -v $(pwd)/app:/parent/app ml-model:0.0.1
```