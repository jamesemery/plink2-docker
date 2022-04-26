# plink2-docker
A dockerfile for plink2 alpha releases

### To build

```
 docker build . --tag us.gcr.io/broad-dsde-methods/plink2-alpha
```


### To Push
```
  # one time only
  gcloud auth configure-docker

  docker push us.gcr.io/broad-dsde-methods/plink2-alpha
```
