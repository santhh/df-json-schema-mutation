## To run locust on a GKE cluster in a distributed mode:

```
Edit docker-image/locust-tasks/tasks.py and provide
1- PubSub TOPIC_NAME
2- GCP PROJECT_ID
3- Adjust MUTATION_PERCENTAGE (between 0 and 1)
4- Name of your EVENT_TYPE eg: game_crash
```

#### 1- Create the GKE cluster with enough resources
#### 2- Connect to the GKE cluster with the provided command in the Web Console
#### 3- Export your project name to the ENV: PROJECT_ID=your-project-id
#### 3- Navigate to ./docker-image/ and build your Docker image with the below commands

```bash
    docker build -t locust-tasks .
    docker tag locust-tasks gcr.io/$PROJECT_ID/locust-tasks:latest
```
#### 4- Push the created image to GCP cloud container register:

```bash
    docker push gcr.io/$PROJECT_ID/locust-tasks:latest
```

#### 5- Build the Locust Pods:

```bash
    kubectl create -f ../kubernetes-config/locust-master-controller.yaml
    kubectl create -f ../kubernetes-config/locust-master-service.yaml
    kubectl create -f ../kubernetes-config/locust-worker-controller.yaml
```

#### To view the deployment:

```bash
    kubectl get deployment 
```

#### To delete the deployment:

```bash
    kubectl delete deployment locust-master
    kubectl delete deployment locust-worker
```

#### To view the service (Locust Master URL):

```bash
    kubectl get svc
```

#### To delete the service:

```bash
    kubectl delete svc locust-master
```

#### To scale the worker nodes to increase the Locust load:

```bash
    kubectl scale --replicas=500 deployment locust-worker
```
