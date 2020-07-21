[![kubebuilder-tutorial-badge]][kubebuilder-tutorial-workflow]

[kubebuilder-tutorial-badge]: https://github.com/jace-ys/kubebuilder-tutorial/workflows/kubebuilder-tutorial/badge.svg
[kubebuilder-tutorial-workflow]: https://github.com/jace-ys/kubebuilder-tutorial/actions?query=workflow%kubebuilder-tutorial

# Kubebuilder tutorial

Building the Kubernetes [CronJob operator](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/), based on the [Kubebuilder tutorial](https://book.kubebuilder.io/cronjob-tutorial/cronjob-tutorial.html).

## Usage

1. Start a kind cluster and install our vendored operators:

```
make cluster
```

2. Install our CRDs into the cluster:

```
make install
```

3. Build the Docker image for our controller and load it into the cluster:

```
make docker-build IMG=kubebuilder-tutorial:v1.0.0
kind load docker-image kubebuilder-tutorial:v1.0.0
```

4. Deploy our controller into the cluster:

```
make deploy IMG=kubebuilder-tutorial:v1.0.0
```

5. Create a sample CronJob:

```
kubectl apply -f config/samples
```

6. View our sample CronJob and scheduled Jobs:

```
kubectl get cronjob.batch.tutorial.kubebuilder.io cronjob-sample
kubectl get jobs | grep cronjob-sample-
```
