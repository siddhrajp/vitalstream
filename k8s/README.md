# Kubernetes (local, Docker Desktop)

Everything runs in the `vitalstream` namespace: Postgres and Kafka (StatefulSets with persistent volumes),
Schema Registry, Keycloak, and the three services. Development-grade: single instances, dev credentials.

Prerequisites: Docker Desktop with Kubernetes enabled (`kubectl config use-context docker-desktop`), and the
docker-compose stack stopped (`docker compose stop`), since both use the same localhost ports and memory.

## Deploy

    k8s/deploy.sh --build     # build the three images, then apply everything
    k8s/deploy.sh             # apply only (images already built)
    kubectl get pods -n vitalstream -w

From your Mac: REST `localhost:8080`, gRPC `localhost:9090`, query API `localhost:8082`,
Keycloak `localhost:8180`. The simulator works unchanged: `simulator/.venv/bin/python sim.py --create 3`.

## Change a service

    docker build --build-arg SERVICE=api-service -t vitalstream/api-service:dev .
    kubectl rollout restart deployment/api-service -n vitalstream   # rolling update: new pod first, then old one removed
    kubectl rollout status deployment/api-service -n vitalstream

## Look around

    kubectl get all -n vitalstream
    kubectl logs -n vitalstream deploy/api-service -f
    kubectl describe pod -n vitalstream <pod>          # events: probe failures, OOMKilled, image problems
    kubectl exec -n vitalstream -it postgres-0 -- psql -U vitalstream
    kubectl top pods -n vitalstream                    # needs metrics-server; otherwise: docker stats

## Tear down

    kubectl delete namespace vitalstream     # everything, including the Postgres and Kafka volumes
    docker compose start                     # back to the compose stack (its data is untouched)
