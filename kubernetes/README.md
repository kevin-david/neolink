### Neolink in Kubernetes

#### Introduction

This README provides guidance on deploying the NeoLink application on Kubernetes using the provided manifest files. The deployment is designed to set up a NeoLink application within its own namespace, with a specific configuration for node affinity, resource limits, and service exposure.

#### Prerequisites

- Kubernetes Cluster
- Command-line tool (kubectl)
- Basic understanding of Kubernetes concepts (Pods, Services, Deployments, ConfigMaps)

#### Deployment Overview

The deployment consists of four main parts:

1. Namespace Creation: A new namespace neolink is created to isolate the resources.

2. Deployment Configuration: The neolink-app-deployment is set up with:
  - A single replica.
  - Affinity to arm64 nodes (Though this is optional).
  - Resource limits and requests for CPU and memory.
  - A custom command and args to use a specific configuration file.

3. Service Exposure: A LoadBalancer type service named neolink exposes the application on port 8554.

4. ConfigMap: Contains the application configuration neolink.toml.

#### Steps to Deploy

```bash
kubectl apply -f manifest.yaml
```

### Configuration Details

- The neolink.toml file in the ConfigMap provides the application configuration. Customize it as per your requirements.
- The deployment is set to favor arm64 architecture nodes via a `nodeAffinity`. This is a personal preference as in my cluster all my raspberrpis (except one) run 32bit raspian OS. And to try and get more bang for my buck I was to run neolink on my arm64 8Gb node. Again, this is totaly optional for you.
- Resource limits are set to ensure efficient use of cluster resources.
- The LoadBalancer service will make the application accessible externally.

#### Troubleshooting

- If the pod fails to start, check the pod logs and describe the pod for more details.
- Ensure that the arm64 node is available and schedulable in your cluster (if you're using nodeAffinity).
- Verify that the ConfigMap is correctly applied and mounted.

#### Conclusion

This guide provides a step-by-step approach to deploying the NeoLink application on a Kubernetes cluster. Modify the manifests as necessary to fit your specific requirements and environment.
