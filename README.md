# kube-lakehouse

# create env
conda create --name kube-lakehouse python=3.13

# install uv if needed
pip install uv

# install main dependencies
uv sync

# install optional env specific dependencies
uv sync --dev

# Export production dependencies to requirements.txt
uv export --format requirements-txt --no-dev > requirements.txt

# Export all dependencies including dev
uv export --format requirements-txt > requirements-dev.txt

# Init pulumi project
pulumi new --dir infra

# Check kube access
kubectl cluster-info
kubectl get nodes

# List pods
kubectl get pods -n k8lh

# View logs
kubectl logs minio-k8lh-dev-0 -n k8lh

# Shell into a pod
kubectl exec -it minio-k8lh-dev-0 -n k8lh -- /bin/sh

# Describe pod (troubleshooting)
kubectl describe pod minio-k8lh-dev-0 -n k8lh

# Delete pod (controller recreates it)
kubectl delete pod minio-k8lh-dev-0 -n k8lh

# Set secrets
pulumi config set --secret minio_root_password "minioadmin"
pulumi config get minio_root_password
pulumi config --show-secrets

# Preview
pulumi stack select dev
pulumi preview
or
pulumi preview --stack dev

# Deploy
pulumi up