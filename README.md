# k8lh

# create env
conda create --name k8lh python=3.13

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

# Remove dangling ns
python -c "namespace='ns-k8lh-dev';import atexit,subprocess,json,requests,sys;proxy_process = subprocess.Popen(['kubectl', 'proxy']);atexit.register(proxy_process.kill);p = subprocess.Popen(['kubectl', 'get', 'namespace', namespace, '-o', 'json'], stdout=subprocess.PIPE);p.wait();data = json.load(p.stdout);data['spec']['finalizers'] = [];requests.put('http://127.0.0.1:8001/api/v1/namespaces/{}/finalize'.format(namespace), json=data).raise_for_status()"


# Hosts
C:\Windows\System32\drivers\etc\hosts

127.0.0.1 minio.k8lh.local
127.0.0.1 minio-console.k8lh.local
127.0.0.1 polaris.k8lh.local
127.0.0.1 kafka-ui.k8lh.local
127.0.0.1 trino.k8lh.local (connect via http://trino.k8lh.local:80)
127.0.0.1 spark.k8lh.local
127.0.0.1 airflow.k8lh.local
127.0.0.1 flink-btc.k8lh.local
127.0.0.1 flink-eth.k8lh.local
127.0.0.1 flink-transactions.k8lh.local
192.168.1.9 host.docker.internal
192.168.1.9 gateway.docker.internal
127.0.0.1 kubernetes.docker.internal


# Start docker local registry
docker run -d -p 5000:5000 --name registry registry:2
docker start registry

# Pulumi commands from
cd infra

# Check backend
pulumi whoami -v

# For local dev
pulumi login --local

# For azure
pulumi login azblob://pulumi-container

# See your state
pulumi stack export > state.json

# Set secrets
pulumi config set minio_root_user "minioadmin"
pulumi config set --secret minio_root_password "minioadmin"

pulumi config set postgres_admin_user "postgres"
pulumi config set --secret postgres_admin_password "postgresql"

pulumi config set polaris_postgres_user "polaris"
pulumi config set --secret polaris_postgres_password "polaris"

pulumi config set airflow_postgres_user "airflow"
pulumi config set --secret airflow_postgres_password "airflow"

pulumi config set docker_registry_username ""
pulumi config set --secret docker_registry_password ""
pulumi config set docker_producer_image_name "localhost:5000/producer"

pulumi config get SECRET_NAME
pulumi config --show-secrets

# Preview
pulumi stack init dev
pulumi stack select dev
pulumi preview
or
pulumi preview --stack dev
pulumi preview --diff --stack dev

# Deploy
pulumi refresh
pulumi up

# Remove lock
pulumi cancel

# Delete
pulumi destroy --yes

# Clear state
pulumi stack rm dev --yes

# Stack documentation and architecture