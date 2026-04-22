'''
MinIO on Kubernetes — Helm chart.

Deploys MinIO object storage and exposes create_buckets() to provision
buckets via a Kubernetes Job using the MinIO Client (mc).

Example:
    minio = Minio('minio', MinioArgs(
        namespace=ns.metadata.name,
        root_password=config.require_secret('minio_root_password'),
        persistence_size='20Gi',
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))

    minio.create_buckets('buckets', [
        BucketArgs(name='bronze'),
        BucketArgs(name='silver'),
        BucketArgs(name='gold'),
        BucketArgs(name='spark-logs'),
    ])
'''

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.batch.v1 import Job
from pulumi_kubernetes.core.v1 import ConfigMap
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts
from pulumi_kubernetes.networking.v1 import Ingress

from config.utils.utils import _deep_merge

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class BucketArgs:
    '''Configuration for a single MinIO bucket.'''

    name: str
    '''Name of the bucket to create.'''

    versioning: bool = False
    '''Enable object versioning on this bucket.'''

    object_locking: bool = False
    '''Enable object locking (WORM). Requires versioning.'''

    quota: Optional[str] = None
    '''Storage quota, e.g. "10GB" or "1TB". None means unlimited.'''

    policy: Optional[str] = None
    '''Canned access policy: "none", "download", "upload", or "public".'''


@dataclass
class MinioArgs:
    '''Configuration arguments for MinIO deployment.'''

    namespace: Input[str] = 'minio'
    '''Kubernetes namespace to deploy MinIO into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '5.2.0'
    '''Version of the MinIO Helm chart (https://charts.min.io/).'''

    mode: str = 'standalone'
    '''Deployment mode: "standalone" (single pod) or "distributed" (multiple pods for HA).'''

    replicas: int = 1
    '''Number of pods — only used when mode="distributed".'''

    root_user: str = 'minioadmin'
    '''MinIO root username (the admin account).'''

    root_password: Optional[Input[str]] = None
    '''MinIO root password. Accepts a Pulumi secret Output. Defaults to "minioadmin".'''

    persistence_enabled: bool = True
    '''Mount a PersistentVolumeClaim for durable data storage.'''

    persistence_size: Input[str] = '10Gi'
    '''PVC size, e.g. "10Gi" or "100Gi".'''

    storage_class: Optional[Input[str]] = None
    '''StorageClass for the PVC. None uses the cluster default.'''

    service_type: str = 'ClusterIP'
    '''Kubernetes service type for the MinIO API: ClusterIP, NodePort, or LoadBalancer.'''

    console_service_type: str = 'ClusterIP'
    '''Kubernetes service type for the MinIO Console (web UI).'''

    api_port: int = 9000
    '''MinIO S3-compatible API port.'''

    console_port: int = 9001
    '''MinIO Console (web UI) port.'''

    cluster_domain: Input[str] = 'cluster.local'
    '''Kubernetes cluster domain suffix, usually "cluster.local".'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '512Mi', 'cpu': '250m'},
        'limits':   {'memory': '1Gi',   'cpu': '500m'},
    })
    '''CPU and memory requests/limits for the MinIO pod.'''

    ingress_enabled: bool = False
    '''Create an Ingress for external access to both the API and Console.'''

    ingress_domain: Optional[Input[str]] = None
    '''
    Base domain for Ingress hosts. Creates:
      - minio.<domain>         → MinIO S3 API
      - minio-console.<domain> → MinIO Console UI
    '''

    ingress_class_name: Input[str] = 'nginx'
    '''Ingress class name (e.g. "nginx", "traefik").'''

    ingress_annotations: Optional[dict] = None
    '''Extra Ingress annotations merged on top of the proxy-body-size/timeout defaults.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config. Use for any chart option not exposed above.'''


class Minio(pulumi.ComponentResource):
    '''
    Deploys MinIO to Kubernetes using the official MinIO Helm chart.

    MinIO provides S3-compatible object storage. Use create_buckets() after
    construction to provision buckets via a Kubernetes Job.

    Outputs:
        namespace        — Kubernetes namespace
        endpoint         — Internal S3 API URL  (http://<release>.<ns>.svc.<domain>:<port>)
        console_endpoint — Internal Console URL  (http://<release>-console.<ns>.svc.<domain>:<port>)
        api_url          — External S3 URL if ingress enabled, else same as endpoint
        console_url      — External Console URL if ingress enabled, else same as console_endpoint

    Example:
        minio = Minio('minio', MinioArgs(
            namespace=ns.metadata.name,
            root_password=config.require_secret('minio_root_password'),
            ingress_enabled=True,
            ingress_domain='k8lh.local',
        ))

        minio.create_buckets('buckets', [
            BucketArgs(name='bronze', versioning=True),
            BucketArgs(name='spark-logs'),
        ])
    '''

    def __init__(
        self,
        name: str,
        args: MinioArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:minio:Minio', name, {}, opts)

        args = args or MinioArgs()
        self._release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)
        self._root_password = Output.from_input(args.root_password or 'minioadmin')
        # Stored for use in create_buckets()
        self._root_user = args.root_user
        self._api_port = args.api_port
        self._cluster_domain = args.cluster_domain

        # ── Helm values ───────────────────────────────────────────────────────
        values = json.loads((CONFIG_DIR / 'helm/helm_values_minio.json').read_text())

        values['fullnameOverride'] = self._release_name
        values['mode']             = args.mode
        values['rootUser']         = args.root_user
        values['replicas']         = args.replicas if args.mode == 'distributed' else 1
        values['persistence']['enabled'] = args.persistence_enabled
        values['persistence']['size']    = args.persistence_size
        values['service']['type']        = args.service_type
        values['consoleService']['type'] = args.console_service_type
        values['resources']              = args.resources

        if args.root_password:
            values['rootPassword'] = args.root_password
        if args.storage_class:
            values['persistence']['storageClass'] = args.storage_class

        values = _deep_merge(values, args.extra_values)

        # ── Chart ─────────────────────────────────────────────────────────────
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='minio',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://charts.min.io/'),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        self.namespace = self._namespace
        self.endpoint = Output.concat(
            'http://', self._release_name, '.', self._namespace,
            '.svc.', args.cluster_domain, ':', str(args.api_port),
        )
        self.console_endpoint = Output.concat(
            'http://', self._release_name, '-console.', self._namespace,
            '.svc.', args.cluster_domain, ':', str(args.console_port),
        )

        # ── Ingress ───────────────────────────────────────────────────────────
        if args.ingress_enabled and args.ingress_domain:
            api_host     = Output.concat('minio.', Output.from_input(args.ingress_domain))
            console_host = Output.concat('minio-console.', Output.from_input(args.ingress_domain))

            annotations = {
                'nginx.ingress.kubernetes.io/proxy-body-size':    '0',
                'nginx.ingress.kubernetes.io/proxy-read-timeout': '600',
                'nginx.ingress.kubernetes.io/proxy-send-timeout': '600',
            }
            if args.ingress_annotations:
                annotations.update(args.ingress_annotations)

            spec_api = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            spec_api['ingressClassName'] = args.ingress_class_name
            api_rule = spec_api['rules'][0]
            api_rule['host'] = api_host
            api_rule['http']['paths'][0]['backend']['service']['name']           = self._release_name
            api_rule['http']['paths'][0]['backend']['service']['port']['number'] = args.api_port

            spec_console = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            spec_console['ingressClassName'] = args.ingress_class_name
            console_rule = spec_console['rules'][0]
            console_rule['host'] = console_host
            console_rule['http']['paths'][0]['backend']['service']['name']           = f'{self._release_name}-console'
            console_rule['http']['paths'][0]['backend']['service']['port']['number'] = args.console_port
            
            spec_api['rules'] = [api_rule, console_rule]

            Ingress(
                f'{self._release_name}-ingress',
                metadata={'namespace': self._namespace, 'annotations': annotations},
                spec=spec_api,
                opts=pulumi.ResourceOptions(parent=self, depends_on=[self.chart]),
            )

            self.api_url     = Output.concat('http://', api_host)
            self.console_url = Output.concat('http://', console_host)
        else:
            self.api_url     = self.endpoint
            self.console_url = self.console_endpoint

        self.register_outputs({
            'namespace':        self.namespace,
            'endpoint':         self.endpoint,
            'console_endpoint': self.console_endpoint,
            'api_url':          self.api_url,
            'console_url':      self.console_url,
        })

    def create_buckets(
        self,
        name: str,
        buckets: 'BucketArgs | List[BucketArgs]',
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Provision one or more MinIO buckets using a Kubernetes Job (mc client).

        The job connects to the MinIO API, creates each bucket, and applies
        versioning, object-locking, quota, and policy settings as requested.
        The job runs once and completes — it is not a daemon.

        Args:
            name:    Pulumi resource name prefix.
            buckets: Single BucketArgs or a list.
            opts:    Optional extra resource options (e.g. depends_on).

        Returns:
            The Kubernetes Job resource.

        Example:
            minio.create_buckets('lakehouse-buckets', [
                BucketArgs(name='bronze', versioning=True),
                BucketArgs(name='silver', versioning=True),
                BucketArgs(name='gold'),
                BucketArgs(name='spark-logs'),
            ])
        '''
        if isinstance(buckets, BucketArgs):
            buckets = [buckets]

        # ── Build mc shell commands ───────────────────────────────────────────
        scripts_dir   = CONFIG_DIR / 'scripts'
        create_tpl    = (scripts_dir / 'create_bucket.sh').read_text().strip()
        version_tpl   = (scripts_dir / 'bucket_versioning.sh').read_text().strip()
        retention_tpl = (scripts_dir / 'bucket_retention.sh').read_text().strip()
        quota_tpl     = (scripts_dir / 'bucket_quota.sh').read_text().strip()
        policy_tpl    = (scripts_dir / 'bucket_policy.sh').read_text().strip()

        commands = ['sleep 5']  # brief wait for MinIO readiness
        for bucket in buckets:
            commands.append(create_tpl.replace('{{NAME}}', bucket.name))
            if bucket.versioning:
                commands.append(version_tpl.replace('{{NAME}}', bucket.name))
            if bucket.object_locking:
                commands.append(retention_tpl.replace('{{NAME}}', bucket.name))
            if bucket.quota:
                commands.append(
                    quota_tpl.replace('{{NAME}}', bucket.name).replace('{{QUOTA}}', bucket.quota)
                )
            if bucket.policy:
                commands.append(
                    policy_tpl.replace('{{NAME}}', bucket.name).replace('{{POLICY}}', bucket.policy)
                )

        # ── Job spec ──────────────────────────────────────────────────────────
        spec = json.loads((CONFIG_DIR / 'jobs/mc_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['args'] = [' && '.join(commands)]
        # MC_HOST_minio env var: http://<user>:<password>@<host>:<port>
        container['env'][0]['value'] = Output.concat(
            'http://', self._root_user, ':',
            self._root_password, '@',
            self._release_name, '.', self._namespace,
            '.svc.', self._cluster_domain, ':', str(self._api_port),
        )

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-bucket-job',
            metadata={
                'namespace': self._namespace,
                'labels': {'app': 'minio-bucket-provisioner'},
            },
            spec=spec,
            opts=job_opts,
        )

    def sync_objects(
        self,
        name: str,
        local_dir: Path,
        bucket: str,
        glob: str = '*',
        opts: pulumi.ResourceOptions = None,
    ) -> Optional[Job]:
        '''
        Sync every file under local_dir (flat, matching `glob`) to the named
        MinIO bucket. File contents are packed into a ConfigMap and copied into
        the bucket by a short-lived `mc` Job.

        The Job's pod template carries a SHA-256 annotation of all file
        contents, so Pulumi replaces the Job whenever any file changes — the
        new Job runs on the next `pulumi up` and re-uploads the set.

        ConfigMap data is capped at 1 MiB across all keys; keep scripts small
        or switch to a different staging path (e.g. presigned upload) for
        larger payloads.

        Args:
            name:      Pulumi resource name prefix.
            local_dir: Host directory to read from. Only top-level files matching
                       `glob` are uploaded; subdirectories are ignored.
            bucket:    Target MinIO bucket. Must already exist (call
                       create_buckets first).
            glob:      File pattern to include (defaults to every file).
            opts:      Optional extra resource options (e.g. depends_on the
                       bucket-provisioning Job).

        Returns:
            The sync Job, or None if local_dir has no matching files.

        Example:
            minio.sync_objects(
                'spark-jobs-sync',
                local_dir=Path(__file__).parent / 'spark' / 'jobs',
                bucket='spark-jobs',
                opts=pulumi.ResourceOptions(depends_on=[buckets]),
            )
        '''
        local_dir = Path(local_dir)
        files     = {p.name: p.read_text() for p in sorted(local_dir.glob(glob)) if p.is_file()}

        if not files:
            return None

        content_hash = hashlib.sha256(
            ''.join(f'{k}:{v}' for k, v in sorted(files.items())).encode()
        ).hexdigest()[:16]

        cm = ConfigMap(
            f'{name}-files',
            metadata={'namespace': self._namespace},
            data=files,
            opts=pulumi.ResourceOptions(parent=self),
        )

        commands = [
            'sleep 5',
            f'mc cp /jobs/* minio/{bucket}/',
        ]

        spec = json.loads((CONFIG_DIR / 'jobs/mc_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['args']         = [' && '.join(commands)]
        container['volumeMounts'] = [{'name': 'jobs', 'mountPath': '/jobs'}]
        container['env'][0]['value'] = Output.concat(
            'http://', self._root_user, ':',
            self._root_password, '@',
            self._release_name, '.', self._namespace,
            '.svc.', self._cluster_domain, ':', str(self._api_port),
        )
        spec['template']['spec']['volumes'] = [
            {'name': 'jobs', 'configMap': {'name': cm.metadata.name}},
        ]
        # Stamp the hash on the pod template so Pulumi treats the Job as
        # changed whenever any source file changes.
        spec['template'].setdefault('metadata', {})['annotations'] = {
            'k8lh.io/content-hash': content_hash,
        }

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart, cm])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-sync-job',
            metadata={
                'namespace': self._namespace,
                'labels':    {'app': 'minio-object-sync'},
            },
            spec=spec,
            opts=job_opts,
        )
