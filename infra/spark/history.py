'''
Spark History Server — single deployment shared by Connect and the Operator.

A passive read-only UI that scans an S3 bucket of Spark event logs and renders
job/stage/SQL history. Both the Connect server and SparkApplication CRs write
their logs to the same bucket, so this one deployment surfaces every job run
across the cluster.

The "wiring" between the history server and the other Spark components is the
shared bucket — there is no direct network connection. Set Connect's
spark.eventLog.dir and the operator's sparkConf.spark.eventLog.dir to the same
s3a:// path, then point this component's event_log_bucket at that bucket.

Live updates:
    Setting spark.eventLog.rolling.enabled=true on each writer (Connect and
    SparkApplications) makes Spark flush event log segments while the job is
    still running. Combined with spark.history.fs.update.interval=5s here,
    in-flight runs become visible in the History UI within ~5 seconds.

Example:
    history = SparkHistory('spark-history', SparkHistoryArgs(
        namespace=ns.metadata.name,
        image=spark.image,
        s3_endpoint=minio.endpoint,
        s3_access_key='minioadmin',
        s3_secret_key=minio_password,
        event_log_bucket='spark-logs',
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))
'''

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apps.v1 import Deployment
from pulumi_kubernetes.core.v1 import Service
from pulumi_kubernetes.networking.v1 import Ingress

CONFIG_DIR = Path(__file__).parent.parent / 'config'

HISTORY_SERVER_PORT = 18080


@dataclass
class SparkHistoryArgs:
    '''Configuration arguments for the Spark History Server.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Resource name prefix for K8s resources. Defaults to the Pulumi resource name.'''

    image: Input[str] = ''
    '''
    Full image ref including tag (e.g. docker.io/myuser/spark:4.0.0).
    Reuse the custom Spark image built by the Spark component so the JAR
    download init container has a matching Spark distribution.
    '''

    event_log_bucket: Input[str] = 'spark-logs'
    '''S3/MinIO bucket the history server reads event logs from.'''

    s3_endpoint: Input[str] = ''
    '''S3/MinIO endpoint URL.'''

    s3_access_key: Input[str] = ''
    '''S3 access key.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    s3_region: Input[str] = 'us-east-1'
    '''S3/MinIO region.'''

    update_interval: str = '5s'
    '''
    How often the history server rescans the log directory.
    Lower = snappier live-job updates but more S3 LIST calls.
    '''

    ingress_enabled: bool = False
    '''Create an Ingress for the History Server UI at spark.<domain>.'''

    ingress_domain: Input[str] = ''
    '''Base domain. Creates spark.<domain> → History Server.'''

    ingress_class_name: Input[str] = 'nginx'
    '''Ingress class name.'''


class SparkHistory(pulumi.ComponentResource):
    '''
    Deploys a Spark History Server that reads event logs from a shared S3 bucket.

    Independent of Connect and the Operator at the K8s layer — wired only by
    pointing all three at the same bucket. Both Connect and SparkApplications
    must enable rolling event logs to surface in-flight runs in the UI.

    Outputs:
        namespace          — Kubernetes namespace
        history_server_url — http://<host>:<port> URL for the History Server UI
    '''

    def __init__(
        self,
        name: str,
        args: SparkHistoryArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:spark:SparkHistory', name, {}, opts)

        args = args or SparkHistoryArgs()
        self._namespace  = Output.from_input(args.namespace)
        image            = Output.from_input(args.image)
        s3_endpoint      = Output.from_input(args.s3_endpoint)
        s3_access_key    = Output.from_input(args.s3_access_key)
        s3_secret_key    = Output.from_input(args.s3_secret_key)
        s3_region        = Output.from_input(args.s3_region)
        event_log_bucket = Output.from_input(args.event_log_bucket)
        ingress_domain   = Output.from_input(args.ingress_domain)
        release          = args.release_name or name

        hs_name      = release
        hs_app_label = {'app': hs_name}

        spark_history_opts = Output.concat(
            '-Dspark.history.fs.logDirectory=s3a://', event_log_bucket, '/ ',
            f'-Dspark.history.fs.update.interval={args.update_interval} ',
            '-Dspark.hadoop.fs.s3a.endpoint=', s3_endpoint, ' ',
            '-Dspark.hadoop.fs.s3a.access.key=', s3_access_key, ' ',
            '-Dspark.hadoop.fs.s3a.secret.key=', s3_secret_key, ' ',
            '-Dspark.hadoop.fs.s3a.path.style.access=true ',
            '-Dspark.hadoop.fs.s3a.endpoint.region=', s3_region, ' ',
            '-Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
        )

        hs_spec = json.loads((CONFIG_DIR / 'resources/history_server_spec.json').read_text())
        hs_spec['selector']['matchLabels']                              = hs_app_label
        hs_spec['template']['metadata']['labels']                       = hs_app_label
        hs_spec['template']['spec']['initContainers'][0]['image']       = image
        hs_spec['template']['spec']['containers'][0]['image']           = image
        hs_spec['template']['spec']['containers'][0]['env'][0]['value'] = spark_history_opts

        Deployment(
            f'{name}-deployment',
            metadata={'name': hs_name, 'namespace': self._namespace},
            spec=hs_spec,
            opts=pulumi.ResourceOptions(parent=self),
        )

        Service(
            f'{name}-svc',
            metadata={'name': hs_name, 'namespace': self._namespace},
            spec={
                'selector': hs_app_label,
                'ports':    [{'port': HISTORY_SERVER_PORT, 'targetPort': HISTORY_SERVER_PORT}],
            },
            opts=pulumi.ResourceOptions(parent=self),
        )

        if args.ingress_enabled and args.ingress_domain:
            host = Output.concat('spark.', ingress_domain)

            ingress_spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            ingress_spec['ingressClassName']                                                     = args.ingress_class_name
            ingress_spec['rules'][0]['host']                                                     = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['name']           = hs_name
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'] = HISTORY_SERVER_PORT

            Ingress(
                f'{name}-ingress',
                metadata={
                    'name':        f'{hs_name}-ingress',
                    'namespace':   self._namespace,
                    'annotations': {'kubernetes.io/ingress.class': args.ingress_class_name},
                },
                spec=ingress_spec,
                opts=pulumi.ResourceOptions(parent=self),
            )
            self.history_server_url = Output.concat('http://', host)
        else:
            self.history_server_url = Output.concat(
                'http://', hs_name, '.', self._namespace,
                '.svc.cluster.local:', str(HISTORY_SERVER_PORT),
            )

        self.namespace = self._namespace
        self.register_outputs({
            'namespace':          self.namespace,
            'history_server_url': self.history_server_url,
        })
