import ssl
import json
import asyncio
import argparse
import os
from vast_client_tools.utils import unix_serializer
from vast_client_tools.drivers.base import DriverBase, InvalidArgument


class KafkaDriver(DriverBase):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--bootstrap-servers', type=str, required=True,
                        help='Comma-separated list of Kafka broker addresses (e.g., "broker1:9092,broker2:9092").')
    parser.add_argument('--topic', type=str, required=True,
                        help='Kafka topic where the message will be published.')
    parser.add_argument('--max-request-size', type=int, default=1048576,  # 1 MB
                        help='Maximum request size in bytes. Default is 1 MB.')
    parser.add_argument('--client-id', type=str, default='vnfs-collector',
                        help='An ID string to pass to Kafka for logging and monitoring purposes.')
    parser.add_argument('--linger-ms', type=int, default=0,
                        help='The time to wait before sending a batch of messages. This can help with batching efficiency.')
    parser.add_argument('--compression-type', type=str, default=None, choices=('gzip', 'snappy', 'lz4', None),
                        help='Compression type for messages (e.g., gzip, snappy, lz4). This helps in reducing the size of messages.')
    parser.add_argument('--max-batch-size', type=int, default=16384,
                        help='Maximum size of buffered data per partition.')
    parser.add_argument('--retry-backoff-ms', type=int, default=100,
                        help='The time to wait before retrying a failed request. Helps in handling transient errors.')
    parser.add_argument('--sasl-username', type=str, default=None,
                        help='Username for SASL authentication.')
    parser.add_argument('--sasl-password', type=str, default=None,
                        help='Password for SASL authentication.')
    parser.add_argument('--security-protocol', type=str, default='PLAINTEXT',
                        choices=('PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'),
                        help='Protocol used to communicate with brokers.\n'
                             '- `PLAINTEXT`: No SASL credentials or SSL context needed.\n'
                             '- `SASL_PLAINTEXT`: SASL credentials are required, SSL context is not needed.\n'
                             '- `SSL`: SSL context is required, SASL credentials are not needed.\n'
                             '- `SASL_SSL`: Both SASL credentials and SSL context are required.\n')
    parser.add_argument('--ssl-ca-cert', type=str, default=None,
                        help='Path to the CA certificate file.')
    parser.add_argument('--ssl-cert', type=str, default=None,
                        help='Path to the client certificate file.')
    parser.add_argument('--ssl-key', type=str, default=None,
                        help='Path to the client private key file.')
    parser.add_argument('--sasl-mechanism', type=str, default='PLAIN',
                        choices=('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'),
                        help='Authentication mechanism for SASL.\n'
                             '- `PLAIN`: Transmits usernames and passwords in plaintext (i.e., not encrypted).\n'
                             '- `SCRAM-SHA-256`: Salted Challenge Response Authentication Mechanism (SCRAM) with SHA-256.\n'
                             '- `SCRAM-SHA-512`: Salted Challenge Response Authentication Mechanism (SCRAM) with SHA-512.\n'
                        )

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(bootstrap_servers={self.bootstrap_servers}, "
            f"client_id={self.client_id}, "
            f"max_request_size={self.max_request_size}, "
            f"linger_ms={self.linger_ms}, "
            f"compression_type={self.compression_type}, "
            f"max_batch_size={self.max_batch_size}, "
            f"retry_backoff_ms={self.retry_backoff_ms}, "
            f"sasl_mechanism={self.sasl_mechanism}, "
            f"security_protocol={self.security_protocol}, "
            f"topic={self.topic})"
        )

    async def setup(self, args=(), namespace=None):
        from aiokafka import AIOKafkaProducer

        args = await super().setup(args, namespace)
        self.bootstrap_servers = [server.strip() for server in args.bootstrap_servers.split(',')]
        self.topic = args.topic
        self.max_request_size = args.max_request_size
        self.client_id = args.client_id
        self.linger_ms = args.linger_ms
        self.compression_type = args.compression_type
        self.max_batch_size = args.max_batch_size
        self.retry_backoff_ms = args.retry_backoff_ms
        self.security_protocol = args.security_protocol
        self.sasl_mechanism = args.sasl_mechanism
        self.sasl_plain_username = args.sasl_username
        self.sasl_plain_password = args.sasl_password
        self.ssl_ca_cert = args.ssl_ca_cert
        self.ssl_cert = args.ssl_cert
        self.ssl_key = args.ssl_key

        # Validate SASL credentials and security_protocol
        if self.security_protocol == 'SASL_PLAINTEXT':
            if not (self.sasl_plain_username and self.sasl_plain_password):
                raise InvalidArgument(
                    "SASL_PLAINTEXT requires SASL credentials."
                )
        elif self.security_protocol == 'SASL_SSL':
            if not (self.sasl_plain_username and self.sasl_plain_password):
                raise InvalidArgument(
                    "SASL_SSL requires SASL credentials."
                )
            if not self.ssl_cert or not self.ssl_key:
                raise InvalidArgument(
                    "SASL_SSL requires both client certificate and key."
                )
        elif self.security_protocol == 'SSL':
            if not self.ssl_cert or not self.ssl_key:
                raise InvalidArgument(
                    "SSL requires both client certificate and key."
                )
        elif self.security_protocol == 'PLAINTEXT':
            # No SASL credentials or SSL context required for PLAINTEXT
            if self.sasl_plain_username or self.sasl_plain_password:
                raise InvalidArgument(
                    "SASL credentials provided but security_protocol is PLAINTEXT."
                )
            if self.ssl_cert or self.ssl_key:
                raise InvalidArgument(
                    "SSL context provided but security_protocol is PLAINTEXT."
                )

        # Create SSL context if needed
        ssl_context = None
        if self.security_protocol in ('SSL', 'SASL_SSL'):
            ssl_context = self._create_ssl_context()

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            max_request_size=self.max_request_size,
            linger_ms=self.linger_ms,
            compression_type=self.compression_type,
            max_batch_size=self.max_batch_size,
            retry_backoff_ms=self.retry_backoff_ms,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            ssl_context=ssl_context,
        )
        await self.producer.start()
        self.logger.info(f"{self} has been initialized.")

    async def teardown(self):
        if hasattr(self, 'producer'):
            self.logger.info("Shutting down Kafka producer.")
            await self.producer.stop()

    def _create_ssl_context(self):
        """Create an SSL context from PEM files."""
        if not (self.ssl_cert and self.ssl_key):
            raise InvalidArgument("Both SSL certificate and key must be provided.")

        if not os.path.exists(self.ssl_cert):
            raise InvalidArgument(f"SSL certificate file not found: {self.ssl_cert}")
        if not os.path.exists(self.ssl_key):
            raise InvalidArgument(f"SSL key file not found: {self.ssl_key}")

        ssl_context = ssl.create_default_context()
        try:
            ssl_context.load_cert_chain(certfile=self.ssl_cert, keyfile=self.ssl_key)
            if self.ssl_ca_cert:
                if not os.path.exists(self.ssl_ca_cert):
                    raise InvalidArgument(f"CA certificate file not found: {self.ssl_ca_cert}")
                ssl_context.load_verify_locations(cafile=self.ssl_ca_cert)
        except Exception as e:
            raise InvalidArgument(f"Error loading SSL files: {e}")

        return ssl_context

    async def store_sample(self, data):
        # Create a list to hold futures for batch sending
        futures = []
        for _, entry in data.iterrows():
            # Create headers from predefined columns in the format [("key", b"value")]
            headers = [
                ("HOSTNAME", entry.HOSTNAME.encode()),
                ("UID", str(entry.UID).encode()),
                ("COMM", entry.COMM.encode()),
                ("MOUNT", entry.MOUNT.encode()),
                ("REMOTE_PATH", entry.REMOTE_PATH.encode()),
            ]
            key = f"{entry.HOSTNAME}:{entry.COMM}:{entry.UID}:{entry.PID}".encode()
            # Add environment variables to headers if provided
            if self.common_args.envs:
                for env in self.common_args.envs:
                    # Retrieve the value and encode it to bytes
                    value = entry.TAGS.get(env, '')
                    headers.append((env, value.encode()))
            try:
                message = json.dumps(entry.to_dict(), default=unix_serializer).encode()
                # Send the message and store the future
                future = await self.producer.send(topic=self.topic, value=message, headers=headers, key=key)
                futures.append(future)
            except Exception as e:
                self.logger.error(f"Error sending message: {e}")

        # Wait for all messages to be acknowledged
        try:
            await asyncio.gather(*futures)
            self.logger.info(f"{len(data)} message(s) have been sent.")
        except Exception as e:
            self.logger.error(f"Error waiting for messages to be acknowledged: {e}")
