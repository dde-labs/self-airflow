# Noted

## Prerequisite

Start loading the Docker Compose file from the Airflow repository.

=== Ubuntu

    ```shell
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'
    ```

=== Windows

    ```shell
    curl -Uri 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml' -outfile 'docker-compose.yaml'
    ```

I will follow the [How-to Guides](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) for provisioning all the Airflow services.
Airflow project structure does not complex, and you can see all configuration template
files on the [Airflow GitHub](https://github.com/apache/airflow/tree/2.9.0/airflow/config_templates).

```text
.
├── dags                        # Folder where all your DAGs go
│   ├── example-dag.py
│   └── redshift_transforms.py
├── Dockerfile                  # For Astronomer's Docker image and runtime overrides
├── include                     # For any scripts that your DAGs might need to access
│   └── sql
│       └── transforms.sql
├── packages.txt                # For OS-level packages
├── plugins                     # For any custom or community Airflow plugins
│   └── example-plugin.py
├── airflow.cfg                 # For Airflow config
└── requirements.txt            # For any Python packages
```

!!! note

    I able to use `include/` to keep DAG's config for dynamice DAG creation.

## Getting Started

```shell
$ docker compose up airflow-init
$ docker compose up -d
```

### Plugins Docker Compose

```shell
$ docker compose -f docker-compose.minio.yml up -d
```

## Production

We set the fernet and secret keys from the `cryptography` package.

```shell
$ pip install cryptography
$ set AIRFLOW__CORE__FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
$ set AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -base64 32)
```

!!! note

    Delete all

    ```shell
    $ docker compose -f docker-compose.minio.yml down --volumes --rmi all; \
        docker compose down --volumes --rmi all
    ```
