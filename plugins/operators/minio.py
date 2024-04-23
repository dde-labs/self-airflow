from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from plugins.hooks.minio import MinIOHook


class MinIOListOperator(BaseOperator):
    """
    List all objects from the MinIO bucket with the given string prefix in name.
    :param bucket_name: Name of the bucket.
    :param prefix: optional prefix of files to list.
    :param minio_conn_id: ID to minio connection.
    """

    def __init__(
        self,
        bucket_name,
        prefix: str = "",
        minio_conn_id: str = MinIOHook.default_conn_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.minio_conn_id = minio_conn_id

    def execute(self, context):
        list_of_objects = MinIOHook(self.minio_conn_id).list_objects(
            self.bucket_name, self.prefix
        )

        list_to_return = [obj.object_name for obj in list_of_objects]

        return list_to_return


class MinIOCopyObjectOperator(BaseOperator):
    """
    Copy files from one MinIO bucket to another.
    :param source_bucket_name: Name of the source bucket.
    :param source_object_name: Name of the source file (key).
    :param dest_bucket_name: Name of the destination bucket.
    :param dest_object_name: Name of the destination file (key).
    :param minio_conn_id: Name of the MinIO connection ID
        (default: minio_default)
    """

    template_fields = (
        "source_bucket_name",
        "source_object_names",
        "dest_bucket_name",
        "dest_object_names",
    )

    def __init__(
        self,
        source_bucket_name,
        source_object_names: str | list,
        dest_bucket_name,
        dest_object_names: str | list,
        minio_conn_id: str = MinIOHook.default_conn_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_bucket_name = source_bucket_name
        self.source_object_names = source_object_names
        self.dest_bucket_name = dest_bucket_name
        self.dest_object_names = dest_object_names
        self.minio_conn_id = minio_conn_id

        if isinstance(self.source_object_names, type(self.dest_object_names)):
            raise AirflowException(
                "Please provide either one string each to source_object_names "
                "and dest_object_names or two lists of strings of equal length"
            )
        if isinstance(self.source_object_names, list) and (
            len(self.source_object_names) != len(self.dest_object_names)
        ):
            raise AirflowException(
                "The lists provided to source_object_names and "
                "dest_object_names need to be of equal length."
            )

    def execute(self, context):
        if isinstance(self.source_object_names, list):
            for source_object, dest_object in zip(
                self.source_object_names, self.dest_object_names
            ):
                MinIOHook(self.minio_conn_id).copy_object(
                    self.source_bucket_name,
                    source_object,
                    self.dest_bucket_name,
                    dest_object,
                )

        else:
            MinIOHook(self.minio_conn_id).copy_object(
                self.source_bucket_name,
                self.source_object_names,
                self.dest_bucket_name,
                self.dest_object_names,
            )


class MinIODeleteObjectsOperator(BaseOperator):
    """
    Copy files from one MinIO bucket to another.
    :param bucket_name: Name of the source bucket.
    :param object_names: Name(s) of the source file(s) (key), string or list.
    """

    template_fields = (
        "bucket_name",
        "object_names",
    )

    def __init__(
        self,
        bucket_name,
        object_names: str | list,
        minio_conn_id: str = MinIOHook.default_conn_name,
        bypass_governance_mode=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_names = object_names
        self.minio_conn_id = minio_conn_id
        self.bypass_governance_mode = bypass_governance_mode

    def execute(self, context):
        MinIOHook(self.minio_conn_id).delete_objects(
            self.bucket_name,
            self.object_names,
            self.bypass_governance_mode,
        )
