from airflow.hooks.base import BaseHook
from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject


class MinIOHook(BaseHook):
    """Interact with MinIO.

    Note:
        This hook reference from
        ref: (
            https://github.com/TJaniF/airflow-elt-blueprint/blob/main/
            include/custom_operators/minio.py
        )

    :param minio_conn_id: ID of MinIO connection. (default: minio_default)
    """

    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "general"
    hook_name = "MinIOHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        minio_conn_id: str = "minio_default",
        secure_connection: bool = False,
        *args,
        **kwargs,
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.minio_conn_id = minio_conn_id
        self.secure_connection = secure_connection
        # call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self) -> Minio:
        """Function that initiates a new connection to MinIO."""

        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        client = Minio(
            conn.host,
            conn.login,
            conn.password,
            secure=self.secure_connection,
        )

        return client

    def put_object(self, bucket_name, object_name, data, length, part_size):
        """Write an object to Minio

        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket (key).
        :param data: An object having callable read() returning bytes object.
        :param length: Data size; (-1 for unknown size and set valid part_size).
        :param part_size: Multipart part size.
        """
        client = self.get_conn()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            length=length,
            part_size=part_size,
        )

    def list_objects(self, bucket_name, prefix: str = ""):
        """List objects in MinIO bucket.

        :param bucket_name: Name of the bucket.
        :param prefix: optional prefix of files to list.
        """

        client = self.get_conn()
        list_of_objects = client.list_objects(bucket_name=bucket_name, prefix=prefix)
        return list_of_objects

    def copy_object(
        self,
        source_bucket_name,
        source_object_name,
        dest_bucket_name,
        dest_object_name,
    ):
        """Copy a file from one MinIO bucket to another.

        :param source_bucket_name: Name of the source bucket.
        :param source_object_name: Name of the source file (key).
        :param dest_bucket_name: Name of the destination bucket.
        :param dest_object_name: Name of the destination file (key).
        """

        client = self.get_conn()
        copy_source = CopySource(source_bucket_name, source_object_name)
        client.copy_object(dest_bucket_name, dest_object_name, copy_source)

    def delete_objects(
        self,
        bucket_name,
        object_names,
        bypass_governance_mode: bool = False,
    ):
        """Delete file(s) in a MinIO bucket.

        :param bucket_name: Name of the source bucket.
        :param object_names: Name of the source file (key).
        :param bypass_governance_mode:
        """
        client = self.get_conn()
        if isinstance(object_names, list):
            objects_to_delete = [
                DeleteObject(object_name) for object_name in object_names
            ]
        else:
            objects_to_delete = [DeleteObject(object_names)]

        errors = client.remove_objects(
            bucket_name,
            objects_to_delete,
            bypass_governance_mode=bypass_governance_mode,
        )

        for error in errors:
            self.log.info("Error occurred when deleting object:", error)
