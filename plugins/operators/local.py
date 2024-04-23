import io
import json

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator


from plugins.hooks.minio import MinIOHook


class LocalFilesystemToMinIOOperator(BaseOperator):
    """Operator that writes content from a local json/csv file or directly
    provided json serializable information to MinIO.

    :param bucket_name: (required) Name of the bucket.
    :param object_name: (required) Object name in the bucket (key).
    :param local_file_path: Path to the local file that is uploaded to MinIO.
    :param json_serializable_information: Alternatively to providing a filepath
        provide json-serializable information.
    :param minio_conn_id: connection id of the MinIO connection.
    :param data: An object having callable read() returning bytes object.
    :param length: Data size; (default: -1 for unknown size and set valid
        part_size).
    :param part_size: Multipart part size (default: 10*1024*1024).
    """

    supported_filetypes = ["json", "csv"]
    template_fields = (
        "bucket_name",
        "object_name",
        "local_file_path",
        "json_serializable_information",
        "minio_conn_id",
        "length",
        "part_size",
    )

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        bucket_name,
        object_name,
        local_file_path=None,
        json_serializable_information=None,
        minio_conn_id: str = MinIOHook.default_conn_name,
        length=-1,
        part_size=10 * 1024 * 1024,
        *args,
        **kwargs,
    ):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.local_file_path = local_file_path
        self.json_serializable_information = json_serializable_information
        self.minio_conn_id = minio_conn_id
        self.length = length
        self.part_size = part_size

        if self.local_file_path:
            self.filetype = self.local_file_path.split("/")[-1].split(".")[1]
            if self.filetype not in self.supported_filetypes:
                raise AirflowException(
                    f"The LocalFilesystemToMinIOOperator currently only "
                    f"supports uploading the following filetypes: "
                    f"{self.supported_filetypes}"
                )
        elif not self.json_serializable_information:
            raise AirflowException(
                "Provide at least one of the parameters local_file_path or "
                "json_serializable_information"
            )

    # .execute runs when the task runs
    def execute(self, context):

        if self.local_file_path:
            if self.filetype == "csv":
                with open(self.local_file_path) as f:
                    string_file = f.read()
                    data = io.BytesIO(bytes(string_file, "utf-8"))

            if self.filetype == "json":
                with open(self.local_file_path) as f:
                    string_file = f.read()
                    data = io.BytesIO(bytes(json.dumps(string_file), "utf-8"))
        else:
            data = io.BytesIO(
                bytes(json.dumps(self.json_serializable_information), "utf-8")
            )

        response = MinIOHook(self.minio_conn_id).put_object(
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            data=data,
            length=self.length,
            part_size=self.part_size,
        )
        self.log.info(response)
