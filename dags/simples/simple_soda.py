from soda.scan import Scan
import pandas as pd
import pendulum

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["simple"],
)
def tutorial_soda_pandas():
    """Template on how to run data quality checks on Pandas dataframes using
    soda-core.
    """

    @task()
    def soda_scan():
        # Create a Soda scan object
        scan = Scan()
        scan.set_scan_definition_name("test")
        scan.set_data_source_name("dask")

        # Create an artificial pandas dataframe
        df_employee = pd.DataFrame(
            {"email": ["a@soda.io", "b@soda.io", "c@soda.io"]},
        )

        # Add Pandas dataframe to scan and assign a dataset name to refer
        # from checks yaml
        scan.add_pandas_dataframe(
            dataset_name="employee",
            pandas_df=df_employee,
        )

        # Define checks in yaml format.
        # Alternatively, you can refer to a yaml file using
        # ``scan.add_sodacl_yaml_file(<filepath>)``
        # NOTE: for more detail,
        #   Checks for basic validations
        # ---
        # checks for dim_customer:
        #   - row_count between 10 and 1000
        #   - missing_count(birth_date) = 0
        #   - invalid_percent(phone) < 1 %:
        #       valid format: phone number
        #   - invalid_count(number_cars_owned) = 0:
        #       valid min: 1
        #       valid max: 6
        #   - duplicate_count(phone) = 0

        checks: str = """
        checks for employee:
            - row_count > 0
        """

        scan.add_sodacl_yaml_str(checks)
        scan.set_verbose(True)
        scan.execute()

    soda_scan()


tutorial_soda_pandas()
