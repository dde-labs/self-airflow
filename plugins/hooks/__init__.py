import requests as re
from airflow.hooks.base import BaseHook


class MyHook(BaseHook):
    """Interact with <external tool>.
    :param my_conn_id: ID of the connection to <external tool>
    """

    # provide the name of the parameter which receives the connection id
    conn_name_attr = "my_conn_id"
    # provide a default connection id
    default_conn_name = "my_conn_default"
    # provide the connection type
    conn_type = "general"
    # provide the name of the hook
    hook_name = "MyHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(self, my_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.my_conn_id = my_conn_id
        # (optional) call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to your external tool."""
        # retrieve the passed connection id
        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        return conn

    # add additional methods to define interactions with your external tool


class CatFactHook(BaseHook):
    """Interact with the CatFactAPI.

    Performs a connection to the CatFactAPI and retrieves a cat fact client.

    :param cat_fact_conn_id: Connection ID to retrieve the CatFactAPI url.
    """

    # This connection will set with:
    # {
    #   "cat_conn_id": {
    #       "conn_type": "http",
    #       "host": "http://localhost"
    #   }
    # }
    conn_name_attr = "cat_conn_id"
    default_conn_name = "cat_conn_default"
    conn_type = "http"
    hook_name = "CatFact"

    def __init__(
        self, cat_fact_conn_id: str = default_conn_name, *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cat_fact_conn_id = cat_fact_conn_id
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to the CatFactAPI."""
        # get the connection object from the Airflow connection
        conn = self.get_connection(self.cat_fact_conn_id)

        # return the host URL
        return conn.host

    def log_cat_facts(self, number_of_cat_facts_needed: int = 1):
        """Function that logs between 1 and 10 catfacts depending on its input."""
        if number_of_cat_facts_needed < 1:
            self.log.info(
                "You will need at least one catfact! Setting request " "number to 1."
            )
            number_of_cat_facts_needed = 1
        if number_of_cat_facts_needed > 10:
            self.log.info(
                f"{number_of_cat_facts_needed} are a bit many. Setting request "
                f"number to 10."
            )
            number_of_cat_facts_needed = 10

        cat_fact_connection = self.get_conn()

        # log several cat facts using the connection retrieved
        latest: int = 0
        for i in range(number_of_cat_facts_needed):
            cat_fact = re.get(cat_fact_connection).json()
            self.log.info(cat_fact["fact"])
            latest = i
        return f"{latest} catfacts written to the logs!"
