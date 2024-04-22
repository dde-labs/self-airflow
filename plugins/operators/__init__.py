from collections.abc import Sequence

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Read more https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html
"""


class HelloOperator(BaseOperator):
    # This template field will use for the below example:
    # >> hello_task = HelloOperator(
    # >>     task_id="task_id_1",
    # >>     name="{{ task_instance.task_id }}",
    # >>     world="Earth",
    # >> )
    template_fields: Sequence[str] = ("name",)

    def __init__(self, name: str, **kwargs) -> None:
        """When implementing custom operators, do not make any expensive
        operations in the __init__ method. The operators will be instantiated
        once per scheduler cycle per task using them, and making database
        calls can significantly slow down scheduling and waste resources.
        """
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        self.log.info(f"Start execute HelloOperator with context:\n{context}")
        message: str = f"Hello {self.name}"
        print(message)
        return message


class GreetingOperator(BaseOperator):
    @apply_defaults
    def __init__(self, greeting, name, **kwargs):
        super().__init__(**kwargs)
        self.greeting = greeting
        self.name = name

    def execute(self, context):
        self.log.info(f"{context}")
        greeting_message = f"{self.greeting}, {self.name}"
        self.log.info(greeting_message)
        return greeting_message


class MyBasicMathOperator(BaseOperator):
    """
    Example Operator that does basic arithmetic.
    :param first_number: first number to put into an equation
    :param second_number: second number to put into an equation
    :param operation: mathematical operation to perform
    """

    # provide a list of valid operations
    valid_operations = ("+", "-", "*", "/")
    # define which fields can use Jinja templating
    template_fields = ("first_number", "second_number")

    def __init__(
        self,
        first_number: float,
        second_number: float,
        operation: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.first_number = first_number
        self.second_number = second_number
        self.operation = operation

        # raise an import error if the operation provided is not valid
        if self.operation not in self.valid_operations:
            raise ValueError(
                f"{self.operation} is not a valid operation. "
                f"Choose one of {self.valid_operations}"
            )

    def execute(self, context):
        self.log.info(
            f"Equation: {self.first_number} {self.operation} " f"{self.second_number}"
        )
        if self.operation == "+":
            res = self.first_number + self.second_number
            self.log.info(f"Result: {res}")
            return res
        if self.operation == "-":
            res = self.first_number - self.second_number
            self.log.info(f"Result: {res}")
            return res
        if self.operation == "*":
            res = self.first_number * self.second_number
            self.log.info(f"Result: {res}")
            return res
        if self.operation == "/":
            try:
                res = self.first_number / self.second_number
            except ZeroDivisionError:
                self.log.critical(
                    "If you have set up an equation where you are trying "
                    "to divide by zero, you have done something WRONG."
                )
                raise ZeroDivisionError
            self.log.info(f"Result: {res}")
            return res
