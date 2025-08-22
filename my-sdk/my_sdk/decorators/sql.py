from typing import Sequence, ClassVar
from airflow.decorators import DecoratedOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class _SQLDecoratedClass(DecoratedOperator, SQLExecuteQueryOperator):
    template_field: Sequence[str] = (
        (*DecoratorOperator.template_field, *SQLExecuteQueryOperator.template_field),
    )
    template_field_renderers: ClassVar[dict[str, str]] = {
        *DecoratorOperator.template_field_renderer,
        **SQLExecuteQueryOperator.template_field_renderer
    }

    custom_operator_name: str = "task.sql"
    overwrite_rtif_after_execution: bool = True
    
    def __init__(self, 
                 *, 
                 callable: Callable, 
                 ops_args: Collection[Any] | None = None,
                 ops_kwags: Collection[Any] | None = None,
                 **kwargs) -> None: 
        
        if kwargs.pop("multiple_outputs", None):
            warning.warn(
                f"multiple_output=True is not supported for {self.__class__.__name__}. Ignoring."
                UserWarning,
                stacklevel=3
            )