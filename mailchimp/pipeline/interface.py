from typing import Callable, Any
from dataclasses import dataclass

Data = list[dict[str, Any]]


@dataclass
class Pipeline:
    name: str
    get_options: tuple[str, str]
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
