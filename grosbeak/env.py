"""
This should be cleaner
"""
import os
from typing import Any, Callable

from pydantic import BaseModel


def get_envs(envs: dict[str, Callable[[str], Any]]) -> dict[str, str | None]:
    """
    Get environment variables and cast them to the correct type.
    """
    env_dict = dict()
    for env in envs:
        env_value = os.environ.get(env)
        if env_value is not None:
            try:
                env_dict[env] = envs[env](env_value)
            except ValueError:
                print("failed type cast")
                env_dict[env] = None
        else:
            print("env value is none")
            env_dict[env] = None
    return env_dict


class Envs(BaseModel):
    """
    Model that represents the environment variables needed
    """

    PICKLIST_PASSWORD: str
    DB_NAME: str
    MONGO_URI: str


env = Envs(**get_envs(Envs.__annotations__))
