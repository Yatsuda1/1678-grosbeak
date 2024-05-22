#!/usr/bin/env python3

import argparse
import subprocess
import dotenv
from pydantic import BaseModel, constr


class Env(BaseModel):
    DB_NAME: constr(min_length=1)
    MONGO_URI: constr(min_length=1)
    PICKLIST_PASSWORD: constr(min_length=1)


def make_env_args(env: Env):
    config = env.dict()
    env_list = []
    for key, value in config.items():
        env_list.append(f'--env "{key}={value}"')
    return " ".join(env_list)


def deploy(config: Env):
    FULL_IMAGE_NAME = "ghcr.io/frc1678/grosbeak:latest"
    CONTAINER_NAME = "grosbeak-prod"

    STOP_CONTAINER = f"docker stop {CONTAINER_NAME}"
    REMOVE_CONTAINER = f"docker rm {CONTAINER_NAME}"
    REMOVE_IMAGE = f"docker rmi {FULL_IMAGE_NAME}"
    PULL_IMAGE = f"docker pull {FULL_IMAGE_NAME}"
    START_CONTAINER = f"docker run -d --name {CONTAINER_NAME} -p 8002:80 {make_env_args(config)} {FULL_IMAGE_NAME}"
    final_command = " ; ".join(
        [STOP_CONTAINER, REMOVE_IMAGE, PULL_IMAGE, START_CONTAINER]
    )
    print(final_command)
    if input("Does this command look good? (y/N): ").lower() == "y":
        ssh_process = subprocess.Popen(
            ["ssh", "picklist@citrus.ant.isi.edu"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            bufsize=0,
        )
        ssh_process.stdin.write(STOP_CONTAINER + "\n")
        ssh_process.stdin.write(REMOVE_CONTAINER + "\n")
        ssh_process.stdin.write(REMOVE_IMAGE + "\n")
        ssh_process.stdin.write(PULL_IMAGE + "\n")
        ssh_process.stdin.write(START_CONTAINER + "\n")
        ssh_process.stdin.close()
        for line in ssh_process.stdout:
            if line == "END\n":
                break
            print(line, end="")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manually redeploy the grosbeak container on prod"
    )
    parser.add_argument("env_file", help="The .env file to use")

    args = parser.parse_args()

    config = dotenv.dotenv_values(args.env_file)
    config = Env(**config)
    deploy(config)
