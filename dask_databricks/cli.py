import json
import logging
import os
import socket
import subprocess
import sys
import time

import click
from rich.logging import RichHandler


def get_logger():
    logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()])
    return logging.getLogger("dask_databricks")


@click.group(name="databricks")
def main():
    """Tools to launch Dask on Databricks."""


@main.command()
@click.option('--worker-command', help='Custom worker command')
@click.option('--worker-args', help='Additional worker arguments')
@click.option(
    "--cuda",
    is_flag=True,
    show_default=True,
    default=False,
    help="Use `dask cuda worker` from the dask-cuda package when starting workers.",
)
def run(worker_command, worker_args, cuda):
    """Run Dask processes on a Databricks cluster."""
    log = get_logger()

    log.info("Setting up Dask on a Databricks cluster.")

    DB_IS_DRIVER = os.getenv("DB_IS_DRIVER")
    DB_DRIVER_IP = os.getenv("DB_DRIVER_IP")

    if DB_DRIVER_IP is None or DB_IS_DRIVER is None:
        log.error(
            "Unable to find expected environment variables DB_IS_DRIVER and DB_DRIVER_IP. "
            "Are you running this command on a Databricks multi-node cluster?"
        )
        sys.exit(1)

    if DB_IS_DRIVER == "TRUE":
        log.info("This node is the Dask scheduler.")
        scheduler_process = subprocess.Popen(["dask", "scheduler", "--dashboard-address", ":8787,:8087"])
        time.sleep(5)  # give the scheduler time to start
        if scheduler_process.poll() is not None:
            log.error("Scheduler process has exited prematurely.")
            sys.exit(1)
    else:
        # Specify the same port for all workers
        worker_port = 8786
        log.info("This node is a Dask worker.")
        log.info(f"Connecting to Dask scheduler at {DB_DRIVER_IP}:{worker_port}")
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((DB_DRIVER_IP, worker_port))
                sock.close()
                break
            except ConnectionRefusedError:
                log.info("Scheduler not available yet. Waiting...")
                time.sleep(1)

        # Construct the worker command
        if worker_command:
            worker_command = worker_command.split()
        elif cuda:
            worker_command = ["dask", "cuda", "worker"]
        else:
            worker_command = ["dask", "worker"]

        if worker_args:
            try:
                # Try to decode the JSON-encoded worker_args
                worker_args_list = json.loads(worker_args)
                if not isinstance(worker_args_list, list):
                    raise ValueError("The JSON-encoded worker_args must be a list.")
            except json.JSONDecodeError:
                # If decoding as JSON fails, split worker_args by spaces
                worker_args_list = worker_args.split()

            worker_command.extend(worker_args_list)
        worker_command.append(f"tcp://{DB_DRIVER_IP}:{worker_port}")

        worker_process = subprocess.Popen(worker_command)
        time.sleep(5)  # give the worker time to start
        if worker_process.poll() is not None:
            log.error("Worker process has exited prematurely.")
            sys.exit(1)


if __name__ == "__main__":
    main()
