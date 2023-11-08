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
def run():
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
        subprocess.Popen(["dask", "scheduler", "--dashboard-address", ":8787,:8265,:8087,:8001"])
    else:
        log.info("This node is a Dask worker.")
        log.info(f"Connecting to Dask scheduler at {DB_DRIVER_IP}:8786")
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((DB_DRIVER_IP, 8786))
                sock.close()
                break
            except ConnectionRefusedError:
                log.info("Scheduler not available yet. Waiting...")
                time.sleep(1)
        subprocess.Popen(["dask", "worker", f"tcp://{DB_DRIVER_IP}:8786"])


if __name__ == "__main__":
    main()
