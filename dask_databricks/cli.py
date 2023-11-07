import click
import os
import subprocess
import time
import socket

@click.group(name="databricks")
def main():
    """Tools to launch Dask on Databricks."""

@main.command()
def run():
    """Run Dask processes on a Databricks cluster."""

    print("Setting up Dask on a Databricks cluster.")

    DB_IS_DRIVER = os.getenv('DB_IS_DRIVER')
    DB_DRIVER_IP = os.getenv('DB_DRIVER_IP')

    if DB_IS_DRIVER == "TRUE":
        print("This node is the Dask scheduler.")
        subprocess.Popen(["dask", "scheduler"])
    else:
        print("This node is a Dask worker.")
        print(f"Connecting to Dask scheduler at {DB_DRIVER_IP}:8786")
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((DB_DRIVER_IP, 8786))
                sock.close()
                break
            except ConnectionRefusedError:
                print("Scheduler not available yet. Waiting...")
                time.sleep(1)
        subprocess.Popen(["dask", "worker", f"tcp://{DB_DRIVER_IP}:8786"])


if __name__ == "__main__":
    main()
