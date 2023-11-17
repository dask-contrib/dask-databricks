import json
import logging
import os
import random
import socket
import subprocess
import sys
import time
from datetime import datetime

import click
from rich import box
from rich.color import ANSI_COLOR_NAMES
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

console = Console()

NODE_COLOURS = ["medium_spring_green", "light_steel_blue1", "wheat1", "medium_orchid"]

# Generate list of random colours from rich
# import random
# from rich.color import Color
#
# for i in range(100):
#     colour = Color.random()
#     print(f'"{colour.name}",', end="


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


@main.group()
def logs():
    """View cluster init logs."""


def _get_logs_at_path(path):
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError:
        raise RuntimeError("Please install databricks-sdk.")
    clusters = {}

    for cluster in dbutils.fs.ls(path):
        cluster_id = cluster.path.split("/")[-1]
        clusters[cluster_id] = {}
        for node in dbutils.fs.ls(cluster.path + "/init_scripts"):
            for log in dbutils.fs.ls(node.path):
                filename = log.path.split("/")[-1]
                channel = filename.split(".")[-2]
                datetime = "_".join(filename.split("_")[:2])
                node_name = log.path.split("/")[-2].split("_", 1)[-1].replace("_", ".")
                if datetime not in clusters[cluster_id]:
                    clusters[cluster_id][datetime] = {}

                if node_name not in clusters[cluster_id][datetime]:
                    clusters[cluster_id][datetime][node_name] = {}

                clusters[cluster_id][datetime][node_name][channel] = log.path
    return clusters


def _get_node_color(i):
    if i < len(NODE_COLOURS):
        return NODE_COLOURS[i]
    else:
        return random.choice(list(ANSI_COLOR_NAMES))


def _prettify_launch_time(launch_time):
    return datetime.strptime(launch_time, "%Y%m%d_%H%M%S").strftime("%b %d %Y %H:%M")


@logs.command()
@click.argument("path")
@click.option("--show-filenames", help="Show filenames in the output", is_flag=True, default=False, show_default=True)
def ls(path, show_filenames):
    # TODO add flag to list filenames
    table = Table(box=box.SIMPLE_HEAD)
    table.add_column("Cluster", style="cyan", no_wrap=True)
    table.add_column("Start time", style="plum2")
    table.add_column("Node Count")
    table.add_column("Node IPs")
    if show_filenames:
        table.add_column("Filenames")
    with console.status("[bright_black]Finding logs..."):
        clusters = _get_logs_at_path(path)
    for cluster in clusters:
        first = True
        for launch_time in sorted(clusters[cluster], reverse=True):
            pretty_launch_time = _prettify_launch_time(launch_time)
            cluster_name = cluster if first else ""
            node_list = ", ".join(
                f"[{_get_node_color(i)}]{name}[/{_get_node_color(i)}]"
                for i, name in enumerate(clusters[cluster][launch_time])
            )
            data = [cluster_name, pretty_launch_time, str(len(clusters[cluster][launch_time])), node_list]
            if show_filenames:
                filenames = ""
                for i, node in enumerate(clusters[cluster][launch_time]):
                    for channel in ["stdout", "stderr"]:
                        node_colour = _get_node_color(i)
                        filenames += f"[{node_colour}]{clusters[cluster][launch_time][node][channel]}[/{node_colour}]\n"
                data.append(filenames)
            table.add_row(*data)
            first = False

    console.print(table)


@logs.command()
@click.argument("path")
@click.argument("cluster")
def cat(path, cluster):
    # TODO add a flag for selecting which start time to view
    # TODO add a flag to filter which nodes to view logs for
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError:
        raise RuntimeError("Please install databricks-sdk.")

    with console.status("[bright_black]Finding logs..."):
        clusters = _get_logs_at_path(path)

    if cluster not in clusters:
        console.print(f"Cluster {cluster} not found.", style="bold red", highlight=False)
        console.print(
            f"Hint: Try running dask [b i]databricks logs ls {path}[/b i] to list clusters.",
            style="bright_black",
            highlight=False,
        )
        sys.exit(1)

    most_recent = sorted(clusters[cluster].keys())[-1]

    console.print(f"Cluster: {cluster}", style="bold cyan", highlight=False)
    console.print(f"Start time: {_prettify_launch_time(most_recent)}", style="bold cyan", highlight=False)

    for i, node in enumerate(clusters[cluster][most_recent]):
        for channel in ["stdout", "stderr"]:
            for line in dbutils.fs.head(clusters[cluster][most_recent][node][channel], 65536).split("\n"):
                node_colour = _get_node_color(i)
                console.print(
                    f"[{node_colour}]{node}[/{node_colour}]: {line}", style="grey89" if channel == "stdout" else "plum4"
                )


if __name__ == "__main__":
    main()
