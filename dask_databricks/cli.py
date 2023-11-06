import click

@click.group(name="databricks")
def main():
    """Tools to launch Dask on Databricks."""

@main.command()
def run():
    """Run Dask processes on a Databricks cluster."""
    print("I am going to run a cluster.")

if __name__ == "__main__":
    main()
