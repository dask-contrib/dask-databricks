import click

@click.group
def main():
    """Subcommands to launch Dask on Databricks."""

@main.command()
def run():
    """Run a databricks cluster"""
    print("I am going to run a cluster.")

if __name__ == "__main__":
    main()
