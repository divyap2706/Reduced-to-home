import click
from lib import get_data


@click.command()
@click.option(
    "--out-file",
    "-o",
    default="./output.csv",
    help="Path to the output file csv."
)
def cli(out_file):
    data = get_data()
    if out_file:
        print(f"Outfile: {out_file}")

    print(f"results: {data}")


if __name__ == "__main__":
    cli()
