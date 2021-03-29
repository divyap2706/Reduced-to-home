import click
import json
from . import constants as daily_constants
from ..request_lib import get_data


@click.command()
@click.option("--county", default=None, type=click.STRING, help="The AQS county code")
@click.option("--state", required=True, type=click.INT, help="The AQS state code")
@click.option(
    "--edate",
    required=True,
    type=click.STRING,
    help="The end date in the format yyyyMMdd",
)
@click.option(
    "--bdate",
    required=True,
    type=click.STRING,
    help="The start date in the format yyyyMMdd",
)
@click.option(
    "--param", "-p", required=True, type=click.INT, help="The AQS Parameter ID"
)
@click.pass_context
def daily_data(ctx, param, bdate, edate, state, county):
    click.echo("\n\nRetrieving daily data")
    if ctx.obj.get("debug"):
        click.echo(f"Parameter: {param}")
        click.echo(f"bdate: {bdate}")
        click.echo(f"edate: {edate}")
        click.echo(f"state: {state}")
        click.echo(f"county: {county}")

    uri = daily_constants.DAILY_URI + (
        daily_constants.COUNTY_URI if county else daily_constants.STATE_URI
    )
    if ctx.obj.get("debug"):
        click.echo(f"daily uri: {uri}")
    param_dict = {"param": param, "bdate": bdate, "edate": edate, "state": state}
    if county:
        param_dict["county"] = county

    data = get_data(
        uri,
        email=ctx.obj.get("email"),
        api_key=ctx.obj.get("api_key"),
        params=param_dict,
    )

    if ctx.obj.get("pretty_print"):
        data = json.dumps(data, indent=2)

    click.echo(f"dailyData retrieved data:\n{data}")
