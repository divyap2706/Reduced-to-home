import click

from .daily_data import command as daily


@click.group()
@click.option("--debug", default=False, is_flag=True, help="Turn on debug logging.")
@click.option(
    "--api-key",
    "-k",
    default=None,
    help="Specify the API Key for the supplied email address.",
)
@click.option(
    "--email", "-em", default=None, help="Specify the identifying email address."
)
@click.option(
    "--pretty-print",
    "-pp",
    is_flag=True,
    default=False,
    help="Prints console output nicely.",
)
@click.pass_context
def main(ctx, pretty_print, email, api_key, debug):
    """Entrypoint for the program"""
    ctx.ensure_object(dict)
    ctx.obj["pretty_print"] = pretty_print
    ctx.obj["email"] = email
    ctx.obj["api_key"] = api_key
    ctx.obj["debug"] = debug


main.add_command(daily.daily_data)
