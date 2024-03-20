import click

from cli.statistic_score_data import statistic_scores


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(statistic_scores, 'statistic_scores')