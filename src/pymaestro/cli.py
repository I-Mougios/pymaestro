# src/pymaestro/cli.py
import shlex
from pathlib import Path

from pymaestro import Maestro

maestro = Maestro()


def main():  # noqa: C901
    try:
        import click
    except ImportError:
        raise RuntimeError(
            "Click is required for the CLI. Install Maestro with CLI support:\n\n    pip install python-maestro[cli]\n"
        ) from None

    @click.group()
    def cli():
        pass

    @click.command
    @click.argument("executable", required=True)
    @click.argument(
        "_type",
        required=True,
        metavar="TYPE",
        type=click.Choice(["script", "callable", "async_callable"], case_sensitive=True),
    )
    @click.option("-n", "--name", default=None)
    @click.option("-p", "--parallel_group", default=None)
    @click.option("-a", "--args", type=str, multiple=True, default=None)
    def add(
        executable: str, _type: str, name: str | None, parallel_group: str | None, args: tuple[str, ...] | None
    ) -> None:
        """Add a new job"""
        if name is None and _type in ("callable", "async_callable"):
            name = executable.rsplit(".", maxsplit=1)[1]
        elif name is None and _type == "script":
            name = executable

        maestro.add(executable, job_type=_type, name=name, parallel_group=parallel_group, args=args)
        click.echo(f"Added job '{name}' ({_type}) from '{executable}'.")

    @click.command
    @click.argument("name_or_idx", required=True)
    @click.option(
        "--by-index", is_flag=True, default=False, help="Treat the argument as a job index instead of a name."
    )
    def remove(name_or_idx: str, by_index: bool) -> None:
        """Remove a scheduled job by name or index."""
        if by_index:
            maestro.registry.pop(int(float(name_or_idx)))
            click.echo(f"Removed job at position {name_or_idx}.")
        else:
            maestro.registry.remove(name_or_idx)
            click.echo(f"Removed job '{name_or_idx}'.")

    @click.command
    @click.option("-n", "--name", default=None)
    def show(name: str | None) -> None:
        """List scheduled jobs"""
        if name:
            job_idx = maestro.registry.index(name)
            job = maestro.registry[job_idx]
            click.echo(f"Job: {str(job.name)}\nInsertion Order: {job_idx}")
        else:
            grouped_jobs = maestro.registry.grouped_jobs.items()
            output = ""
            for job, priority in grouped_jobs:
                output += f"{priority}: {str(job)}\n"
                output += "--" * 10 + "\n"

            click.echo(output)

    @click.command
    def execute():
        """Execute all the scheduled job"""
        results = maestro.execute()
        click.echo("Results: ")
        for result in results:
            click.echo(f"  > {result}")

    @click.command
    @click.option("-p", "--path", default=None, help="Path to write the json string of JobRegistry. Default to console")
    def serialize_command(path: str | None) -> str | None:
        """
        Serialize the current state of JobRegistry
        """
        json_str = maestro.serialize(path)
        click.echo(json_str)

    @click.command
    @click.argument("path", type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path), required=True)
    def deserialize_command(path: str | None) -> None:
        """
        Initialize the current state of JobRegistry from a JSON string
        """
        maestro.deserialize(path)
        click.echo(maestro.registry.grouped_jobs)

    @click.command()
    def shell():
        """
        Open an interactive Maestro shell to allow chaining multiple commands.
        Type 'exit' or 'quit' to leave the shell.
        """
        import sys
        from pathlib import Path

        cwd = Path.cwd()
        sys.path.insert(0, str(cwd))
        click.echo("Entering Maestro shell. Type 'exit' or 'quit' to terminate the session.")
        while True:
            try:
                user_input = click.prompt("> maestro", prompt_suffix=" ").strip()
                if not user_input:
                    continue
                if user_input in ("exit", "quit"):
                    break

                args = shlex.split(user_input)

                cli.main(args=args, standalone_mode=False)
            except click.ClickException as e:
                e.show()
            except Exception as e:
                click.secho(f"Unexpected error: {e}", fg="red")

        click.echo("Exiting Maestro shell...")

    cli.add_command(shell)
    cli.add_command(add)
    cli.add_command(remove)
    cli.add_command(show)
    cli.add_command(execute)
    cli.add_command(serialize_command)
    cli.add_command(deserialize_command)

    return cli()


def entrypoint():
    return main()
