import click


def main() -> None:  # pragma: no cover
    click.secho("Starting ...", fg="bright_blue")
    click.secho("Done", fg="green")


if __name__ == "__main__":  # pragma: no cover
    main()
