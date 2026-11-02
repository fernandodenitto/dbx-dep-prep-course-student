"""Format Lakeshore report rows for a notebook or a job log."""

from tabulate import tabulate


def revenue_table(rows):
    """Render (category, revenue) pairs as a GitHub-style text table."""
    return tabulate(rows, headers=["category", "revenue"], tablefmt="github")
