from __future__ import annotations

import csv
from pathlib import Path

from harlequin import HarlequinCompletion


def _get_completions() -> list[HarlequinCompletion]:
    completions: list[HarlequinCompletion] = []

    # source: https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/keywords_r.html
    keyword_path = Path(__file__).parent / "keywords.tsv"
    with keyword_path.open("r") as f:
        keyword_reader = csv.reader(
            f,
            delimiter="\t",
        )
        _header = next(keyword_reader)
        for keyword, kind in keyword_reader:
            completions.append(
                HarlequinCompletion(
                    label=keyword.lower(),
                    type_label="kw",
                    value=keyword.lower(),
                    priority=100 if kind.startswith("reserved") else 1000,
                    context=None,
                )
            )

    return sorted(completions)
