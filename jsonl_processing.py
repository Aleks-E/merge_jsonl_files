from datetime import datetime
from typing import Iterable, List, TextIO, Tuple, Union

import jsonlines


def create_pairs(
    *text_wrapers: Tuple[TextIO],
) -> Union[datetime.strptime, str, jsonlines.Reader(Iterable).read]:
    for text_wraper in text_wrapers:
        reader = jsonlines.Reader(text_wraper)
        try:
            row = reader.read()
            timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S") # noqa flake8 conflicts with black
            yield [timestamp, row, reader.read]
        except EOFError:
            ...


def write_next_min_item(
    packs: List[Union[datetime.strptime, str, jsonlines.Reader(Iterable).read]], # noqa flake8 conflicts with black
    text_writer: jsonlines.Writer,
) -> None:
    packs.sort(key=lambda x: x[0])
    min_item = packs[0]
    row, next_row = min_item[1::]
    text_writer.write(row)
    try:
        row = next_row()
        timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
        min_item[0:2] = timestamp, row
    except EOFError:
        packs.remove(min_item)


def merge_sorted_jsonl_files(
    input_file_path_1: str, input_file_path_2: str, result_file_path: str
) -> None:
    with open(input_file_path_1, "r") as data_read_1, open(
        input_file_path_2, "r"
    ) as data_read_2, open(result_file_path, "w") as data_write:
        pairs = list(create_pairs(data_read_1, data_read_2))
        writer = jsonlines.Writer(data_write)
        while pairs:
            write_next_min_item(pairs, writer)
