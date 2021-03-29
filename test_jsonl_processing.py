import os

import pytest

from jsonl_processing import merge_sorted_jsonl_files

JSONL_TEST_LOG_PATH_1 = "test_log_a.jsonl"
JSONL_TEST_LOG_PATH_2 = "test_log_b.jsonl"
JSONL_TEST_RESULT_LOG_PATH = "test_out_log.jsonl"


@pytest.fixture()
def test_files():
    def write_content(content_1, content_2):
        with open(JSONL_TEST_LOG_PATH_1, "w") as data:
            data.write(content_1)

        with open(JSONL_TEST_LOG_PATH_2, "w") as data:
            data.write(content_2)

        return JSONL_TEST_LOG_PATH_1, JSONL_TEST_LOG_PATH_2, JSONL_TEST_RESULT_LOG_PATH

    yield write_content
    os.remove(JSONL_TEST_LOG_PATH_1)
    os.remove(JSONL_TEST_LOG_PATH_2)
    os.remove(JSONL_TEST_RESULT_LOG_PATH)


def test_both_files_is_empty(test_files):
    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files("", "")
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert reader.read() == ""


def test_one_of_the_files_is_empty(test_files):
    jsonl_content_1 = '{"timestamp": "2000-01-01 00:00:01"}\n'
    jsonl_content_2 = ""

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_1, jsonl_content_2
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert reader.read() == '{"timestamp": "2000-01-01 00:00:01"}\n'

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_2, jsonl_content_1
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert reader.read() == '{"timestamp": "2000-01-01 00:00:01"}\n'


def test_files_with_different_lengths(test_files):
    jsonl_content_1 = (
        '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
        '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
    )

    jsonl_content_2 = '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_1, jsonl_content_2
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
        )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_2, jsonl_content_1
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
        )


def test_all_timestamps_in_one_file_is_less_than_all_timestamps_in_second_file(
    test_files,
):
    jsonl_content_1 = (
        '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
        '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
    )

    jsonl_content_2 = (
        '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
        '{"timestamp": "2000-01-01 00:00:04", "message": "4"}\n'
    )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_1, jsonl_content_2
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
            '{"timestamp": "2000-01-01 00:00:04", "message": "4"}\n'
        )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_2, jsonl_content_1
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
            '{"timestamp": "2000-01-01 00:00:04", "message": "4"}\n'
        )


def test_timestamps_in_both_files_grows_in_turn(test_files):
    jsonl_content_1 = (
        '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
        '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
    )

    jsonl_content_2 = (
        '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
        '{"timestamp": "2000-01-01 00:00:04", "message": "4"}\n'
    )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_1, jsonl_content_2
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
            '{"timestamp": "2000-01-01 00:00:04", "message": "4"}\n'
        )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_2, jsonl_content_1
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
            '{"timestamp": "2000-01-01 00:00:04", "message": "4"}\n'
        )


def test_one_of_the_files_has_the_same_timestamps(test_files):
    jsonl_content_1 = (
        '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
        '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
    )

    jsonl_content_2 = (
        '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
        '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
    )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_1, jsonl_content_2
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
        )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_2, jsonl_content_1
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read() == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
            '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
            '{"timestamp": "2000-01-01 00:00:03", "message": "3"}\n'
        )


def test_both_files_have_the_same_timestamps(test_files):
    jsonl_content_1 = (
        '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
        '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
    )

    jsonl_content_2 = '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_1, jsonl_content_2
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read()
            == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
               '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
               '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
        )

    jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path = test_files(
        jsonl_content_2, jsonl_content_1
    )
    merge_sorted_jsonl_files(
        jsonl_file_path_1, jsonl_file_path_2, jsonl_result_file_path
    )
    with open(jsonl_result_file_path, "r") as reader:
        assert (
            reader.read()
            == '{"timestamp": "2000-01-01 00:00:01", "message": "1"}\n'
               '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
               '{"timestamp": "2000-01-01 00:00:02", "message": "2"}\n'
        )
