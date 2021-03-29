# the script merges two jsonl files into one.
## jsonl file formats:
* ##### {"log_level": "DEBUG", "timestamp": "2022-07-01 18:36:38", "message": "some message"}
## Source files are sorted in ascending order by field "timestamp"
## The resulting file is also sorted in ascending order by field "timestamp"

# Running the tests
## You can launch the tests as follows:
* pytest path/test_jsonl_processing.py
