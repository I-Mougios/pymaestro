import os
from tempfile import NamedTemporaryFile

from pymaestro import Resource
from pymaestro.jobs import create_job


def open_file(mode="w"):
    f = NamedTemporaryFile(mode=mode, delete=False, delete_on_close=False)
    try:
        yield f
    finally:
        f.close()


def write_file(file, content) -> NamedTemporaryFile:
    file.write(content)
    return file


def test_inject_file_as_resource():
    job_1 = create_job(
        "callable",
        name="write_text_to_file",
        executable=write_file,
        args=(Resource(open_file, generator_kwargs={"mode": "w"}),),
        kwargs={"content": "This is the first line"},
    )
    f = job_1.execute()

    with open(f.name) as f:
        first_line = next(f)
        assert first_line.strip() == "This is the first line"

    os.remove(f.name)
