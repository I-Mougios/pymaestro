# maestro/docs/01_example/cleanup.py
from pathlib import Path

base_dir = Path(__file__).resolve().parent
raw_images = base_dir / "raw_images"


def cleanup(input_directory: Path):
    print("Remove directory: {}".format(input_directory))  # noqa: T201
    for _path in raw_images.iterdir():
        if _path.is_dir():
            cleanup(_path)
        else:
            _path.unlink()

    input_directory.rmdir()


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    raw_images = base_dir / "raw_images"
    cleanup(raw_images)
