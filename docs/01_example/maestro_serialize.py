# maestro/docs/01_example/maestro_serialize.py
from pathlib import Path

from common import IMAGE_URLS, download_images, remove_background_many

from pymaestro import DependsOn, Maestro

base_dir = Path(__file__).parent
raw_images = base_dir / "raw_images"

maestro = Maestro()

maestro.add(download_images, job_type="async_callable", name="download_images", args=(IMAGE_URLS,))
maestro.add(remove_background_many, job_type="callable", args=(DependsOn("download_images"),))
maestro.add("cleanup.py", job_type="script")


if __name__ == "__main__":
    maestro.serialize(base_dir / "image_jobs.json")
