# maestro/docs/01_example/decorator_api.py
import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable

import httpx
from common import IMAGE_URLS, download_image, remove_background

from pymaestro import DependsOn, Maestro

maestro = Maestro()

base_dir = Path(__file__).parent
raw_images = base_dir / "raw_images"


@maestro.add(args=(IMAGE_URLS,))
async def download_images(
    urls: Iterable[str],
    download_dir: Path = None,
    semaphore_limit: int = 4,
) -> list[Path]:
    semaphore = asyncio.Semaphore(semaphore_limit)

    async with httpx.AsyncClient() as client:
        try:
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(
                        download_image(client=client, url=url, idx=i, semaphore=semaphore, save_dir=download_dir)
                    )
                    for i, url in enumerate(urls, start=1)
                ]
        except* Exception as eg:
            # TaskGroup bundles failures into ExceptionGroup
            print("One or more downloads failed:")  # noqa: T201
            for exc in eg.exceptions:
                print(f"  - {exc!r}")  # noqa: T201
            raise

    return [task.result() for task in tasks]


@maestro.add(args=(DependsOn("download_images"),))
def remove_background_many(image_paths: Iterable[Path]) -> Iterable[Path]:
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results_iter = executor.map(remove_background, image_paths)

    return list(results_iter)


maestro.add("cleanup.py", job_type="script", name="cleanup")

if __name__ == "__main__":
    maestro.execute()
