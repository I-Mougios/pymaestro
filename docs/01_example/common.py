# maestro/docs/01_example/common.py
import asyncio
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable

import aiofiles
import httpx
from PIL import Image
from rembg import remove

IMAGE_URLS = [
    "https://images.unsplash.com/photo-1516117172878-fd2c41f4a759?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1532009324734-20a7a5813719?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1524429656589-6633a470097c?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1530224264768-7ff8c1789d79?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1564135624576-c5c88640f235?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1541698444083-023c97d3f4b6?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1522364723953-452d3431c267?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1493976040374-85c8e12f0c0e?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1530122037265-a5f1f91d3b99?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1516972810927-80185027ca84?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1550439062-609e1531270e?w=1920&h=1080&fit=crop",
    "https://images.unsplash.com/photo-1549692520-acc6669e2f0c?w=1920&h=1080&fit=crop",
]


async def download_image(client, url, idx, semaphore, save_dir=None) -> Path:
    if save_dir is None:
        save_dir = Path(__file__).parent / "raw_images"
        save_dir.mkdir(parents=True, exist_ok=True)

    async with semaphore:
        print(f"{idx} --> Downloading {url}")  # noqa: T201
        response = await client.get(url, follow_redirects=True, timeout=3)
        response.raise_for_status()

        out_file = save_dir / f"image_{idx}.png"
        out_file.touch()
        async with aiofiles.open(out_file, "wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=8192):
                await f.write(chunk)

    return out_file


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


def remove_background(image_path: Path, save_dir: Path | None = None) -> Path:
    print(f"--> Removing background: {image_path}")  # noqa: T201
    if save_dir is None:
        save_dir = Path(__file__).parent / "processed_images"
        save_dir.mkdir(parents=True, exist_ok=True)

    image = Image.open(str(image_path))
    processed_image = remove(image)
    out_path = save_dir / image_path.name
    processed_image.save(str(out_path))
    return out_path


def remove_background_many(image_paths: Iterable[Path], save_dir: Path | None = None) -> Iterable[Path]:
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results_iter = executor.map(remove_background, image_paths)

    return list(results_iter)
