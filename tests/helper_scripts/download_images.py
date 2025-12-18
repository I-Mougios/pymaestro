# tests/helper_scripts/download_images.py
import asyncio
import time
from pathlib import Path
from typing import Iterable

import requests  # type: ignore[import-untyped]

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


def download_single_image(url: str, idx: int, download_dir: Path) -> Path:
    print(f"Downloading image {idx} from {url}")  # noqa: T201
    ts = int(time.time())
    url = f"{url}?{ts=}"
    response = requests.get(url, allow_redirects=True, timeout=5)
    response.raise_for_status()

    download_path = download_dir / f"image_{idx}.png"

    # print(f"Start writing to {download_path}")
    with download_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Image {idx} saved to {download_path}")  # noqa: T201
    return download_path


async def download_images(urls: Iterable[str], download_dir: Path) -> Iterable[Path]:
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(asyncio.to_thread(download_single_image, url, idx, download_dir))
            for idx, url in enumerate(urls)
        ]
        # async.TaskGroup context manager executes the tasks on exiting

    downloaded_images = [task.result() for task in tasks]
    print(f"Downloaded {len(downloaded_images)} images")  # noqa: T201
    return downloaded_images


def remove_dir(path: Path) -> None:
    for childpath in path.iterdir():
        if childpath.is_dir():
            remove_dir(childpath)
        else:
            childpath.unlink()

    path.rmdir()


if __name__ == "__main__":
    download_folder = Path(__file__).parent / "downloads_folder"
    download_folder.mkdir(parents=True, exist_ok=True)
    asyncio.run(download_images(IMAGE_URLS, download_folder))
    remove_dir(download_folder)
