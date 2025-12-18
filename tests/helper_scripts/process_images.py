# tests/helper_scripts/process_images.py
from pathlib import Path

from PIL import Image


def process_single_image(orig_path: Path, save_dir: Path) -> Path:
    save_path = save_dir / orig_path.name

    with Image.open(orig_path) as img:
        data = list(img.getdata())
        width, height = img.size
        new_data = []

        for i in range(len(data)):
            current_r, current_g, current_b = data[i]

            total_diff = 0
            neighbor_count = 0

            for dx, dy in [(1, 0), (0, 1)]:
                x = (i % width) + dx
                y = (i // width) + dy

                if 0 <= x < width and 0 <= y < height:
                    neighbor_r, neighbor_g, neighbor_b = data[y * width + x]
                    diff = abs(current_r - neighbor_r) + abs(current_g - neighbor_g) + abs(current_b - neighbor_b)
                    total_diff += diff
                    neighbor_count += 1

            if neighbor_count > 0:
                edge_strength = total_diff // neighbor_count
                if edge_strength > 30:
                    new_data.append((255, 255, 255))
                else:
                    new_data.append((0, 0, 0))
            else:
                new_data.append((0, 0, 0))

        edge_img = Image.new("RGB", (width, height))
        edge_img.putdata(new_data)
        edge_img.save(save_path)

    print(f"Processed {orig_path} and saved to {save_path}")  # noqa: T201
    return save_path
