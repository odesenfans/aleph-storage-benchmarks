"""
Generates N files of random size between the specified limits.
"""

import argparse
import asyncio
import os
import random
from hashlib import sha256
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket


async def generate_file(output_dir: Path, size: int, gridfs: AsyncIOMotorGridFSBucket) -> Path:
    content = os.urandom(size)
    h = sha256(content)
    file_path = output_dir / h.hexdigest()
    file_path.write_bytes(content)

    await gridfs.upload_from_stream(file_path.name, content)

    return file_path


def parse_size(size_str: str) -> int:
    def split_str():
        suffix_idx = -1
        for i in range(1, 4):
            if size_str[-i].isdigit():
                suffix_idx = -i + 1
                break

        return int(size_str[:suffix_idx]), size_str[suffix_idx:]

    multipliers = {"KB": 10**3, "KiB": 2**10, "MB": 10**6, "MiB": 2**20}

    base_value, suffix = split_str()
    multiplier = multipliers[suffix]
    return base_value * multiplier


def make_gridfs_client() -> AsyncIOMotorClient:
    connection = AsyncIOMotorClient("mongodb://localhost:27017", tz_aware=True)
    db = connection["gridfs_perf_tests"]
    return AsyncIOMotorGridFSBucket(db)


async def main(args: argparse.Namespace):
    min_size_str = args.min_size
    max_size_str = args.max_size
    nb_files = args.files

    gridfs = make_gridfs_client()

    output_dir: Path = args.output_dir
    summary_file = output_dir / "summary.txt"
    file_dir = output_dir / "files"
    file_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating {nb_files} files between {min_size_str} and {max_size_str}...")
    min_size = parse_size(min_size_str)
    print(f"{min_size_str} = {min_size}")
    max_size = parse_size(max_size_str)
    print(f"{max_size_str} = {max_size}")

    generated_files = []

    for i in range(1, nb_files + 1):
        file_size = int(random.uniform(min_size, max_size))
        file_path = await generate_file(file_dir, file_size, gridfs)
        print(f"{i}/{nb_files}: {file_path.name}")

        generated_files.append(file_path.name)

    print(f"Generating summary in {summary_file}...")

    with summary_file.open("w") as f:
        f.write("\n".join(generated_files))

    print("Done!")


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        "--output-dir", "-o",
        action="store",
        type=Path,
        required=False,
        default=Path.cwd() / "random_files",
        help="Output directory (default: './random_files').",
    )
    parser.add_argument(
        "--min-size",
        action="store",
        type=str,
        required=False,
        default="1KB",
        help="Minimum file size (default: 1KB).",
    )
    parser.add_argument(
        "--max-size",
        action="store",
        type=str,
        default="100KB",
        help="Maximum file size (default: 100KB).",
    )
    parser.add_argument("files", type=int, help="Number of files to generate")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(cli_parse()))
