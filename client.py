import argparse
import asyncio
import random

from aiohttp import ClientSession
from pathlib import Path
from typing import List
import time

SUMMARY_FILE = Path(__file__).parent / "random_files" / "summary.txt"


def get_random_files(summary_file: Path, nb_files: int) -> List[str]:
    with summary_file.open() as f:
        files = [line.rstrip() for line in f.readlines()]

    return random.sample(files, nb_files)


async def get_file(client: ClientSession, endpoint: str, filename: str):
    async with client.get(f"/{endpoint}/{filename}") as response:
        response.raise_for_status()
        print(f"Retrieved {filename} successfully.")


async def main(args: argparse.Namespace):
    method = args.method
    nb_files = args.files
    files = get_random_files(SUMMARY_FILE, nb_files)

    start_time = time.perf_counter()

    async with ClientSession("http://localhost:8000/") as client:
        tasks = [get_file(client, method, filename) for filename in files]
        await asyncio.gather(*tasks)

    end_time = time.perf_counter()

    print(f"Fetched {nb_files} in {end_time - start_time} seconds.")


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--method",
        "-m",
        action="store",
        type=str,
        choices=("filesystem", "gridfs"),
        required=False,
        default="filesystem",
        help="Method to retrieve files (default: filesystem).",
    )
    parser.add_argument(
        "files",
        type=int,
        help="Number of files to generate",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(cli_parse()))
