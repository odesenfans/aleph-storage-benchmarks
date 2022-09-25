from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse, Response
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket

GENERATED_FILES_DIR = Path(__file__).parent / "random_files" / "files"

app = FastAPI()
gridfs = None


def make_gridfs_client():
    connection = AsyncIOMotorClient("mongodb://localhost:27017", tz_aware=True)
    db = connection["gridfs_perf_tests"]
    return AsyncIOMotorGridFSBucket(db)


async def get_gridfs_client():
    global gridfs
    if gridfs is None:
        gridfs = make_gridfs_client()
    return gridfs


@app.get("/filesystem/{file_hash}", response_class=FileResponse)
async def get_from_filesystem(file_hash):
    return GENERATED_FILES_DIR / file_hash


@app.get("/gridfs/{file_hash}")
async def get_from_gridfs(file_hash, gridfs: AsyncIOMotorGridFSBucket = Depends(get_gridfs_client)):
    gridout = await gridfs.open_download_stream_by_name(file_hash)
    content = await gridout.read()
    return Response(content)


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
