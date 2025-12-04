# From https://github.com/prefix-dev/sharded-repo

import argparse
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import boto3
import msgpack
import requests
import zstandard as zstd
from botocore.exceptions import ClientError
from rich.progress import (
    BarColumn,
    Progress,
    TimeElapsedColumn,
    TransferSpeedColumn,
    track,
)

fake_token = {"token": "iamasecrettoken"}

COMPRESS_LEVEL = 1


def download_file(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raises an HTTPError for bad responses

    # Total size in bytes, might be None if the server doesn't provide it
    total_size = int(response.headers.get("content-length", 0)) or None

    # Initialize the progress bar
    with Progress(BarColumn(), TransferSpeedColumn(), TimeElapsedColumn()) as progress:
        task = progress.add_task("[cyan]Downloading...", total=total_size)
        file_content = bytearray()

        # Download the file in chunks
        for data in response.iter_content(chunk_size=4096):
            file_content.extend(data)
            progress.update(task, advance=len(data))

    if url.endswith(".zst"):
        file_content = zstd.decompress(file_content)

    return file_content


def sha256(data):
    hash = hashlib.sha256(data)
    return hash.digest(), hash.hexdigest()


def pack_package_record(record, run_exports=None):
    if sha256 := record.get("sha256"):
        record["sha256"] = bytes.fromhex(sha256)
    if md5 := record.get("md5"):
        record["md5"] = bytes.fromhex(md5)
    if run_exports:
        record["run_exports"] = run_exports["run_exports"]
    return record


def split_repo(repo_url, subdir, folder):
    repodata = folder / subdir / "repodata.json"
    run_exports = folder / subdir / "run_exports.json"

    if not repodata.parent.exists():
        repodata.parent.mkdir(parents=True)

    is_fast = any([x in repo_url for x in ("conda-forge", "bioconda")])

    if not repodata.exists():
        repo_url = repo_url.rstrip("/")
        if is_fast:
            response = download_file(f"{repo_url}/{subdir}/repodata.json.zst")
        else:
            response = download_file(f"{repo_url}/{subdir}/repodata.json")
        repodata.write_bytes(response)
    else:
        print(f"Skipping download of {subdir}/repodata.json. Using cached file.")

    if not run_exports.exists() and is_fast:
        response = download_file(f"{repo_url}/{subdir}/run_exports.json.zst")
        run_exports.write_bytes(response)
    else:
        print(f"Skipping download of {subdir}/run_exports.json. Using cached file.")

    return split_repo_file(repo_url, subdir, folder, repodata, run_exports)


def split_repo_file(repo_url, subdir, folder, repodata, run_exports):
    # Parse repodata.json and split into shards
    repodata = json.loads(repodata.read_text())
    if run_exports.exists():
        run_exports = json.loads(run_exports.read_text())
    else:
        run_exports = None
    packages = repodata["packages"]
    package_names = dict()
    for fn, package in packages.items():
        name = package["name"]
        if name not in package_names:
            package_names[name] = []
        package_names[name].append(fn)

    conda_packages = repodata["packages.conda"]
    conda_package_names = dict()
    for fn, package in conda_packages.items():
        name = package["name"]
        if name not in conda_package_names:
            conda_package_names[name] = []
        conda_package_names[name].append(fn)

    all_names = set(package_names.keys()) | set(conda_package_names.keys())

    # write out the shards into `folder/shards/<package_name>.json`
    shards = folder / subdir / "shards"
    if shards.exists():
        for file in shards.glob("*.msgpack.zst"):
            file.unlink()

    shards.mkdir(exist_ok=True)
    shards_index = {"info": repodata["info"], "shards": {}}
    shards_index["info"]["base_url"] = f"{repo_url}/{subdir}/"
    shards_index["info"]["shards_base_url"] = "./shards/"

    compressor = zstd.ZstdCompressor(level=COMPRESS_LEVEL)

    before = 0
    after_compression = 0

    # create a rich progress bar
    for name in track(all_names, description=f"Processing {subdir}"):
        if run_exports:
            run_exports_packages = run_exports.get("packages", {})
            run_exports_conda_packages = run_exports.get("packages.conda", {})
        else:
            run_exports_packages = {}
            run_exports_conda_packages = {}

        d = {"packages": {}, "packages.conda": {}}

        for fn in package_names.get(name, []):
            if run_exports and fn not in run_exports_packages:
                print(f"Missing run_exports for {fn}")

            d["packages"][fn] = pack_package_record(
                packages[fn], run_exports_packages.get(fn)
            )

        for fn in conda_package_names.get(name, []):
            if run_exports and fn not in run_exports_conda_packages:
                print(f"Missing run_exports for {fn}")

            d["packages.conda"][fn] = pack_package_record(
                conda_packages[fn], run_exports_conda_packages.get(fn)
            )

        encoded = msgpack.dumps(d)
        # encode with zstd
        compressed = compressor.compress(encoded)
        # use the sha hash of the compressed data as the filename
        digest, hexdigest = sha256(compressed)

        before += len(encoded)
        after_compression += len(compressed)

        shard = shards / f"{hexdigest}.msgpack.zst"
        shard.write_bytes(compressed)

        # store the byte digest of the shard / compressed data
        shards_index["shards"][name] = digest

    print("Before compression: ", before)
    print("After compression: ", after_compression)
    if before > 0:
        print("Percentage saved by zstd: ", (1 - after_compression / before) * 100, "%")

    # write a repodata_shards.json file that has an index of all the shards
    repodata_shards_file = folder / subdir / "repodata_shards.msgpack.zst"
    repodata_shards = compressor.compress(msgpack.dumps(shards_index))
    repodata_shards_file.write_bytes(repodata_shards)

    return package_names


s3_client = boto3.client(
    service_name="s3",
    endpoint_url="https://e1a7cde76f1780ec06bac859036dbaf7.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID", ""),
    aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY", ""),
    region_name="weur",
)


def upload(file_name: Path, bucket: str, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name.name

    if file_name.name.startswith("repodata"):
        cache = "public, max-age=60"
    else:
        cache = "public, max-age=31536000, immutable"

    try:
        response = s3_client.upload_file(
            file_name,
            bucket,
            object_name,
            ExtraArgs={
                "CacheControl": cache,
            },
        )
        print("Upload successful: ", object_name)
    except ClientError as e:
        print(e)
        return False
    return True


def files_to_upload(outpath, timestamp, subdir, channel_name):
    # first download current index file from the fast-repo
    index_file = outpath / "old" / timestamp / subdir / "repodata_shards.msgpack"
    index_file.parent.mkdir(parents=True, exist_ok=True)
    index_url = f"https://fast.prefix.dev/{channel_name}/{subdir}/repodata_shards.msgpack.zst?bust_cache={timestamp}"

    response = requests.get(index_url)
    files = []
    shard_hashes = set()
    if response.status_code == 200:
        # decode with zstd and msgpack
        decompressor = zstd.ZstdDecompressor()
        decompressed = decompressor.decompress(response.content)
        index_data = msgpack.loads(decompressed)
        index_file.write_bytes(decompressed)

        # find all shard hashes already in the index
        print("Reading shard hashes from index file")
        for name, shard in index_data["shards"].items():
            if isinstance(shard, bytes):
                shard: bytes = shard
                shard_hashes.add(shard.hex())
            else:
                shard_hashes.add(shard["sha256"])

    skipped = 0
    total = 0
    # Iterate over all files in the directory
    for file in (outpath / subdir).rglob("shards/*"):
        if file.is_file():
            # Skip the 'repodata.json' file
            if file.name.startswith("repodata.json"):
                continue

            # skip if we have the shard already
            filename = file.name
            # remove msgpack.zst extension
            if filename.endswith(".msgpack.zst"):
                filename = filename[:-12]

            total += 1
            if filename in shard_hashes:
                skipped += 1
                continue

            # Submit the 'upload' function to the executor for each file
            object_name = f"{channel_name}/{file.relative_to(outpath)}"
            print("Uploading: ", object_name)
            files.append((file, object_name))

    print(f"Skipped {skipped} out of {total} files")
    if total > 0:
        print(f"Percentage skipped: {skipped / total * 100}%")

    return files


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Process some integers.")

    # Add arguments
    parser.add_argument("--channel", type=str, help="The channel name")
    parser.add_argument(
        "--cache-dir", type=str, help="The cache directory to use", default="cache"
    )
    parser.add_argument(
        "--subdirs", type=str, nargs="+", help="List of subdirs to clone"
    )
    parser.add_argument(
        "--all-subdirs", help="Whether to clone all subdirs", action="store_true"
    )

    # Parse the arguments
    args = parser.parse_args()
    all_subdirs = [
        "noarch",
        "osx-arm64",
        "linux-64",
        "win-64",
        "osx-64",
        "linux-aarch64",
        "linux-ppc64le",
    ]
    subdirs = args.subdirs if not args.all_subdirs else all_subdirs

    final_subdirs = []
    for s in subdirs:
        if "," in s:
            final_subdirs.extend(s.split(","))
        else:
            final_subdirs.append(s)
    subdirs = final_subdirs

    channel_name = args.channel
    outpath = Path(args.cache_dir) / channel_name
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    for subdir in subdirs:
        channel_url = f"https://conda.anaconda.org/{channel_name}/"
        split_repo(channel_url, subdir, outpath)

        files = files_to_upload(outpath, timestamp, subdir, channel_name)

        with ThreadPoolExecutor(max_workers=50) as executor:
            for file, object_name in files:
                executor.submit(upload, file, "fast-repo", object_name=object_name)

        # Upload the index file
        upload(
            outpath / subdir / "repodata_shards.msgpack.zst",
            "fast-repo",
            f"{channel_name}/{subdir}/repodata_shards.msgpack.zst",
        )

        # Upload the fake token
        tempfile = outpath / subdir / "token"
        tempfile.write_text(json.dumps(fake_token))
        upload(
            tempfile,
            "fast-repo",
            f"{channel_name}/{subdir}/token",
        )
