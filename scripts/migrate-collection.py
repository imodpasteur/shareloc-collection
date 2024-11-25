

import os
import asyncio
import logging
import sys
from pathlib import Path

import httpx
import yaml
from hypha_rpc import connect_to_server
from dotenv import load_dotenv


# Define log file path
LOG_FILE_PATH = Path("migration.log")

# Create a logger
logger = logging.getLogger("artifact")
logger.setLevel(logging.INFO)

# Formatter for log messages
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# File handler
file_handler = logging.FileHandler(LOG_FILE_PATH)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


load_dotenv()

SERVER_URL = "https://hypha.aicell.io"
COLLECTION_YAML_URL = "https://raw.githubusercontent.com/imodpasteur/shareloc-collection/refs/heads/gh-pages/collection.yaml"
DEFAULT_TIMEOUT = 20
CONCURENT_TASKS = 10

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger("artifact")
logger.setLevel(logging.INFO)

async def fetch_collection_yaml():
    async with httpx.AsyncClient(headers={"Connection": "close"}) as client:
        response = await client.get(COLLECTION_YAML_URL)
        assert response.status_code == 200, f"Failed to fetch collection.yaml from {COLLECTION_YAML_URL}"
        return yaml.safe_load(response.text)

async def download_manifest(rdf_source):
    async with httpx.AsyncClient(headers={"Connection": "close"}) as client:
        response = await client.get(rdf_source)
        assert response.status_code == 200, f"Failed to fetch manifest from {rdf_source}"
        return yaml.safe_load(response.text.replace("!<tag:yaml.org,2002:js/undefined>", ""))
    

async def download_file(url, dest_path, max_retries=5, retry_delay=5):
    """Download a file with retry logic for handling rate limits (HTTP 429)."""
    retries = 0
    while retries < max_retries:
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers={"Connection": "close"}) as client:
                response = await client.get(f"{url}/content")
                if response.status_code == 200:
                    with open(dest_path, 'wb') as f:
                        f.write(response.content)
                    return True
                elif response.status_code == 429:  # Too Many Requests
                    logger.warning(f"Rate limit hit for {url}, retrying after {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.warning(f"Failed to download {url}, status code: {response.status_code}")
                    return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
        retries += 1
        if retries < max_retries:
            await asyncio.sleep(retry_delay * retries)  # Exponential backoff
    logger.error(f"Failed to download {url} after {max_retries} retries.")
    return False

async def upload_file(artifact_manager, artifact_id, base_url, file_path, file_keys, max_retries=5, retry_delay=5, download_weight=0):
    """Modified upload_file function to include retry logic."""
    file_path = file_path.lstrip("./")
    if file_path not in file_keys:
        logger.warning(f"File {file_path} not found in {base_url}")
        return

    file_url = f"{base_url}/{file_path}/content"
    try:
        file_url = await artifact_manager.get_file(
            artifact_id=artifact_id,
            file_path=file_path
        )
        logger.info(f"File {file_path} already exists in {artifact_id}")
        return
    except Exception:
        logger.info(f"Uploading {file_path} from {file_url}")
    put_url = await artifact_manager.put_file(
        artifact_id=artifact_id,
        file_path=file_path,
        download_weight=download_weight,
    )
    retries = 0
    while retries < max_retries:
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers={"Connection": "close"}) as client:
                async with client.stream("GET", file_url) as response:
                    if response.status_code == 200:
                        headers = {"Connection": "close"}
                        if "Content-Length" in response.headers:
                            headers["Content-Length"] = response.headers["Content-Length"]
                        upload_response = await client.put(put_url, data=response.aiter_bytes(), headers=headers)
                        if upload_response.status_code == 200:
                            logger.info(f"Uploaded {artifact_id}: {file_path}")
                            return
                        elif response.status_code == 429:  # Too Many Requests
                            logger.warning(f"Rate limit hit for {file_url}, retrying after {retry_delay} seconds...")
                            await asyncio.sleep(retry_delay)
                        else:
                            logger.warning(f"Failed to upload {artifact_id}: {file_path}, status code: {upload_response.status_code}, {upload_response.text}")
                            return
                    else:
                        logger.exception(f"Failed to download {file_url}, status code: {response.status_code}")
                        return
        except httpx.ReadTimeout:
            logger.warning(f"Failed to upload {artifact_id}: {file_path}, read timeout")
        except Exception as e:
            logger.error(f"Error uploading {file_path}: {e}")
        retries += 1
        if retries < max_retries:
            await asyncio.sleep(retry_delay * retries)  # Exponential backoff
    logger.error(f"Failed to upload {artifact_id}: {file_path} after {max_retries} retries.")


async def upload_files(artifact_manager, artifact_id, base_url, documentation, covers, attachments):
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers={"Connection": "close"}) as client:
        response = await client.get(base_url)
        assert response.status_code == 200, f"Failed to fetch {base_url}"
        data = response.json()
        entries = data['entries']
        file_keys = [entry['key'] for entry in entries]

    # Upload README
    if documentation:
        await upload_file(artifact_manager, artifact_id, base_url, documentation, file_keys)

    # Upload cover images
    for cover in covers:
        await upload_file(artifact_manager, artifact_id, base_url, cover, file_keys)

    # Upload samples
    for sample in attachments.get('samples', []):
        sample_name = sample.get('name')
        for file_info in sample.get('files', []):
            file = f"{sample_name}/{file_info['name']}"
            await upload_file(artifact_manager, artifact_id, base_url, file, file_keys)

    logger.info(f"Uploaded all files for {artifact_id}")

async def migrate_collection(skip_migrated):
    server = await connect_to_server({"server_url": SERVER_URL, "workspace": "shareloc-xyz", "token": os.environ.get("WORKSPACE_TOKEN")})
    artifact_manager = await server.get_service("public/artifact-manager")

    # Fetch collection YAML
    collection_yaml = await fetch_collection_yaml()
    if not collection_yaml:
        logger.info("Failed to fetch collection.yaml.")
        return

    # Create the new collection
    new_collection_manifest = {
        "name": "ShareLoc.XYZ",
        "description": "ShareLoc.XYZ -- A Repository for Sharing Single Molecular Localization Microscopy Data",
    }
    
    assert os.environ.get("S3_ENDPOINT_URL"), "S3_ENDPOINT_URL is not set"
    assert os.environ.get("S3_ACCESS_KEY_ID"), "S3_ACCESS_KEY_ID is not set"
    assert os.environ.get("S3_SECRET_ACCESS_KEY"), "S3_SECRET_ACCESS_KEY is not set"
    assert os.environ.get("SANDBOX_ZENODO_ACCESS_TOKEN"), "SANDBOX_ZENODO_ACCESS_TOKEN is not set"
    assert os.environ.get("ZENODO_ACCESS_TOKEN"), "ZENODO_ACCESS_TOKEN is not set"
    
    collection = await artifact_manager.create(
        alias="shareloc-collection",
        type="collection",
        manifest=new_collection_manifest,
        config={"permissions": {"*": "r", "@": "r+"}},
        secrets={
            "SANDBOX_ZENODO_ACCESS_TOKEN": os.environ.get("SANDBOX_ZENODO_ACCESS_TOKEN"),
            "ZENODO_ACCESS_TOKEN": os.environ.get("ZENODO_ACCESS_TOKEN"),
            "S3_ENDPOINT_URL": os.environ.get("S3_ENDPOINT_URL"),
            "S3_ACCESS_KEY_ID": os.environ.get("S3_ACCESS_KEY_ID"),
            "S3_SECRET_ACCESS_KEY": os.environ.get("S3_SECRET_ACCESS_KEY"),
            "S3_REGION_NAME": os.environ.get("S3_REGION_NAME"),
            "S3_BUCKET": os.environ.get("S3_BUCKET"),
        },
        publish_to="sandbox_zenodo",
        overwrite=True
    )
    
    collection = await artifact_manager.read("shareloc-collection")
    collection_manifest = collection["manifest"]
    print(f"Collection created: {collection_manifest}")

    # Create a semaphore to limit concurrent tasks
    semaphore = asyncio.Semaphore(CONCURENT_TASKS)  # Limit to CONCURENT_TASKS concurrent tasks

    async def migrate_dataset(item, skip_migrated):
        async with semaphore:
            dataset_id = item["id"]
            base_url = item["rdf_source"].replace('/rdf.yaml/content', '')

            try:
                # Download full manifest
                full_manifest = await download_manifest(item["rdf_source"])
            except Exception:
                logger.error(f"Failed to fetch manifest for {dataset_id}")
                return
            if not full_manifest:
                logger.info(f"Failed to fetch manifest for {dataset_id}")
                return
            full_manifest.update(item)
            
            try:
                artifact = await artifact_manager.read(dataset_id)
            except Exception:
                pass
            else:
                artifact = await artifact_manager.edit(
                    type="dataset",
                    artifact_id=artifact.id,
                    manifest=full_manifest,
                )
                if skip_migrated:
                    logger.info(f"Dataset {dataset_id} already migrated.")
                    return
            # Create child artifact (dataset)
            artifact = await artifact_manager.create(
                type="dataset",
                alias=dataset_id,
                parent_id="shareloc-xyz/shareloc-collection",
                manifest=full_manifest,
                version="stage",
                overwrite=True
            )

            # Upload files (covers, attachments)
            await upload_files(
                artifact_manager=artifact_manager,
                artifact_id=artifact.id,
                base_url=base_url,
                documentation=item.get('documentation', ''),
                covers=item.get('covers', []),
                attachments=full_manifest.get('attachments', {})
            )

            # Commit the artifact
            await artifact_manager.commit(artifact_id=artifact.id)
            logger.info(f"Dataset {dataset_id} migrated.")

    # Create a list of tasks
    tasks = [migrate_dataset(item, skip_migrated) for item in collection_yaml["collection"]]
    
    # Run tasks and wait for them to complete
    await asyncio.gather(*tasks)

    logger.info("Migration completed.")

# await migrate_collection(skip_migrated=False)
asyncio.run(migrate_collection(skip_migrated=False))
