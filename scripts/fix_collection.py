import os
import asyncio
import logging
import sys
from pathlib import Path

import httpx
from hypha_rpc import connect_to_server
from dotenv import load_dotenv

# Define log file path
LOG_FILE_PATH = Path("fix_collection.log")

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
CONCURENT_TASKS = 10

async def fix_collection():
    server = await connect_to_server({
        "server_url": SERVER_URL, 
        "workspace": "shareloc-xyz", 
        "token": os.environ.get("WORKSPACE_TOKEN")
    })
    
    artifact_manager = await server.get_service("public/artifact-manager")

    # Get the collection
    try:
        collection = await artifact_manager.read("shareloc-collection")
        logger.info(f"Found collection: {collection['id']}")
    except Exception as e:
        logger.error(f"Failed to fetch collection: {e}")
        return
    
    # List all child artifacts
    try:
        children = await artifact_manager.list(parent_id=collection["id"], stage=None)
        logger.info(f"Found {len(children)} child artifacts")
    except Exception as e:
        # print stack trace
        logger.exception(f"Failed to list children: {e}")
        return
    
    # Create a semaphore to limit concurrent tasks
    semaphore = asyncio.Semaphore(CONCURENT_TASKS)
    
    async def fix_artifact(artifact):
        async with semaphore:
            try:
                # Get the full artifact
                full_artifact = await artifact_manager.read(artifact["id"])
                
                # Edit the artifact (no changes, just to trigger an update)
                logger.info(f"Editing artifact: {artifact['id']}")
                updated_artifact = await artifact_manager.edit(
                    artifact_id=artifact["id"],
                    manifest=full_artifact["manifest"],
                )
                
                # Commit the artifact
                logger.info(f"Committing artifact: {artifact['id']}")
                updated_artifact = await artifact_manager.commit(artifact_id=artifact["id"])
                assert len(updated_artifact["versions"]) >= 1, "No versions found"
                logger.info(f"Successfully fixed artifact: {artifact['id']}")
            except Exception as e:
                logger.error(f"Failed to fix artifact {artifact['id']}: {e}")
    
    # Create tasks for each child artifact
    tasks = [fix_artifact(child) for child in children]
    
    # Run tasks
    await asyncio.gather(*tasks)
    
    logger.info("Collection fix completed.")

if __name__ == "__main__":
    asyncio.run(fix_collection()) 