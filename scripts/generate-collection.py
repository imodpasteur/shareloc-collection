import requests

import yaml
import json
import os
from tqdm import tqdm
from shareloc_utils.batch_download import (
    download_url,
    resolve_url,
    convert_potree,
    convert_smlm,
)
import boto3
import shutil
import random

S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_BUCKET = "public"
S3_DATA_DIR = "pointclouds"
S3_KEY = os.environ.get("S3_KEY")
S3_SECRET = os.environ.get("S3_SECRET")

S3_URL = f"{S3_ENDPOINT}/{S3_BUCKET}/{S3_DATA_DIR}"

SUMMARY_FIELDS = [
    "authors",
    "badges",
    "covers",
    "description",
    "download_url",
    "github_repo",
    "icon",
    "id",
    "license",
    "links",
    "name",
    "rdf_source",
    "source",
    "tags",
    "type",
    "doi",
]


def generate_potree(rdf, dataset_dir):
    attachments = rdf["attachments"]
    rdf_url = rdf["rdf_source"]
    for sample in attachments["samples"]:
        for file in sample.get("files", []):
            if file["name"].endswith(".smlm"):
                # TODO: check if the file is already exists in the s3
                zip_name = os.path.join(
                    rdf["doi"],
                    sample["name"],
                    file["name"].replace(".smlm", ".potree.zip"),
                )
                target_url = S3_URL + "/" + zip_name
                r = requests.head(target_url)
                if r.status_code == 200:
                    continue
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_KEY,
                    aws_secret_access_key=S3_SECRET,
                )
                object_name = S3_DATA_DIR + "/" + zip_name

                file_path = os.path.join(
                    dataset_dir, rdf["doi"], sample["name"], file["name"]
                )
                os.makedirs(
                    os.path.join(dataset_dir, rdf["doi"], sample["name"]), exist_ok=True
                )
                # download the file
                download_url(
                    resolve_url(rdf_url, sample["name"] + "/" + file["name"]),
                    file_path,
                )
                print("Converting " + file_path + "...")
                convert_potree(file_path, True)

                potree_file_path = file_path.replace(".smlm", ".potree.zip")
                print("Uploading " + potree_file_path + " to s3...")
                s3_client.upload_file(potree_file_path, S3_BUCKET, object_name)

                # Remove the folder
                shutil.rmtree(os.path.join(dataset_dir, rdf["doi"]))
                print("Potree file generated successfully: " + target_url)


def generate_collection(potree=False):
    rdfs = []
    with open("collection.yaml", "rb") as f:
        collection = yaml.safe_load(f.read())
    items = collection["collection"]

    # Randomize to allow parallel processing
    random.shuffle(items)
    for item in tqdm(items):
        rdf_source = item["rdf_source"]
        if item.get("status") == "blocked":
            print(f"Skipping blocked item {item['doi']}: {item['name']}...")
            continue
        r = requests.get(rdf_source)
        if not r.status_code == 200:
            print(f"Could not get item {item['id']}: {r.status_code}: {r.reason}")
            break
        rdf = yaml.safe_load(
            r.text.replace("!<tag:yaml.org,2002:js/undefined>", ""),
        )
        rdf.update(item)
        if potree:
            generate_potree(rdf, "datasets")
        summary = {k: v for k, v in rdf.items() if k in SUMMARY_FIELDS}
        rdfs.append(summary)

    print(f"Generating collection.json for {len(rdfs)} items...")

    def sort_by_id(x):
        return -int(x['id'])

    rdfs.sort(key=sort_by_id)

    collection["collection"] = rdfs
    os.makedirs("dist", exist_ok=True)
    json.dump(collection, open("dist/collection.json", "w"))
    with open("dist/collection.yaml", "wb") as f:
        f.write(yaml.dump(collection, encoding="utf-8"))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--potree', action='store_true',
                        help='Convert to potree and upload')

    args = parser.parse_args()
    
    generate_collection(args.potree)
