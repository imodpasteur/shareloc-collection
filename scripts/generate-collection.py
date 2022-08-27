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

assert S3_ENDPOINT is not None, "S3_ENDPOINT must be set"

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
    "owners",
    "conversions",
]


def convert_formats(rdf, dataset_dir, force=False, potree=False, csv=False):
    if not potree and not csv:
        return
    attachments = rdf["attachments"]
    rdf_url = rdf["rdf_source"]
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )
    rdf["conversions"] = {}
    for sample in attachments["samples"]:
        conversions = {}
        rdf["conversions"][sample["name"]] = conversions
        for file in sample.get("files", []):
            if file["name"].endswith(".smlm"):
                conversions[file["name"]] = {}
                file_path = os.path.join(
                    dataset_dir, rdf["doi"], sample["name"], file["name"]
                )
                os.makedirs(
                    os.path.join(dataset_dir, rdf["doi"], sample["name"]), exist_ok=True
                )
                sample_path = os.path.join(rdf["doi"], sample["name"])

                response = s3_client.list_objects(
                    Bucket=S3_BUCKET,
                    Prefix=S3_DATA_DIR + "/" + os.path.join(sample_path) + "/",
                )
                existing_files = [f["Key"] for f in response["Contents"]] if "Contents" in response else []
                if potree:
                    potree_files = sorted(
                        [
                            os.path.basename(f)
                            for f in existing_files
                            if f.endswith(".potree.zip")
                        ]
                    )
                    if not potree_files or force:
                        if not os.path.exists(file_path):
                            # download the file
                            download_url(
                                resolve_url(
                                    rdf_url, sample["name"] + "/" + file["name"]
                                ),
                                file_path,
                            )
                        print("Converting " + file_path + " to potree...")
                        files = convert_potree(file_path, True)
                        potree_files = []
                        for potree_file_path in files:
                            object_name = (
                                S3_DATA_DIR
                                + "/"
                                + os.path.join(
                                    sample_path, os.path.basename(potree_file_path)
                                )
                            )
                            print("Uploading " + potree_file_path + " to s3...")
                            s3_client.upload_file(
                                potree_file_path, S3_BUCKET, object_name
                            )
                            print("potree file uploaded successfully")
                            os.remove(potree_file_path)
                            potree_files.append(os.path.basename(object_name))

                    conversions[file["name"]]["potree"] = potree_files
                if csv:
                    csv_files = sorted(
                        [
                            os.path.basename(f)
                            for f in existing_files
                            if f.endswith(".csv")
                        ]
                    )
                    if not csv_files or force:
                        if not os.path.exists(file_path):
                            # download the file
                            download_url(
                                resolve_url(
                                    rdf_url, sample["name"] + "/" + file["name"]
                                ),
                                file_path,
                            )
                        print("Converting " + file_path + " to csv...")
                        files = convert_smlm(file_path, delimiter=",", extension=".csv")
                        csv_files = []
                        for csv_file_path in files:
                            object_name = (
                                S3_DATA_DIR
                                + "/"
                                + os.path.join(
                                    sample_path, os.path.basename(csv_file_path)
                                )
                            )
                            print("Uploading " + csv_file_path + " to s3...")
                            s3_client.upload_file(csv_file_path, S3_BUCKET, object_name)
                            print("csv file uploaded successfully")
                            os.remove(csv_file_path)
                            csv_files.append(os.path.basename(object_name))
                    conversions[file["name"]]["csv"] = csv_files
                # Remove the folder
                shutil.rmtree(os.path.join(dataset_dir, rdf["doi"]))


def generate_collection(potree=False, csv=False, force=False):
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
        if potree or csv:
            convert_formats(rdf, "datasets", force, potree, csv)
        summary = {k: v for k, v in rdf.items() if k in SUMMARY_FIELDS}
        rdfs.append(summary)

    print(f"Generating collection.json for {len(rdfs)} items...")

    def sort_by_id(x):
        return -int(x["id"])

    rdfs.sort(key=sort_by_id)

    collection["collection"] = rdfs
    os.makedirs("dist", exist_ok=True)
    json.dump(collection, open("dist/collection.json", "w"))
    with open("dist/collection.yaml", "wb") as f:
        f.write(yaml.dump(collection, encoding="utf-8"))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--potree", action="store_true", help="Convert to potree and upload"
    )
    parser.add_argument("--csv", action="store_true", help="Convert to csv and upload")
    parser.add_argument(
        "--force", action="store_true", help="Force regenerate and upload"
    )

    args = parser.parse_args()

    generate_collection(potree=args.potree, csv=args.csv, force=args.force)
