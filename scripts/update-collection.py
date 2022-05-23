import requests
import yaml

def update_from_zenodo():
    items = []
    for page in range(1, 1000):
        zenodo_request = f"https://zenodo.org/api/records/?&sort=mostrecent&page={page}&size=1000&all_versions=1&keywords=shareloc.xyz"
        r = requests.get(zenodo_request)
        if not r.status_code == 200:
            print(f"Could not get zenodo records page {page}: {r.status_code}: {r.reason}")
            break

        print(f"Collecting items from zenodo: {zenodo_request}")

        hits = r.json()["hits"]["hits"]
        if not hits:
            break

        for hit in hits:
            # created = datetime.fromisoformat(hit["created"]).replace(tzinfo=None)
            # assert isinstance(created, datetime), created
            # resource_path = collection / resource_doi / "resource.yaml"
            # resource_output_path = dist / resource_doi / "resource.yaml"
            # version_name = f"version {hit['metadata']['relations']['version'][0]['index'] + 1}"
            rdf_urls = [file_hit["links"]["self"] for file_hit in hit["files"] if file_hit["key"] == "rdf.yaml"]
            item = {
                "id": hit["conceptrecid"],
                "doi": hit["doi"],
                "rdf_source": sorted(rdf_urls)[0],
                "name": hit["metadata"]["title"],
            }
            items.append(item)

    with open("collection.yaml", "rb") as f:
        collection = yaml.safe_load(f.read())
    collection["collection"] = items
    
    with open("collection.yaml", "wb") as f:
        f.write(yaml.dump(collection, encoding='utf-8'))
    
update_from_zenodo()