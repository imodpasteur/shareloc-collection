import requests
import yaml


def update_from_zenodo():
    with open("collection.yaml", "rb") as f:
        collection = yaml.safe_load(f.read())
    items = collection["collection"]
    old_dois = [item["doi"] for item in items]
    new_dois = []
    for page in range(1, 1000):
        zenodo_request = f"https://zenodo.org/api/records?sort=newest&page={page}&size=100&all_versions=1&q=keywords:shareloc.xyz"
        r = requests.get(zenodo_request)
        if not r.status_code == 200:
            print(
                f"Could not get zenodo records page {page}: {r.status_code}: {r.reason}"
            )
            break

        print(f"Collecting items from zenodo: {zenodo_request}")

        hits = r.json()
        if not hits:
            break

        for hit in hits:
            new_dois.append(hit["doi"])
            if (
                hit["doi"] in old_dois
                or ("relations" in hit["metadata"] and not hit["metadata"]["relations"]["version"][0]["is_last"])
            ):
                continue
            file_base_url = hit['links']['files']
            rdf_urls = [
                file_base_url + '/' + file_hit["filename"] + '/content'
                for file_hit in hit["files"]
                if file_hit["filename"] == "rdf.yaml"
            ]
            if len(rdf_urls) == 0:
                continue
            item = {
                "id": hit["conceptrecid"],
                "doi": hit["doi"],
                "rdf_source": sorted(rdf_urls)[0],
                "name": hit["metadata"]["title"],
                "owners": [hit["owner"]],
            }
            old_items = list(filter(lambda x: x["id"] == item["id"], items))
            if len(old_items) > 0:
                old_item = old_items[0]
                # In case there are fields that are overwritten, we inherit them
                item.update(
                    {
                        k: old_item[k]
                        for k in old_item
                        if k not in ["id", "name", "rdf_source", "doi", "owners"]
                    }
                )
                # Remove old item with the same id
                items = [x for x in items if x["id"] != item["id"]]
            items.append(item)

    # Remove item from collection if the doi does not exist any more in new_dois
    clean_items = []
    for item in items:
        if item["doi"] in new_dois:
            clean_items.append(item)

    def sort_by_id(x):
        return -int(x["id"])

    clean_items.sort(key=sort_by_id)
    collection["collection"] = clean_items

    with open("collection.yaml", "wb") as f:
        f.write(yaml.dump(collection, encoding="utf-8"))


update_from_zenodo()
