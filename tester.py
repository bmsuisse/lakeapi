import requests

user = "test"
pw = "B~C:BB*_9-1u"
auth = (user, pw)
engines = ["polars"]
client = requests.Session()
response = client.get("http://localhost:8080/metadata", auth=auth)
assert response.status_code == 200
jsd = response.json()
for item in jsd:
    for e in engines:
        name = item["name"]
        tag = item["tag"]
        route = item["route"]
        meta_detail_route = (
            "http://localhost:8080/" + route + f"/metadata_detail?%24engine={e}"
        )
        print(meta_detail_route)
        response = client.get(meta_detail_route, auth=auth)
        if name not in ["not_existing", "not_existing2"]:
            assert name + "_" + str(response.status_code) == name + "_200"
        else:
            assert name + "_" + str(response.status_code) == name + "_404"
