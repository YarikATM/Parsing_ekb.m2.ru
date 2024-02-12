import json
from main import normalize_time_publication
import re

def json_save(data, path):
    with open(path, 'w', encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def json_read(path):
    with open(path, 'r', encoding="utf-8") as f:
        return json.load(f)


data = json_read("result2.json")


for apart in data:
    # print(re.match(r"([0-9]){4}(-)([0-9]){2}(-)([0-9]){2}(T)([0-9]){2}(:)([0-9]){2}(:)([0-9]){2}(Z)", apart["date"]["publication_date"]))
    if not re.match(r"([0-9]){4}(-)([0-9]){2}(-)([0-9]){2}(T)([0-9]){2}(:)([0-9]){2}(:)([0-9]){2}(Z)", apart["date"]["publication_date"]):
        print(apart["date"]["publication_date"])
    #     apart["date"]["publication_date"] = normalize_time_publication(apart["date"]["publication_date"])
    #     print(apart["date"]["publication_date"])

json_save(data, "result2.json")