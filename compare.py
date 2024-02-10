import json
def json_save(data, path):
    with open(path, 'w', encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def json_read(path):
    with open(path, 'r', encoding="utf-8") as f:
        return json.load(f)





res = []
for i in range(1, 6):
    data = json_read(f"raw_json/{i}_page.json")
    for i in data:
        res.append(i)

json_save(res, 'result.json')