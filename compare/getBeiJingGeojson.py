import requests
import json
adcodes = [110101,
           110102,
           110105,
           110106,
           110107,
           110108,
           110109,
           110111,
           110112,
           110113,
           110114,
           110115,
           110116,
           110117,
           110118,
           110119, ]

url = "https://geo.datav.aliyun.com/areas_v3/bound/geojson"
with open("counties_beijing.geojson", "a+", encoding="utf-8") as f:
    for adcode in adcodes:
        params = {"code": adcode}
        res = requests.get(url, params=params)
        json.dump(res.json(), f)
        f.write("\n")

