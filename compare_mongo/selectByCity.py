import json
import time

import pymongo



countyJson = "../compare/counties_beijing.geojson"


def from_counties_beijing(file):
    with open(file, "r", encoding="utf-8") as f:
        line = f.readline().strip()
        while line != "":
            line = json.loads(line)
            county_adcode = line["features"][0]["properties"]["adcode"]
            county_name = line["features"][0]["properties"]["name"]
            geojson = line["features"][0]["geometry"]
            line = f.readline().strip()
            yield [county_adcode, county_name, geojson]


def test(cursor, table_name, county_adcode, county_name, geojson):
    # s = {
    #     "geometry": {
    #         "$geoIntersects": {
    #             "$geometry": "geometry_name"
    #         }
    #     }
    # }
    s = {
        "properties.city": {
            "$eq": "",
        },
        "geometry": {
            "$geoIntersects": {
                "$geometry": "geometry_name"
            }
        }
    }

    geojson = json.dumps(geojson)
    start = time.time()
    s["geometry"]["$geoIntersects"].update({"$geometry": geojson})
    s["properties.city"].update({"$eq": county_name})
    _sql = s
    cursor.find(_sql)
    num = cursor.count_documents({})
    end = time.time()
    with open("mongo_res_city_geometry_index.txt", "a+", encoding="utf-8") as f:
        res_str = f"""{county_name}-----{county_adcode}-----{table_name}-----{num.__str__()}-----{end - start}\n"""
        print(res_str)
        f.write(res_str)


def test_postgresql1():
    """
    测试各个城区，加索引
    :return:
    """
    datas = from_counties_beijing(countyJson)
    myclient = pymongo.MongoClient(f"mongodb://192.168.23.10:27017/")
    mydb = myclient["test_beijing"]
    tables_nums = [6000000, 20000000, 40000000, 80000000]
    for data in datas:
        for tables_num in tables_nums:
            tables = f"test_building_beijing_{tables_num}"
            cursor = mydb[tables]
            data.insert(0, tables)
            data.insert(0, cursor)
            test(*data)
            data.pop(0)
            data.pop(0)



if __name__ == '__main__':
    test_postgresql1()
