import json

import pymongo

from decorators import cal_time
from data import process


## 对mongo建立索引

## 对mongo进行查询


@cal_time("mongo_query_test.txt")
def query(cursor, table_name, county_adcode, county_name, geojson, proportion):
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
    s["geometry"]["$geoIntersects"].update({"$geometry": geojson})
    s["properties.city"].update({"$eq": county_name})
    _sql = s
    print(_sql)
    cursor.find(_sql)
    num = cursor.count_documents({})
    res_str = f"""{county_name}-----{county_adcode}-----{table_name}-----{num.__str__()}-----{proportion}"""
    return res_str


def query_postgresql(file, file_type, tables_nums):
    """
    测试各个城区
    :return:
    """

    datas = process(file, file_type)
    myclient = pymongo.MongoClient(f"mongodb://127.0.01:27017/")
    mydb = myclient["test_beijing"]
    # tables_nums = [6000000, 20000000, 40000000, 80000000]

    i = 1
    for data in datas:
        print(i)
        i += 1
        for tables_num in tables_nums:
            tables = f"test_building_beijing_{tables_num}"
            cursor = mydb[tables]
            data.insert(0, tables)
            data.insert(0, cursor)
            query(*data)
            data.pop(0)
            data.pop(0)
    myclient.close()


def test_query_postgresql():
    tables_nums = [2000000, 4000000, 6000000, 8000000]
    file1 = "./data/5_1_朝阳区.geojson"
    file2 = "./data/counties_beijing.geojson"
    query_postgresql(file2, 1, tables_nums)
    query_postgresql(file1, 2, tables_nums)


def start():
    # tables_nums = [6000000, 20000000, 40000000, 80000000]
    tables_nums = [6000000]
    # tables_nums = [2000000, 4000000, 6000000, 8000000]
    file1 = "./data/5_1_朝阳区.geojson"
    file2 = "./data/10_1_朝阳区.geojson"
    file3 = "./data/20_1_朝阳区.geojson"
    file = "./data/counties_beijing.geojson"
    # addIndex_postgresql(tables_nums)
    query_postgresql(file1, 2, tables_nums)
    query_postgresql(file2, 2, tables_nums)
    query_postgresql(file3, 2, tables_nums)
    query_postgresql(file, 1, tables_nums)


if __name__ == '__main__':
    # test_query_postgresql()
    start()
