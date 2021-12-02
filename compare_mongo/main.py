import time

import pymongo
from pymongo import TEXT, GEOSPHERE


def cal_time(file):
    def decorator(func):
        def wrapper(*args, **kw):
            start = time.time()
            res = func(*args, **kw)
            end = time.time()
            with open(file, "a+", encoding="utf-8") as f:
                f.write(res)
                f.write("-----" + (end-start).__str__() + "\n")
            return
        return wrapper
    return decorator

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

@cal_time("mongo_create_index.txt")
def addIndex(table, key, index_type):
    myclient = pymongo.MongoClient(f"mongodb://192.168.23.10:27017/")
    mydb = myclient["test_beijing"]
    cursor = mydb[table]
    # print("begin")
    cursor.create_index([(key, index_type)], name=f"index_{key}")
    # cursor.close()
    # mydb.close()
    myclient.close()
    print(f"{table}-----{key}---{index_type}-----'index_'{key}")
    return f"{table}-----{key}---{index_type}-----'index_'{key}"

def create_index():
    # d = {"city": TEXT, "geometry": GEOSPHERE, "province": TEXT}
    d = {"city": TEXT, "geometry": GEOSPHERE}
    tables_nums = [6000000, 20000000, 40000000, 80000000]
    for tables_num in tables_nums:
        table = f"test_building_beijing_{tables_num}"
        for k in d.keys():
            key = k
            type = d.get(key)
            addIndex(table, key, type)

if __name__ == '__main__':
    create_index()
