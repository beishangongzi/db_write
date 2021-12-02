import datetime
import json
from multiprocessing import Process

import psycopg2
import pymongo

from checkData.process_data import process_data
import AMap_adcode_citycode
from utils.getRandomDates import getRandomDates, getRandomYear
from sql import createTable


def process_data_mongo(file, adcodes, time, p):
    return process_data(file, adcodes, time, p)


def insertIntoMongo(datas, time, nameP):
    # myclient = pymongo.MongoClient(f"mongodb://192.168.23.10:27017/")
    myclient = pymongo.MongoClient(f"mongodb://127.0.0.1:27017/")
    mydb = myclient["test_beijing"]

    name = f"test_building_beijing_{nameP * 2000000}"
    cursor = mydb[name]
    print("success")
    some_data = []
    i = 1
    for d in datas:
        some_data.append(d)
        if i % 10000 == 0:
            try:
                for one_some_data in some_data:
                    if "_id" in one_some_data:
                        del one_some_data["_id"]
                # print(some_data)
                cursor.insert_many(some_data)
                print("success: " + str(i))
            except Exception as e:
                # print(e)
                for one_data in some_data:
                    try:
                        cursor.insert_one(one_data)
                    except:
                        with open("error.txt", "a+", encoding="utf-8") as f:
                            f.write(one_data.__str__() + "\n")

            some_data = []
        if i > 100:
            break
        i += 1
    try:
        cursor.insert_many(some_data)
        print("success: " + str(i))
    except Exception as e:
        # print(e)
        for one_data in some_data:
            try:
                cursor.insert_one(one_data)
            except:
                with open("error.txt", "a+", encoding="utf-8") as f:
                    f.write(str(one_data) + "\n")



    print("finished")
    myclient.close()

def main(adcodes_file="adcode_view.csv", geojson_file="../fix_beijing.geojson", time=2, name=1):
    # 1. process data.
    adcodes = AMap_adcode_citycode.main.getAdcode(adcodes_file)
    p = 1 / time
    datas = process_data_mongo(geojson_file, adcodes, time, p)
    # 2. connect database.
    insertIntoMongo(datas, time, name)


if __name__ == '__main__':
    # geojsonFile = "/z/00STAFF/Lu Yifei/数据/bldg/fix_beijing.geojson"
    geojsonFile = "../fix_beijing.geojson"
    """500万、2000万、4000万、8000万"""
    # main(geojson_file=geojsonFile, time=1)
    for i in range(3):
        # Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 1, "name": 3}).start()
        main(geojson_file=geojsonFile, time=1, name=3)
    for i in range(10):
        main(geojson_file=geojsonFile, time=1, name=10)
        # Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 1, "name": 10}).start()

    for i in range(20):
        main(geojson_file=geojsonFile, time=1, name=20)
        # Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 20, "name": 20}).start()

    for i in range(40):
        main(geojson_file=geojsonFile, time=1, name=40)
        # Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 40, "name": 40}).start()
