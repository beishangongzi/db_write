import datetime
import json
from multiprocessing import Process

import psycopg2

from checkData.process_data import process_data
import AMap_adcode_citycode
from utils.getRandomDates import getRandomDates, getRandomYear
from sql import createTable


def process_data_postgresql(file, adcodes, time, p):
    datas = process_data(file, adcodes, time, p)
    for data in datas:
        geometry = data["geometry"]
        properties = list(data["properties"].values())
        properties[-1] = str(datetime.datetime.fromtimestamp(properties[-1]))
        properties[-2] = str(datetime.datetime.fromtimestamp(properties[-2]))
        res = [str(json.dumps(geometry))] + properties
        yield str(tuple(res)) + ","

def insertIntoPostgreSql(datas, time):
    conn = psycopg2.connect(user="postgres", password=123456, host="127.0.0.1", port=5434)
    cursor = conn.cursor()
    name = f"test_building_beijing_{time * 2000000}"
    cursor.execute("DROP TABLE IF EXISTS {};".format(name))
    conn.commit()
    cursor.execute(createTable.format(name))
    conn.commit()
    sql = f"""
        insert into {name} (geometry, BID, typeLab, year, level, bArea, tArea, subtype, province, city, county, adcode, createtime, updatetime)
    values """
    some_data = []
    i = 1
    for data in datas:
        some_data.append(data)
        if i % 10000 == 0:
            _sql = sql + "".join(some_data)[:-1]
            try:
                cursor.execute(_sql)
                conn.commit()
                print("success")
            except Exception as e:
                conn.rollback()
                print(e)
                for one_data in some_data:
                    try:
                        cursor.execute(sql + one_data[:-1])
                        conn.commit()
                    except:
                        conn.rollback()
                        with open("error.txt", "a+", encoding="utf-8") as f:
                            f.write(sql + one_data[:-1] + "\n")
            some_data = []
        if i > 100000:
            break

        i += 1
    if len(some_data) != 0:
        _sql = sql + "".join(some_data)[:-1]
        try:
            cursor.execute(_sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)
            for one_data in some_data:
                try:
                    cursor.execute(sql + one_data[:-1])
                    conn.commit()
                except:
                    conn.rollback()
                    with open("error.txt", "a+", encoding="utf-8") as f:
                        f.write(sql + one_data[:-1] + "\n")

    print("finished")
    cursor.close()
    conn.close()


def main(adcodes_file="adcode_view.csv", geojson_file="../fix_beijing.geojson", time=2):
    # 1. process data.
    adcodes = AMap_adcode_citycode.main.getAdcode(adcodes_file)
    p = 1 / time
    datas = process_data_postgresql(geojson_file, adcodes, time, p)
    # 2. connect database.
    insertIntoPostgreSql(datas, time)
    with open("main_res.txt", "a+", ) as f:
        f.write("finished")




if __name__ == '__main__':
    # geojsonFile = "/z/00STAFF/Lu Yifei/数据/bldg/fix_beijing.geojson"
    geojsonFile = "../fix_beijing.geojson"
    """500万、2000万、4000万、8000万"""
    # main(geojson_file=geojsonFile, time=3)
    Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 1}).start()
    Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 2}).start()
    Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 3}).start()
    Process(target=main, kwargs={"geojson_file": geojsonFile, "time": 4}).start()
