import datetime
import json

from checkData.readData import readData
from AMap_adcode_citycode.main import getAdcode
from utils.getRandomOne import getRandomOne
from utils.getRandomDates import getRandomDates

import psycopg2


def processData(file, adcodes, adcodes_beijing, years):
    datas = readData(file)

    i = 0
    for data in datas:
        adcode = getRandomOne(adcodes)
        data["properties"].update(adcode)
        data["properties"].update({"year": getRandomOne(years)})
        data["properties"].update(getRandomDates())
        yield data
        i += 1
        if i % 1000000 == 0:
            print(i)

    print(i)


def prodessDataForPostger(datas):
    for data in datas:
        geometry = data["geometry"]
        properties = list(data["properties"].values())
        properties[-1] = str(datetime.datetime.fromtimestamp(properties[-1]))
        properties[-2] = str(datetime.datetime.fromtimestamp(properties[-2]))
        res = [str(json.dumps(geometry))] + properties
        yield res


def getConnect():
    # conn = psycopg2.connect(user="postgres", password=123456, host="192.168.23.10", port=5434)
    conn = psycopg2.connect(user="postgres", password=123456, host="127.0.0.1", port=5434)
    return conn


def insertIntoPostgreSql(datas, host):
    global _sql
    conn = getConnect()
    sql = """
    insert into test_building_beijing_more_data (geometry, BID, typeLab, year, level, bArea, tArea, subtype, province, city, county, adcode, createtime, updatetime)
values """
    cursor = conn.cursor()
    i = 1
    inserDatas = []
    for data in datas:
        inserDatas.append(tuple(data))
        if i % 10000 == 0:
            if i % 1000 == 0:
                print(i)
            try:
                _sql = sql + (str(tuple(inserDatas))[1:-1]).replace("None", "null")

                if _sql.endswith(","):
                    _sql = _sql[:-1]
                cursor.execute(_sql)
                conn.commit()
                print("success")
            except:
                conn.rollback()
                for inserData in inserDatas:
                    try:
                        _sql = sql + (str(tuple([inserData]))[1:-1]).replace("None", "null")

                        if _sql.endswith(","):
                            _sql = _sql[:-1]
                        print(_sql)
                        cursor.execute(_sql)
                        conn.commit()

                    except Exception as e:
                        conn.rollback()
                        print(e)
                        with open("error_text.txt", "a+", encoding="utf-8") as f:
                            f.write(_sql)
                            f.write("\n")
            inserDatas = []
        i += 1
    if len(inserDatas) != 0:
        try:
            _sql = sql + (str(tuple(inserDatas))[1:-1]).replace("None", "null")

            if _sql.endswith(","):
                _sql = _sql[:-1]
            cursor.execute(_sql)
            conn.commit()
            print("success")
        except:
            conn.rollback()
            for inserData in inserDatas:
                try:
                    _sql = sql + (str(tuple([inserData]))[1:-1]).replace("None", "null")

                    if _sql.endswith(","):
                        _sql = _sql[:-1]
                    print(_sql)
                    cursor.execute(_sql)
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print(e)
                    with open("error_text.txt", "a+", encoding="utf-8") as f:
                        f.write(_sql)
                        f.write("\n")
    print(i)

    cursor.close()
    conn.commit()
    conn.close()


def main():
    file = "adcode_view.csv"

if __name__ == '__main__':
    file = "adcode_view.csv"
    file_beijing = ""
    adcodes = getAdcode(file)
    # adcodes_beijinig = getAdcode(file_beijing)
    geojsonFile = "fix_beijing.geojson"
    # geojsonFile = "/z/00STAFF/Lu Yifei/数据/bldg/fix_beijing.geojson"
    years = [round(float(i), 2) for i in range(2000, 2022)]
    for i in range(int(input("请输入循环次数： "))):
        dates = processData(geojsonFile, adcodes, "", years)
        dates = prodessDataForPostger(dates)
        insertIntoPostgreSql(dates, "")
