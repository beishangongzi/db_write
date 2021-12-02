import functools
import json

import psycopg2

import sql
import time

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
    geojson = json.dumps(geojson)
    start = time.time()
    _sql = sql.is_interects.format(table_name, "'"+geojson.__str__()+"'")
    cursor.execute(_sql)
    time.sleep(2)
    end = time.time()
    with open(f"postgresql_res_city_geometry_index.txt", "a+", encoding="utf-8") as f:
        res_str = f"""{county_name}-----{county_adcode}-----{table_name}-----{cursor.fetchone()[0].__str__()}-----{end-start}\n"""
        f.write(res_str)

def test2(file, cursor, table_name, county_adcode, county_name, geojson):
    geojson = json.dumps(geojson)
    start = time.time()
    _sql = sql.is_interects.format(table_name, "'"+geojson.__str__()+"'")
    cursor.execute(_sql)
    time.sleep(2)
    end = time.time()
    with open(file, "a+", encoding="utf-8") as f:
        res_str = f"""{county_name}-----{county_adcode}-----{table_name}-----{cursor.fetchone()[0].__str__()}-----{end-start}\n"""
        f.write(res_str)



def test_postgresql2():
    """
    测试各个城区，没有索引
    :return:
    """
    datas = from_counties_beijing("counties_beijing.geojson")
    conn = psycopg2.connect(user="postgres", password=123456, host="192.168.23.10", port=5434)
    cursor = conn.cursor()
    tables_nums = [6000000, 20000000, 40000000, 80000000]
    i = 1
    for data in datas:
        print(i)
        i += 1
        for tables_num in tables_nums:
            tables = f"test_building_beijing_{tables_num}"
            data.insert(0, tables)
            data.insert(0, cursor)
            test(*data)
            data.pop(0)
            data.pop(0)



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


@cal_time("add_index.txt")
def AddIndex(conn, cursor, table, field):
    s = """CREATE INDEX geometry_index_on_{} ON {} USING GIST ({});""".format(table, table, field)
    print(s)
    cursor.execute(s)
    conn.commit()
    return f"{table}-----{field}-----geometry_index_on_{table}"


def testAddIndex():
    tables_nums = [6000000, 20000000, 40000000, 80000000]
    conn = psycopg2.connect(user="postgres", password=123456, host="192.168.23.10", port=5434)
    cursor = conn.cursor()
    for tables_num in tables_nums:
        table = f"test_building_beijing_{tables_num}"
        AddIndex(conn,cursor, table, "geometry")
    cursor.close()
    conn.close()


if __name__ == '__main__':
    test_postgresql2()


