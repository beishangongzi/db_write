import json
import time

import psycopg2
from data import process

from decorators import cal_time


# 对postgrelsql建立索引

@cal_time("add_index.txt")
def addIndex(conn, cursor, table, field):

    s = """CREATE INDEX geometry_index_on_{} ON {} USING GIST ({});""".format(table, table, field)
    cursor.execute(s)
    conn.commit()
    return f"{table}-----{field}-----geometry_index_on_{table}"


def addIndex_postgresql(tables_nums):
    conn = psycopg2.connect(user="postgres", password=123456, host="127.0.0.1", port=5434)
    cursor = conn.cursor()
    for tables_num in tables_nums:
        table = f"test_building_beijing_{tables_num}"
        addIndex(conn, cursor, table, "geometry")
        print(table)
    cursor.close()
    conn.close()


def test_addIndex_postgresql():
    tables_nums = [2000000, 4000000, 6000000, 8000000]
    addIndex_postgresql(tables_nums)



# 对postgresql进行查询


@cal_time("postgresql_query_test.txt")
def query(cursor, table_name, county_adcode, county_name, geojson, proportion):
    geojson = json.dumps(geojson)
    is_interects = """select count(*) from {} where st_intersects(geometry,  st_geomfromgeojson({})) is true and city='{}';"""
    _sql = is_interects.format(table_name, "'" + geojson.__str__() + "'", county_name)
    cursor.execute(_sql)
    res_str = f"""{county_name}-----{county_adcode}-----{table_name}-----{cursor.fetchone()[0].__str__()}-----{proportion}"""
    return res_str


def query_postgresql(file, file_type, tables_nums):
    """
    测试各个城区，没有索引
    :return:
    """
    datas = process(file, file_type)
    conn = psycopg2.connect(user="postgres", password=123456, host="127.0.0.1", port=5434)
    cursor = conn.cursor()
    # tables_nums = [6000000, 20000000, 40000000, 80000000]


    i = 1
    for data in datas:
        print(i)
        i += 1
        for tables_num in tables_nums:
            tables = f"test_building_beijing_{tables_num}"
            data.insert(0, tables)
            data.insert(0, cursor)
            query(*data)
            data.pop(0)
            data.pop(0)
    cursor.close()
    conn.close()


def test_query_postgresql():
    tables_nums = [2000000, 4000000, 6000000, 8000000]
    file1 = "./data/5_1_朝阳区.geojson"
    file2 = "./data/counties_beijing.geojson"
    query_postgresql(file2, 1, tables_nums)
    query_postgresql(file1, 2, tables_nums)


def start():
    # tables_nums = [6000000, 20000000, 40000000, 80000000]
    tables_nums = [2000000, 4000000, 6000000, 8000000]
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
    # test_addIndex_postgresql()
    # test_query_postgresql()
    start()