## 读取geojson文件，返回格式化数据
import json
import os


def process(file, file_type):
    if file_type == 1:
        return process_type1(file)
    else:
        return process_type2(file)


def process_type1(file):
    """
    the type of the file is line by line
    :param file : the path of the input file
    :return: [county_adcode, county_name, geojson, 比例]
    """
    with open(file, "r", encoding="utf-8") as f:
        line = f.readline().strip()
        while line != "":
            line = json.loads(line)
            county_adcode = line["features"][0]["properties"]["adcode"]
            county_name = line["features"][0]["properties"]["name"]
            geojson = line["features"][0]["geometry"]
            line = f.readline().strip()
            yield [county_adcode, county_name, geojson, 'all']


def process_type2(file):
    file_name = os.path.basename(file).split(".")[0]
    county_name = file_name.split("_")[-1]
    proportion = "_".join([file_name.split("_")[0], file_name.split("_")[1]])
    county_adcode = "000000"
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
        geojson = data["features"][0]["geometry"]
        yield [county_adcode, county_name, geojson, proportion]


if __name__ == '__main__':
    data = process("../data/5_1_chaoyang.geojson", 2)
    print(data.__next__())