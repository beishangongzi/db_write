import json


def readData(file):
    with open(file, "r", encoding="utf-8") as f:
        line = f.readline().strip()
        while line != "":
            l = line
            if l.startswith('{ "type":'):
                if l.endswith(",\n"):
                    l = l[0:-2]
                if l.endswith("\n") or l.endswith(","):
                    l = l[0:-1]
                yield json.loads(l)
            line = f.readline()


if __name__ == '__main__':
    # 2175385 lines
    # adcodes = readData("/media/andy/Data/python/db/AMap_adcode_citycode/adcode_view.csv")
    adcodes = readData("../beijing.geojson")
    # adcodes = readData("/media/andy/Data/historyWeather/Scrapy2/liyuanchen/_11_24/record_interdelay.csv")
    i = 0
    # for adcode in adcodes:
    #     if i % 100000 == 0:
    #         print(i)
    #     i += 1
    # print(i)

    for data in readData("../beijing.geojson"):
        print(data)
        i += 1
        if i > 10:
            break
