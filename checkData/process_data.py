import AMap_adcode_citycode
from checkData.readData import readData
from utils.getRandomDates import getRandomDates, getRandomYear


def process_data(file, adcodes, times, p):
    datas = readData(file)

    i = 0
    for data in datas:
        for j in range(times):
            adcode = AMap_adcode_citycode.main.getRandomOne(adcodes, p)
            data["properties"].update(adcode)
            data["properties"].update({"year": getRandomYear()})
            data["properties"].update(getRandomDates())
            yield data
            i += 1
            if i % 1000000 == 0:
                print(i)

    print(i)

if __name__ == '__main__':
    file = "../../fix_beijing.geojson"
    adcodes = AMap_adcode_citycode.main.getAdcode("../adcode_view.csv")
    times = 1
    p = 0.5
    datas = process_data(file, adcodes, times, p)
    i = 1
    for data in datas:
        print(i)
        i += 1
