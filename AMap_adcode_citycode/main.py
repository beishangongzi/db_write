from utils.readCSV import readCsv
import random


# 省 市 县
def getAdcode(file):
    res = []
    datas = readCsv(file)
    for data in datas:
        if data[2].endswith("市辖区"):
            data[2] = data[2][:-3]
        res.append({"province": data[0], "city": data[2], "county": data[4], "adcode": data[3]})
    return res


def getRandomOne(adcodes: list, p=1):
    score_beijing = p * len(adcodes)
    score_other = (1-p) * len(adcodes)
    weights = [score_beijing / 16] * 16
    weights = weights + [score_other / (len(adcodes) - 16)] * (len(adcodes) - 16)
    res = random.choices(adcodes, weights=weights, k=1)
    return res[0]



if __name__ == '__main__':
    file = "adcode_view.csv"

    adcodes = getAdcode(file)
    s = getRandomOne(adcodes)
    print(s)
