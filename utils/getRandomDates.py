import datetime
import random

year_2000 = datetime.datetime.timestamp(datetime.datetime.fromisoformat("2000-01-01"))
year_2021 = datetime.datetime.timestamp(datetime.datetime.fromisoformat("2021-11-12"))


def getRandomDates():
    years = [random.uniform(year_2000, year_2021), random.uniform(year_2000, year_2021)]
    years.sort()
    years = {"createtime": years[0], "updatetime": years[1]}
    return years


def getRandomYear():
    return random.choice([i for i in range(2000, 2022)])



if __name__ == '__main__':
    ys = getRandomDates()
    print(ys)
