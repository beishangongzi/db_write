import csv


def readCsv(file):
    with open(file, "r", newline="", encoding="utf-8") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        header = spamreader.__next__()
        for row in spamreader:
            # res = zip(header, row)
            # yield dict(res)
            yield row

def getHeader(file):
    with open(file, newline="", encoding="utf-8") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        return spamreader.__next__()
