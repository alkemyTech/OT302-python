import pandas as pd

from functools import reduce
import xml.etree.ElementTree as ET
import os
from collections import Counter
import datetime

# functions open files XML´s


def read_xml(file):
    read = ET.parse(file)
    root = read.getroot()
    return root

# Files in to chunks


def chunckify(file, chunks):
    for i in range(0, len(file), chunks):
        yield file[i:i + chunks]

# Convert to datetime and get attrib[date]


def date_post(alkemy):
    # date in %Y-%m-%dT%H:%M:%S.%f', convert to data in '%Y-%m-%d
    change_datetime = datetime.datetime.strptime(
        alkemy.attrib["CreationDate"], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')
    return change_datetime

# Functions Map()


def map_post(data):
    map_date_post = list(map(date_post, data))
    date_count = Counter(map_date_post)
    return date_count


# Reduce Function
"""
reduce(D1 --> D2 )"key" + sum "values"
Retorna una lista de los n elementos mas comunes y sus conteos, del mas común al menos común
"""


def merge_date(D1, D2):
    D1.update(D2)
    return D1


def reduce_date(iter):
    reduce_post = reduce(merge_date, iter)
    top10 = reduce_post.most_common(10)
    df = pd.DataFrame(top10, columns=['date', 'count'])
    longest = max(len(word) for word, count in top10)
    for word, count in top10:
        print('{word:<{len}}: {count:5}'.format(
            len=longest + 1,
            word=word,
            count=count)
        )
    print("by : Jeanvitola")
    # save df to csv
    df.to_csv("top10_date.csv", index=False)


if __name__ == '__main__':
    read_file = read_xml("posts.xml")
    chunky_data = chunckify(read_file, 50)
    Map_data = list(map(map_post, chunky_data))
    reduce_date(Map_data)
