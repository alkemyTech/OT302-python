#!/usr/bin/python
# -*-coding:utf-8 -*

import sys
import re
# from operator import itemgetter

aux_list = []
num = 0
den = 0
for key_value in sys.stdin:
    aux = (re.findall(r'(\d+)', key_value))
    num += int(aux[0])
    den += int(aux[1])
    # aux_list.append(key_value.split(' '))

# aux_list.sort(key = itemgetter(1), reverse = True)
# print(aux_list[0:10])

# aux_list = [(float(e[0]), float(e[0])) for e in aux_list]
# result = list(zip(*aux_list))
# print(sum(result[0]) / sum(result[1]))
# print(str(result[0]) + ' ' + str(result[1]))
print(float(num) / float(den))