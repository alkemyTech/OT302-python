#!/usr/bin/python3
# -*-coding:utf-8 -*

import sys
import re
from operator import itemgetter

aux_list = []
for key_value in sys.stdin:
    aux_list.append(re.findall(r'(\d+)', key_value))
    # key_value.split(' '))

aux_list.sort(key = itemgetter(1), reverse = True)
print(aux_list[0:10])