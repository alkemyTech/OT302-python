#!/usr/bin/python
# -*-coding:utf-8 -*

import sys
import xml.etree.ElementTree as ET
from datetime import datetime

def _mapper_task_3(item):
    """
        Aux function that returns single tuple with id and timedelta using CreationDate and LastActivityDate
        For using in reduce optimized method
    Args:
        item (dict from root object): dictionary get from getroot method of and xlm file
    Returns:
        tuple: tuple with Id and Timedelta set in days
    """
    if item.get('PostTypeId') == '1':
        aux_delta = datetime.fromisoformat(item.get('LastActivityDate')) - datetime.fromisoformat(item.get('CreationDate'))
        aux_tuple = (item.get('Id'), aux_delta.days)
        return aux_tuple

def _mapper_task_2(item):
    """
        Aux Function that returns a sinle tuple with answers and score
    Args:
        item (dict):  dictionary get from getroot method of and xlm file

    Returns:
        tuple: tuple with answers and scores
    """
    answers = 0 if item.get('AnswerCount') is None else int(item.get('AnswerCount', 0))
    scores = int(item.get('Score', 0))
    return (answers, scores)

tree = ET.parse(sys.stdin)
root = tree.getroot()

for node in root:
    printable = _mapper_task_2(node)
    if printable is not None:
        # print('{} {}'.format(printable[0], printable[1]))
        print(str(printable[0]) + ' ' + str(printable[1]))