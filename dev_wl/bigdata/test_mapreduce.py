from functools import reduce
from mapreduce import *

FILE_PATH = "../meta_stackOverflow/posts.xml"


def test_mapreduce_tasker():
    assert mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_1,
                         reduce = reducer_task_1) == {  'discussion':2916,
                                                        'feature-request':2815,
                                                        'bug':1396,
                                                        'support':1261,
                                                        'stackoverflow':848,
                                                        'status-completed':647,
                                                        'tags':524,
                                                        'reputation':427,
                                                        'area51':372,
                                                        'questions':354}


def test_mapreduce_tasker2():
    assert mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_2,
                         reduce = reducer_task_2) == 1.254095319555108


def test_mapreduce_tasker3():
    assert isinstance(mapreduce_tasker(xml_file_path = FILE_PATH,
                         map_root = mapper_task_3,
                         reduce = reducer_task_3), list)


def test_get_single_xml():
    assert isinstance(get_single_xml(FILE_PATH), object)


def test_map_function():
    assert isinstance(map_function(get_single_xml(FILE_PATH)), dict)

