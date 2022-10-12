# Modules
import mapreduce

xml_file_path = './raw_data/posts.xml'
xml_root = mapreduce.get_single_xml(xml_file_path = xml_file_path)
t1 = mapreduce.mapreduce_tasker_from_root(xml_root = xml_root, map_root = mapreduce.mapper_task_1, reduce = mapreduce.reducer_task_1)

def tester():
    assert mapreduce.mapreduce_tasker_from_root(
        xml_root, map_root = mapreduce.mapper_task_1, reduce = mapreduce.reducer_task_1) == t1