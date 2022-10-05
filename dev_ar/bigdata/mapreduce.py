# Functions used for mapreduce operations

# Modules
import xml.etree.ElementTree as ET
import re
from pathlib import Path
from datetime import datetime
from operator import itemgetter
from bigdata_logger import bigdata_logger

# Functions
# XML Parser Function
def get_dict_from_xml_files(files_path):
    """
        Function that takes xlm files path as argument end returns dictionary with xml tags and roots
        Raise error if path not found or no xlm files found
    Args:
        files_path (str): path with xlm files
    Returns:
        dict: dictionary with xlm tags as keys and xlm roots as values
    """
    # Check if path exists
    # Abs path from file using pathlib.Path
    files_path = Path(__file__).resolve().parent / files_path
    if not files_path.resolve().exists():
        raise FileNotFoundError(f'Path {files_path} not fount.')
    # List xml files in path if any, else raise error
    files = files_path.iterdir()
    xml_files = [file.resolve() for file in files if file.suffix == '.xml']
    if len(xml_files) == 0:
        raise FileNotFoundError(f'XML files not fount in {files_path} path')
    # Use xml.etree.ElementTree methods to build dictionary
    xml_list = [ET.parse(file).getroot() for file in xml_files]
    xml_dict = {root.tag : root for root in xml_list}
    return xml_dict

def get_single_xml(
    xml_file_path
    ):
    """
        Functions to extract data from a single XML file
    Args:
        xml_file_path (str): file path
    Returns:
        iterable root: root from xlm file to be used in mapreduce functions
    """
    xml_file_path = Path(__file__).resolve().parent / xml_file_path
    if not xml_file_path.resolve().exists():
        raise FileNotFoundError(f'File {xml_file_path} not fount.')
    return ET.parse(xml_file_path).getroot()

# Mapping Function
def map_function(
    xml_root,
    ):
    """
        Function that returns a dictionary of every attribute contained in a root object previously extracted from a XML with xml.etree.ElementTree getroot() method.
    Args:
        xml_root (root object) : XLM root
    Returns:
        (dictionary): Dictionay with attributes as key-values
    """
    # Alternative using ET .get method. Will parse as none if no attributes found - NOT USED -
    # xml_tuples = [tuple(map(post.get, attributes)) for post in xml_dict[key]]
    # Maps using attribute method from ElementTree and returns a generator that can be iterated as a dictionary
    mapped_xml = map(lambda x: x.attrib, xml_root)
    return mapped_xml

def reduce_tasks(result_tuples):
    pass

# Map-Reduce Functions
def mapper_task_3(
    mapped_xml
    ):
    """
        Basic reducing and extracting function that filters questions using 'PostTypeId' == '1' and gets the Post ID and Timedelta of time activity.
        Timedeltas are calculculated using CreationDate and LastActivityDate
        Then returns a list of tuples with id and timedelta. The timedelta is set to days.
    Args:
        mapped_xml (generator): generator of dict attributes made with map_function using xml ElementTree attrib method
    Returns:
        (list of tuples): List of tuples with ids and activity deltas (deltas in days)
    """
    # Last Task Top 10 preguntas que tuvieron mayor tiempo de actividad (PostTypeId == 2, LastActivityDate - CreationDate) (ojo no cerradas)
    deltas = []
    # For loop to iterate the generator object
    for xml_item in mapped_xml:
        # Check if it is a question
        if xml_item.get('PostTypeId') == '1':
            # Use of timedeltas and id for the tuple. Set deltatime in days for the return
            aux_delta = datetime.fromisoformat(xml_item['LastActivityDate']) - datetime.fromisoformat(xml_item['CreationDate'])
            aux_tuple = (xml_item['Id'], aux_delta.days)
            deltas.append(aux_tuple)
    return deltas

def reducer_task_3(
    mapped_tuples
    ):
    """
        Basic reducer function. Sort the tuples on deltatime and return the top 10.
        Use Built-in std library operator.itemgetter for key in sorting
    Args:
        mapped_tuples (list of tuples): Tuples from the mapping and extracting function with ids and deltatimes.
    Returns:
        list of tuples: list of tuples with the top 10 activity deltatimes and ids
    """
    # Use of sorted standard python function for aggregation
    return sorted(mapped_tuples, key = itemgetter(1), reverse = True)[0:10]

def mapper_task_2(
    mapped_xml
    ):
    """
        Basic reducing and extracting function that filters AnswerCount and Score of each post into a tuple.
        Tuples, with cast them to int type, later added on a list and returned.
    Args:
        mapped_xml (generator): generator of dict attributes made with map_function using xml ElementTree attrib method
    Returns:
        (list of tuples): List of tuples with AnswerCount and Score
    """
    # Relación entre cantidad de respuestas de un post y su puntaje. (AnswerCount - Score)
    regression = []
    # For loop to iterate the mapper generator
    for xml_item in mapped_xml:
        # try-except block, probably won't needed because of .get method
        try:
            answers = 0 if xml_item.get('AnswerCount') is None else int(xml_item.get('AnswerCount'))
        except KeyError:
            answers = 0
        scores = int(xml_item.get('Score'))
        # Append tuples to regression list
        regression.append((answers, scores))
    return regression

def reducer_task_2(
    result_tuples
    ):
    """
        Basic reducer function, using sum, list and zip Built-in Functions methods. Return just the ratio of both.
        Use std library operator.itemgetter for key in sorting
    Args:
        mapped_tuples (list of tuples): List of tuples with AnswerCount and Score.
    Returns:
        Float: AnswerCount and Score ratio
    """
    result = list(zip(*result_tuples))
    return sum(result[0]) / sum(result[1])

def mapper_task_1(
    mapped_xml
    ):
    """
        Basic reducing and extracting function that filters tags, AcceptedAnswerId and AnswerCount of each post.
        Filters posts with no accepted answers, clean tags with aux function _get_tags and adds them to a predefine ('tag', 1) tuple for later aggregation
    Args:
        mapped_xml (generator): generator of dict attributes made with map_function using xml ElementTree attrib method
    Returns:
        (list of tuples): List of tuples with tags
    """
    # Top 10 tipo de post sin respuestas aceptadas (tag - AcceptedAnswerId - AnswerCount)
    top_tags = []
    # For loop for generator
    for xml_item in mapped_xml:
        # Check if post has no accepted answers and tags are present
        if xml_item.get('AcceptedAnswerId') is None and xml_item.get('Tags') is not None:
            for t in _get_tags(xml_item.get('Tags')):
                # Predefined tuple 
                top_tags.append((t, 1))
            # top_tags.append([(e, 1) for e in _get_tags(xml_tuple.get('Tags'))])
    return top_tags

def reducer_task_1(
    result_tuples
    ):
    """
        Extremely Basic reducer function, using for loop and try-except block to build a dictionary with every tag count/sum.
    Args:
        mapped_tuples (list of tuples): List of tuples with items to be aggregated.
    Returns:
        dictionary: dict with tags as keys and sum as values
    """
    d = {}
    for k, v in result_tuples:
        try:
            d[k] += v
        except KeyError:
            d[k] = v
    # Order top 10 tags in dict with eclectic method
    ordered = {e: d[e] for e in sorted(d, key = d.get, reverse = True)[0:10]}
    return ordered

# Aux Functions
def _get_tags(string):
    if string is None:
        return None
    return re.findall(r'<(.+?)>', string)