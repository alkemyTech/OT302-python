
# Modules
from itertools import chain
from itertools import islice
from mapreduce import *

# Chunkify Functions
# V1 using for range loop
def chunkify(
    iterable,
    chunk_size
    ):
    """
    Function that chunks an iterable into parts of chunk size
    Args:
        iterable (iter): Iterable form of iterator, list, generator, etc-
        chunk_size (int): size of the chunk
    Yields:
        iter: iterable of same type as entered iterable with chunk_size
    """
    for e in range(0, len(iterable), chunk_size):
        yield iterable[e: e + chunk_size]

# V2 Using walrus operator
def chunking_express(
    items,
    chunk_size
    ):
    """
    Function that chunks an iterable into parts of chunk size
    Args:
        items (iter): Iterable form of iterator, list, generator, etc
        chunk_size (int): size of the chunk
    Yields:
        iter: iterable of same type as entered iterable with chunk_size
    """
    iterator = iter(items)
    while chunk := tuple(islice(iterator, chunk_size)):
        yield chunk

# Shuffling reducer
def shuffle_express(chunks):
    """
    Function that reduce tuples made in mapping stage using reduce from functools and add from operator built in modules
    Args:
        chunks (iterable): iterable with all mapped and proceced chunks in form of tuples

    Returns:
        iterable of tuples: full tuple list or iterable
    """
    #return reduce(add, chunks)
    return flatten(chain.from_iterable(chunks))

# Chunkatelechy Ops
def chunkerizer(xml_file_path, mapper_task, reducer_task, N = 50):
    """
    Function that takes xml file path, extract data from it, map and reduce it with custom functions as params
    Args:
        xml_file_path (str): file path
        mapper_task (function): function that performs the mapping
        reducer_task (function): function that performs the reducing
        N (int, optional): Number of chunks. Defaults to 50.

    Returns:
        Depends on the reducing function what is returned
    """
    # Get root from file path
    xml_root = get_single_xml(xml_file_path)
    # Use function below
    return chunkerizer_from_root(xml_root = xml_root, mapper_task = mapper_task, reducer_task = reducer_task, N = N )

def chunkerizer_from_root(
    xml_root,
    mapper_task,
    reducer_task,
    N = 50
    ):
    """
    Function that takes xml root, extract data from it, map and reduce it with custom functions as params
    Args:
        xml_file_path (root object): root from xml.etree.ElementTree getroot()
        mapper_task (function): function that performs the mapping
        reducer_task (function): function that performs the reducing
        N (int, optional): Number of chunks. Defaults to 50.

    Returns:
        Depends on the reducing function what is returned
    """
    # n length of each chunk from N number of chunks
    n = len(xml_root) // N
    chunked_xml = chunking_express(xml_root, n)
    # Map each chunk, shuffle and reduce it
    def aux_mapper(roots):
        aux = map(mapper_task, map_function(roots))
        return filter(lambda x: x is not None,  aux)
    chunked_map = map(aux_mapper, chunked_xml)
    return reducer_task(shuffle_express(chunked_map))

# Aux Functions
def flatten(items):
    """
    Function that Yield items from any nested iterable. Set to not flatten tuple type files.
    Args:
        items (iterable) : iterable to be flatten
    Returns:
        Flatten iterable
    """
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes, tuple)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x