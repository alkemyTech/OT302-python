import xml.etree.ElementTree as ET
from big_data_logging import configured_logger

logger = configured_logger()


def chunks(list, n=10):
    """Yield n number of striped chunks from list.
    Parameters
    ----------
        list : list
            List that must be chunked
        n : int
            Number of chunks

    Returns
    -------
        yield : generator
            Generator with the different chunks

    """

    for i in range(0, n):
        yield list[i::n]


def generate_chunks():
    """Function that creates a generator with the different chunks

    Returns
    -------
        generator : generator
            returns a generator with the different chunks of the xml file
    """

    logger.info("Starting the chunks module...")

    # Load and parse the posts.xml file
    tree = ET.parse("./112010 Meta Stack Overflow/posts.xml")
    # Get the root of the xml
    root = tree.getroot()

    list_to_chunk = []

    # Loop into each row element
    for child in root:
        # Get the attributes of each row element
        list_to_chunk.append(child.attrib)

    return chunks(list_to_chunk)
