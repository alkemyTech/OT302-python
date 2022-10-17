# This file is going to be used to test Jeremy's mapreduce script.
# The objectives of the MapReduce are:
#   - Top 10 tags con mayores respuestas aceptadas
#   - Relación entre cantidad de palabras en un post y su puntaje
#   - Demora de respuesta promedio en posts

# Tests have been done to what can be tested. The final results are outside a function
# so they cannot be tested in a separate file.

import datetime

# Import all functions from map_reduce
from mapreduce_big_data import mapper

# path and file names
file_path = r"/Users/rodri/OneDrive/Documentos/Data Science/Courses/Alkemy/112010 Meta Stack Overflow"
file_name = "posts.xml"
file_name2 = "comments.xml"


def test_mapper_types():
    """According to Jeremy's documentation the mapper function returns 3 lists.

    This Function tests that the mapper function returns the 3 lists.
    If the assertion fails it is going to raise an AssertionError with the corresponding message"""

    list_tags, list_relation, list_answer_delay = mapper(
        file_path=file_path, file_name=file_name, file_name2=file_name2
    )
    assert isinstance(list_tags, list), "A list is expected for the tags"
    assert isinstance(list_relation, list), "A list is expected for the relation"
    assert isinstance(list_answer_delay, list), "A list is expected for the answer delay"


def test_correct_elements_into_lists():
    """According to Jeremy's documentation the lists that are returned from the mapper function
    have the following characteristics:
        - list_tags: list of str with the different tags
        - list_relation: list with 2 coordinates [score, post_quantity_of_words]
        - list_answer_delay: list with 3 coordinates [post_id, creation_date_post, creation_date_first_comment]

    This function tests that the lists have the characteristics that are mentioned above
    """
    list_tags, list_relation, list_answer_delay = mapper(
        file_path=file_path, file_name=file_name, file_name2=file_name2
    )

    # Assert that all tags are of type string
    assert all(map(lambda x: isinstance(x, str), list_tags)), "Not all tags are of type str"

    # Assert that all list_relation elements are a list with 2 integers
    for coordinates in list_relation:
        # Assert that coordinates is a list
        assert isinstance(coordinates, list), "Not all coordinates are lists"
        # Assert that it has 2 elements
        assert len(coordinates) == 2, "There are coordinates with more than 2 elements"

        # Assert that every coordinate is an integer
        for coordinate in coordinates:
            assert isinstance(coordinate, int), "Coordinates elements are not of type int"

    # Assert that all list_answer_delay have 3 elements and that post_id can be transformed to int
    # and that creation_date_post, creation_date_first_comment can be transformed to datetime
    for coordinates in list_answer_delay:
        # Assert that coordinates is a list
        assert isinstance(coordinates, list), "Not all coordinates are lists"
        # Assert that it has 3 elements
        assert len(coordinates) == 3, "There are coordinates with more than 3 elements"

        # Assert for every coordinate

        # post_id can be transformed to int
        assert isinstance(int(coordinates[0]), int)
        # creation_date_post can be transformed to a datetime.datetime object
        assert isinstance(datetime.datetime.fromisoformat(coordinates[1]), datetime.datetime)
        # creation_date_first_comment can be transformed to a datetime.datetime object
        assert isinstance(datetime.datetime.fromisoformat(coordinates[2]), datetime.datetime)


test_mapper_types()
test_correct_elements_into_lists()

# All test pass
