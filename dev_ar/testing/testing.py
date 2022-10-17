# Script for testing Walter MapReduce Functions from B Group
# Tickets
# Top 10 Tag without accepted answers
# Word count vs views ratio
# Average Score vs most favourites ratio

# Modules
import pytest
import wl_map_reduce
import pandas as pd

# Testing Class for later Inheritance
class Test_MapReduce:
    xml_file_path = '../bigdata/raw_data/posts.xml'
    mapper_result = wl_map_reduce.mapper(xml_file_path)
    shuffle_sort_result = wl_map_reduce.shuffle_sort(mapper_result)
    reduce_result = wl_map_reduce.reduce(shuffle_sort_result)

    # General testing functions
    # Parametrize results and types
    @pytest.mark.parametrize(
        'result, idx, types', [
            (mapper_result, 0, list),
            (mapper_result, 1, list),
            (mapper_result, 2, list),
            (shuffle_sort_result, 0, list),
            (shuffle_sort_result, 1, list),
            (shuffle_sort_result, 2, list),
            (reduce_result, 0, list),
            (reduce_result, 1, list),
            (reduce_result, 2, pd.Series)
        ]
    )
    
    # Function for testing types
    def testing_general_types(self, result, idx, types):
        assert isinstance(result[idx], types)

    # Parameters for three main functions type results
    @pytest.mark.parametrize(
        'result', [
            mapper_result, shuffle_sort_result, reduce_result
        ]
    )

    # Function for types
    def test_types(self, result):
        assert isinstance(result, list)

    # Parameters for lenght results
    @pytest.mark.parametrize(
        'result, length', [
            (mapper_result, 3),
            (shuffle_sort_result, 3),
            (reduce_result, 3)
        ]
    )

    # Function for testing lenghts
    def testing_general_length(self, result, length):
        assert len(result) == length

    # Parameter for testing the actual result in one function
    @pytest.mark.parametrize(
        'idx2, results', [
            (0, [('discussion', 2916), ('feature-request', 2815), ('bug', 1396), ('support', 1261), ('stackoverflow', 848), ('status-completed', 647), ('tags', 524), ('reputation', 427), ('area51', 372), ('questions', 354)])
        ]
    )

    # Result testing function
    def testing_general_results(self, idx2, results):
        assert wl_map_reduce.reduce(wl_map_reduce.shuffle_sort(wl_map_reduce.mapper(self.xml_file_path)))[idx2] == results