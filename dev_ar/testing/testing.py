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
    """
    Class with testing methods and parameters for wl_map_reduce.py mapreduce functions
    Uses parametrize decorators for looping testing functions
    Focus on type, length and just one result testing
    """
    # Class Parameters
    # XML File Path
    xml_file_path = '../bigdata/raw_data/posts.xml'
    # Testing Call Functions Results
    # Mapper Function call and definition
    mapper_result = wl_map_reduce.mapper(xml_file_path)
    # Shuffle Function call and definition
    shuffle_sort_result = wl_map_reduce.shuffle_sort(mapper_result)
    # Reduce Function call and definition
    reduce_result = wl_map_reduce.reduce(shuffle_sort_result)

    # General testing functions
    # Parametrize results and types
    # To be used on above function calls
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
        """
        Tests general types from results of the mapreduce functions from wl_map_reduce script.
        Use parametrized args from decorator pytests method
        Args:
            result (to be checked): function call result
            idx (int): index of list returned by function
            types (type): type of the function result
        """
        assert isinstance(result[idx], types)

    # Parameters for three main functions type results
    # Parametrizes results of mapreduce functions. Should be lists of lists.
    @pytest.mark.parametrize(
        'result', [
            mapper_result, shuffle_sort_result, reduce_result
        ]
    )

    # Function for types
    def test_types(self, result):
        """
        Function for testing basic mapreduce functions that returns list of lists
        Args:
            result (list): list of lists with mapreduce results
        """
        assert isinstance(result, list)

    # Parameters for lenght results
    # Parametrize lenght result of mapreduce functions. Should all be on lenght 3
    @pytest.mark.parametrize(
        'result, length', [
            (mapper_result, 3),
            (shuffle_sort_result, 3),
            (reduce_result, 3)
        ]
    )

    # Function for testing lenghts
    def testing_general_length(self, result, length):
        """
        Function for testing lenghts of list results of mapreduce basic call
        Args:
            result (list): Each mapreduce function call result
            length (int): Lenght of each list, should be 3 in all cases
        """
        assert len(result) == length

    # Parameter for testing the actual result in one function
    # Checked result for one of the final results
    @pytest.mark.parametrize(
        'idx2, results', [
            (0, [('discussion', 2916), ('feature-request', 2815), ('bug', 1396), ('support', 1261), ('stackoverflow', 848), ('status-completed', 647), ('tags', 524), ('reputation', 427), ('area51', 372), ('questions', 354)])
        ]
    )

    # Result testing function
    def testing_general_results(self, idx2, results):
        """
        Function for testing final result of one of the tickets. Others results are too long for testing here.
        Args:
            idx2 (int): Index of mapreduce function list result
            results (list of tuples): Actual and Cheched result 
        """
        assert wl_map_reduce.reduce(wl_map_reduce.shuffle_sort(wl_map_reduce.mapper(self.xml_file_path)))[idx2] == results