# Modulos #
import pytest
from mapper_rf import mapper
from reduce_rf import reducer
"""
Testing mapreduce de archivo .xml realizado por Rodrigo Fuentes
"""
# Se genera las varaibles a testear
post_views, mapped_tags, score_answertime = mapper() 
top10_post_views, tags_reduced, average_answer_time = reducer(post_views, mapped_tags, score_answertime)

"""
Verifica la cantiadad de registros mapeados por la funcion mapper para el top 10 post mas vistos

Resultado: passed
"""
def test_mapreduce_post_views(
):
    assert len(post_views) == 3675849

"""
Verifica la cantiadad de registros mapeados por la funcion mapper para el top 10 palabras mas nombradas en los post por tag

Resultado: passed
"""    
def test_mapreduce_mapped_tags(
):
    assert len(mapped_tags) == 3044145

"""
Verifica la cantiadad de registros mapeados por la funcion mapper para el tiempo de respuestas con score entre 200 y 300 puntos

Resultado: passed
"""  
def test_mapreduce_score_answertime(
):
    assert len(score_answertime) == 652089

"""
Verifica si el output del reducer coincide con lo esperado para el top 10 post mas vistos

Resultado: passed
"""
def test_mapreduce_top10_post_view(
):
    assert top10_post_views == [{'Id': '4', 'ViewCount': 5534},
                                {'Id': '6', 'ViewCount': 1175},
                                {'Id': '7', 'ViewCount': 0},
                                {'Id': '8', 'ViewCount': 736},
                                {'Id': '9', 'ViewCount': 24779},
                                {'Id': '11', 'ViewCount': 11628},
                                {'Id': '12', 'ViewCount': 0},
                                {'Id': '13', 'ViewCount': 8601},
                                {'Id': '14', 'ViewCount': 9906},
                                {'Id': '16', 'ViewCount': 9390}]


"""
Verifica si el output del reducer coincide con lo esperado para el top 10 palabras mas nombradas en los post por tag

Resultado: passed
"""
def test_mapreduce_tags(
):
    assert tags_reduced == [('c#', 118980),
                            ('java', 75037),
                            ('php', 65739),
                            ('javascript', 57210),
                            ('.net', 56348),
                            ('iphone', 54736),
                            ('asp.net', 52655),
                            ('jquery', 47927),
                            ('c++', 45909),
                            ('python', 38083)]

"""
Verifica si el output del reducer coincide con lo esperado para el tiempo de respuestas con score entre 200 y 300 puntos

Resultado: passed
""" 
def test_mapreduce_average_answer_time(
):
    assert average_answer_time == 4.64395
    