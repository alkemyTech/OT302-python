import collections
from statistics import mean

def reducer(post_views, mapped_tags, score_answertime):
    """Function to reduce the results that come from shuffler
    Parameters
    -------
        post_views : list
            List of dicts. Each dict has key 'Id' and 'ViewCount'. It is sorted
        mapped_tags : list
            List of dicts. Each dict has as key and tag and a value 1. It is sorted
        score_answertime : list
            List of dicts. Each dict has key 'Score' and 'ResponseTime' in hours. It is sorted
    Returns
    -------
        top10_post_views : list
            List of dicts. Only the Top 10
        mapped_tags : list
            List of dicts. Only the Top 10
        score_answertime : float
            Result of the average time to get an accepted answer of the top 200 questions by highest score (hour)
    """

    # Get the 10 most viewed posts
    top10_post_views = post_views[0:10]

    # Use a Counter to reduce the tags
    counter = collections.Counter()
    for d in mapped_tags:
        counter.update(d)

    # Transform the counter object to a dictionary
    tags_reduced = {}
    for key, value in counter.items():
        tags_reduced[key] = value

    # Sort the reduced tags and get the top 10
    tags_reduced = sorted(tags_reduced.items(), key=lambda x: x[1], reverse=True)[0:10]

    # Get the top 200 answers by score
    score_answertime = score_answertime[0:200]

    # Get the top 200 answers response_time
    answer_times = []
    for dict in score_answertime:
        answer_times.append(dict["ResponseTime"])

    # Calculate the average response time
    average_answer_time = mean(answer_times)

    return top10_post_views, tags_reduced, average_answer_time