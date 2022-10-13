from big_data_logging import configured_logger

logger = configured_logger()


def shuffler(post_views, mapped_tags, score_answertime):
    """Function to shuffle the results that come from mapper

    Parameters and Returns (sorted)
    -------
        post_views : list
            List of dicts. Each dict has key 'Id' and 'ViewCount'.
        mapped_tags : list
            List of dicts. Each dict has as key and tag and a value 1.
        score_answertime : list
            List of dicts. Each dict has key 'Score' and 'ResponseTime' in hours.

    """

    logger.info("Starting the shuffler module...")

    # Sort post_views from higher views to lower views
    post_views = sorted(post_views, key=lambda x: x["ViewCount"], reverse=True)

    # Sort word_tags by alphabetical order
    mapped_tags = sorted(mapped_tags, key=lambda d: list(d.keys()))

    # Sort score_answertime from higher score to lower score
    score_answertime = sorted(score_answertime, key=lambda x: x["Score"], reverse=True)

    return post_views, mapped_tags, score_answertime
