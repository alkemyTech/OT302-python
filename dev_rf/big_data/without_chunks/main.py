import pandas as pd
import time
import os

from mapper import mapper
from shuffler import shuffler
from reducer import reducer
from big_data_logging import configured_logger

logger = configured_logger()


def main():
    """Main function that calls the mapper, shuffler and reducer functions and then save to csv the results
    Returns
    -------
    csv files with the results
    """
    logger.info("Starting the main module...")

    start_time = time.time()
    # Use the mapper function to map the different tasks
    post_views, mapped_tags, score_answertime = mapper()
    # Use the shuffler function to shuffle the mapped tasks
    post_views, mapped_tags, score_answertime = shuffler(post_views, mapped_tags, score_answertime)
    # Use the reducer function to reduce the shuffled tasks
    top10_post_views, tags_reduced, average_answer_time = reducer(post_views, mapped_tags, score_answertime)

    # Create resultss folder if it doesnt exist
    cwd = os.getcwd()

    try:
        if not os.path.exists(cwd + "/results"):
            os.makedirs(cwd + "/results")
    except:
        print("Folder cannot be created")

    # Save to a csv the Top 10 most viewed posts
    top10_post_views_df = pd.DataFrame(top10_post_views)
    top10_post_views_df.to_csv("./results/Top10MostViewedPosts.csv", index=False)

    # Save to a csv the Top 10 tags
    tags_reduced_df = pd.DataFrame(tags_reduced, columns=["Tag", "Count"])
    tags_reduced_df.to_csv("./results/MostUsedTags.csv", index=False)

    # Save to a csv the Average time to get an accepted answer
    average_answer_time_serie = pd.Series(average_answer_time, name="Average time to get an accepted answer (hours)")
    average_answer_time_serie.to_csv("./results/AverageAnswerTime.csv", index=False)

    duration = time.time() - start_time
    logger.info(f"Took {round(duration,1)} seconds to finish the tasks")


if __name__ == "__main__":
    main()
