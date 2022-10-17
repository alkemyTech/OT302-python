import time

from chunks import generate_chunks
from mapper import mapper
from shuffler import shuffler
from reducer import reducer
from big_data_logging import configured_logger
from save_into_file import save_into_file

logger = configured_logger()


def main():
    """Main function that calls the mapper, shuffler and reducer functions and then save to csv the results"""
    logger.info("Starting the main module...")

    start_time = time.time()

    # Create chunks
    chunks = generate_chunks()

    duration = time.time() - start_time
    logger.info(f"Took {round(duration,1)} seconds to finish the tasks")

    # Initialize lists for the results
    post_views_list = []
    mapped_tags_list = []
    score_answertime_list = []

    for chunk in chunks:
        # Use the mapper function to map the different tasks
        post_views, mapped_tags, score_answertime = mapper(chunk)

        # Append result to list of results
        post_views_list.append(post_views)
        mapped_tags_list.append(mapped_tags)
        score_answertime_list.append(score_answertime)

    duration = time.time() - start_time
    logger.info(f"Took {round(duration,1)} seconds to finish the tasks")

    # Make the lists of lists only one list
    post_views_list = sum(post_views_list, [])
    mapped_tags_list = sum(mapped_tags_list, [])
    score_answertime_list = sum(score_answertime_list, [])

    # Use the shuffler function to shuffle the mapped tasks
    post_views_list, mapped_tags_list, score_answertime_list = shuffler(
        post_views_list, mapped_tags_list, score_answertime_list
    )

    # Use the reducer function to reduce the shuffled tasksS
    top10_post_views, tags_reduced, average_answer_time = reducer(
        post_views_list, mapped_tags_list, score_answertime_list
    )

    save_into_file(top10_post_views, tags_reduced, average_answer_time)

    duration = time.time() - start_time
    logger.info(f"Took {round(duration,1)} seconds to finish the tasks")


if __name__ == "__main__":
    main()
