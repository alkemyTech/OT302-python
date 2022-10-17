import os
import pandas as pd


def save_into_file(top10_post_views, tags_reduced, average_answer_time):
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
