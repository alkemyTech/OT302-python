import xml.etree.ElementTree as ET
import datetime

def mapper():
    """Function used to map the 3 required tasks:
        - 1 - Top 10 posts views
        - 2 - Top 10 words in tags
        - 3 - Score and answer time
    Returns
    -------
        post_views : list
            List of dicts. Each dict has key 'Id' and 'ViewCount'.
        mapped_tags : list
            List of dicts. Each dict has as key and tag and a value 1.
        score_answertime : list
            List of dicts. Each dict has key 'Score' and 'ResponseTime' in hours.
    """
    
    def get_answer_dict(root):
        """Function to get the answers ids with the creation dates
        Parameters
        ----------
            root : object
                Element Tree root object
        Returns
        -------
            answer_dict : dict
                Dictionary that has as key the answer_id and as value the creation_date
        """

        # Initialize variable
        answer_dict = {}

        # Loop into each row to get the answer_id and creation_date
        for child in root:
            dict = child.attrib

            # PostTypeId == 2 means that it is an answer. PostTypeId == 1 means is a question.
            if dict["PostTypeId"] == "2":
                answer_dict[dict["Id"]] = dict["CreationDate"]

        return answer_dict
    
    # ./112010 Meta Stack Overflow/posts.xml
    # Load and parse the posts.xml file
    tree = ET.parse(r"D:\Alkemy\dev_jb\big_data\112010 Stack Overflow\posts.xml")
    # Get the root of the xml
    root = tree.getroot()

    # Initialize variables
    post_views = []
    mapped_tags = []
    score_answertime = []

    # Get the answer dict. key=answer_id, value=CreationDate
    answer_dict = get_answer_dict(root)

    # Loop into each row element
    for child in root:
        # Get the attributes of each row element
        dict = child.attrib

        # 1 - Top 10 posts views
        # Append to the list the post_id and the view_count of each post
        post_views.append({"Id": dict["Id"], "ViewCount": int(dict["ViewCount"])})

        # 2 - Top 10 words in tags
        # If the post has a tag replace the <> and split to get the different words.
        try:
            tags = dict["Tags"].replace("<", " ").replace(">", " ").strip().split()
            # Map each individual tag
            for tag in tags:
                mapped_tags.append({tag: 1})
        except:
            # If the post hasn't a tag then continue
            continue

        # 3 - Score and answer time
        # If the post is a question
        if dict["PostTypeId"] == "1":
            # Get question score and creation_time
            post_score = int(dict["Score"])
            post_creation_time = datetime.datetime.fromisoformat(dict["CreationDate"])
            try:
                # Some posts haven't an accepted answer, so they will be skipped as they do not have an AcceptedAnswerId

                # Get the accepted_answer_id
                accepted_answer_id = dict["AcceptedAnswerId"]

                # With the accepted_answer_id go to the answer_dict and take the creation_date value and transform it to datetime
                accepted_answer_time = datetime.datetime.fromisoformat(answer_dict[accepted_answer_id])

                # Calculate response time from question creation to accepted answer creation (in hours)
                response_time = round((accepted_answer_time - post_creation_time).seconds / 3600, 2)

                # Append the score and response time to a list of dicts
                score_answertime.append({"Score": post_score, "ResponseTime": response_time})
            except:
                continue

    return post_views, mapped_tags, score_answertime