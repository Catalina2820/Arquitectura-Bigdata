Summary
=======

Tag Genome is a data structure containing scores indicating the degree to which tags apply to items, such as movies or books. This dataset contains a Tag Genome generated for a set of books along with the data used for its generation (raw data). Raw data consists of a subset of the Goodreads dataset [Wan and McAuley, 2018, Wan et al., 2019] and book-tag ratings. The Goodreads subset includes information on popular books, such as titles, authors, release years, user ratings, reviews and shelves. Shelves are lists that users use to organize books in Goodreads (https://www.goodreads.com/). In these instructions, we refer to adding books to shelves as attaching tags (shelf names) to books. To collect book-tag ratings, we conducted a survey on Amazon Mechanical Turk, where we asked users to indicate degree to which tags apply to books from this subset. To generate book-tag scores, we used two state-of-the-art algorithms: Glmer [Vig et al., 2012] and TagDL [Kotkov et al., 2021]. The code is available in the following GitHub repository: https://github.com/Bionic1251/Revisiting-the-Tag-Relevance-Prediction-Problem

This dataset can be used for single domain tasks, such as critiquing or describing books with tags [Vig et al., 2012], as well as cross-domain tasks, such as recommending books to users based on their movie preferences. To support cross-domain tasks, this dataset has 512/727 book-tags overlapping with the Tag Genome dataset for movies [Vig et al., 2012, Kotkov et al., 2021].

The structure of this dataset partly follows the structure of the data folder in the GitHub repository above. This dataset contains raw input data for the algorithms, features generated based on these data, evaluation results of the prediction algorithms, Tag Genome relevance scores and movie-book tag correspondences. The structure of the dataset is as follows:

│
├─ matching.csv – correspondence of tags in this dataset to tags in Tag Genome for movies (MovieLens (https://movielens.org/))
│
├─── raw – raw book data
│   ├─metadata.json – book data, such as authors and release year
│   ├─ratings.json – ratings user gave to books in Goodreads
│   ├─reviews.json –reviews from Goodreads
│   ├─survey_answers.json – user answers to the survey questions regarding the degree, to which a tag applies to a book
│   ├─tag_count.json – numbers of times users attached tags to books
│   └─tags.json – tags and tag ids
│
├─── scores – Tag Genome scores that indicate degree, with which tags apply to books
│   ├─glmer.csv – scores generated based on Glmer [Vig et al., 2012]
│   └─tagdl.csv - scores generated based on TagDL [Kotkov et al., 2021]
│
├─── processed
│   ├─features_r.csv - user survey answers along with book-tag features
│   └─10folds – features_r.csv split into 10 folds
│     ├─test0.csv – test for fold 0
│     ├─...
│     ├─test9.csv – test for fold 9
│     ├─train0.csv – train for fold 0
│     ├─...
│     └─train9.csv – train for fold 9
│
└─── predictions – results of the regression [Vig et al., 2012] and the TagDL [Kotkov et al., 2021] prediction algorithms
    ├─performance_results_tenfolds.txt – mean absolute error summary
    ├─tagdl_predictions_fold_0.txt – predictions of TagDL for fold 0
    ├─...
    ├─tagdl_predictions_fold_9.txt – predictions of TagDL for fold 9
    ├─glmer_predictions_fold_0.txt – predictions of regression for fold 0
    ├─...
    └─ glmer_predictions_fold_9.txt – predictions of regression for fold 9



Usage License
=============

This work is licensed under the Creative Commons Attribution-NonCommercial 3.0 License.

Citation
========

To acknowledge use of the dataset in publications, please cite the following paper:

@inproceedings{kotkov2022tag,
author = {Kotkov, Denis and Medlar, Alan and Maslov, Alexandr and Satyal, Umesh Raj and Neovius, Mats and Glowacka, Dorota}, 
title = {The Tag Genome Dataset for Books}, 
year = {2022}, 
isbn = {978-1-4503-9186-3/22/03}, 
doi = {10.1145/3498366.3505833},
booktitle = { Proceedings of the 2022 ACM SIGIR Conference on Human Information Interaction and Retrieval (CHIIR '22)},
numpages = {5}
}

Acknowledgements
========================
We would like to thank Mengting Wan for allowing us to publish this dataset and organizations that supported publication of this dataset: the Academy of Finland, grant #309495 (the LibDat project) and the Academy of Finland Flagship programme: Finnish Center for Artificial Intelligence FCAI.

Files
========================
The dataset files contain json objects (one line per object). The files are encoded as UTF-8. User ids (user_id), tag ids (tag_id) and book ids (item_id) are consistent across all files of the dataset.

matching.csv
--------
The file contains correspondence of tags in this dataset with tags from MovieLens. We merged tags based on their names and manually added a few correspondences, such as tags that differed only in hyphens (“-”). The file contains 1,343 lines, out of which 512 lines indicate correspondence (616 movie-tags and 215 book-tags are missing corresponding tags). The file has the csv format and the following fields:
movie_tag – MovieLens tag (1,128 unique)
book_tag – book tag (727 unique)
Example lines:
movie_tag,book_tag
007,
007 (series),
18th century,18th century


Folder `raw`
--------
The folder contains raw book data.


raw/metadata.json
--------
The file contains information about books from Goodreads – 9,374 lines of json objects that have the following fields:
item_id – book id, which is consistent across files (9,374 unique ids)
url – link to the book page at the Goodreads website
title – book title (9,348 unique titles)
authors – book authors
lang – book language
img – link to an image of book cover
year – book release year
description – book description
pop - remove
Example line:
{'item_id': 16827462, 'url': 'https://www.goodreads.com/book/show/11870085-the-fault-in-our-stars', 'title': 'The Fault in Our Stars', 'authors': 'John Green', 'lang': 'eng', 'img': 'https://images.gr-assets.com/books/1360206420m/11870085.jpg', 'year': 2012, 'description': 'There is an alternate cover edition \x01.\n"I fell in love the way you fall asleep: slowly, then all at once."\nDespite the tumor-shrinking medical miracle that has bought her a few years, Hazel has never been anything but terminal, her final chapter inscribed upon diagnosis. But when a gorgeous plot twist named Augustus Waters suddenly appears at Cancer Kid Support Group, Hazel\'s story is about to be completely rewritten.\nInsightful, bold, irreverent, and raw, The Fault in Our Stars is award-winning author John Green\'s most ambitious and heartbreaking work yet, brilliantly exploring the funny, thrilling, and tragic business of being alive and in love.'}


raw/reviews.json
--------
The file contains 5,307,626 lines of book reviews from the Goodreads website. The json objects have the following fields:
item_id – book id (9,374 unique ids)
txt – review text
Example line:
{'item_id': 41335427, 'txt': 'Annual re-read 2013. I liked HBP much more this time. \n Annual re-read 2017.'}


raw/tags.json
--------
The file contains 727 lines of json objects with the following fields:
tag – tag string (727 unique)
id – tag id (727 unique)
Example line:
{'tag': '18th century', 'id': 0}


raw/tag_count.json
--------
The file contains numbers of times Goodreads users attached tags to books (added books to shelves). The file contains 239,252 lines of json objects with the following fields:
item_id – book id (9,374 unique ids)
tag_id – tag id (727 unique ids)
num – number of times users have attached the tag to the book
Example line:
{'item_id': 115, 'tag_id': 13, 'num': 52}


raw/ratings.json
--------
The file contains ratings that users assigned to books in Goodreads. Each rating represents the degree, to which the user enjoyed reading the book. Each rating corresponds to a number of stars from 1 till 5 with the granularity of 1 star. The higher the rating, the higher the enjoyment. The file contains 5,152,656 lines of json objects with the following fields:
item_id – book id (9,374 unique ids)
user_id – user id (350,332 unique ids)
rating – the number of stars
Example line:
{'item_id': 41335427, 'user_id': 0, 'rating': 5}


raw/survey_answers.json
--------
The file contains ratings Amazon Mechanical Turk users gave to book-tag pairs in the survey. The users were asked to indicate the degree, to which a tag applies to a book on a 5-point scale from the tag not applying at all (1 point) to applying very strongly (5 points). Users could also indicate that they are not sure about the degree (the -1 value). The file contains 
145,825 lines of json objects with the following fields:
user_id – user id (986 unique ids)
item_id – book id (2,535 unique ids)
tag_id – tag id (727 unique ids)
score – book-tag rating, which takes values: 1 (`not at all`), 2, 3, 4, 5 (`very much`) and -1 (`not sure`)
Example line:
{'user_id': 2, 'item_id': 604666, 'tag_id': 151, 'score': 5}


Folder `processed`
--------
The folder contains features generated based on the raw book data.

processed/features_r.csv
--------
The file contains features of book-tag pairs along with user survey answers with excluded `not sure` (-1) ratings. These features are calculated based on the raw data and used to generate book-tag scores of Tag Genome by Glmer and TagDL. The file has 112,887 lines in the csv format with the following fields:
tag – tag string (727 unique tags)
item_id – book id (2,500 unique ids)
log_goodreads – log of frequency with which the corresponding tag appears in text reviews of the book
log_ goodreads_nostem – the same as the previous field, but without stemming
rating_similarity - cosine similarity between ratings of the corresponding book and aggregated ratings of books tagged with the tag
avg_rating – average Goodreads rating of the book
tag_exists - 1 if the tag has been applied to the book and 0 otherwise
lsi_tags_75 - similarity between the tag and the book using latent semantic indexing, where each document is the set of tags applied to the book
lsi_imdb_175 - similarity between the tag and the book using latent semantic indexing, where each document is the set of words in user reviews of the book
tag_prob – the score predicted by a regression model using tag_exists as the output variable and the other features as the input variables
targets – user answers to survey questions
Example lines:
tag,item_id,log_IMDB,log_IMDB_nostem,rating_similarity,avg_rating,tag_exists,lsi_tags_75,lsi_imdb_175,tag_prob,targets
18th century,18538602,-0.0799393859902254,-0.186729441547532,-0.776866238485761,0.677924051877647,1.83056878346075,2.41586049711819,-0.0974751113798336,-0.8004779541726,5
18th century,16657990,-0.0799393859902254,-0.186729441547532,2.7494591362046,1.31154580111925,1.83056878346075,3.29322483282867,-0.0974751113798336,1.32821602261498,5
18th century,3123704,-0.0799393859902254,-0.186729441547532,3.27267583198486,0.630262238881599,1.83056878346075,3.47877553355663,-0.0974751113798336,1.75152187523117,5


Folder `processed/10folds`
--------
This folder contains 10-fold split of features_r.csv


Folder `scores`
--------
The folder contains Tag Genome scores that indicate degrees, with which tags apply to books. To generate these files, we used the two state-of-the-art algorithms: Glmer [Vig et al., 2012] and TagDL [Kotkov et al., 2021]. In case these files are missing necessary books or tags, you can generate them with the code provided in the GitHub repository (see the link above).

scores/glmer.csv
--------
The file contains 6,814,898 Tag Genome scores between 0 and 1, which have been generated with Glmer [Vig et al., 2012]. The file has the csv format and contains the following fields:
tag - tag string (727 unique tags)
item_id - book id (9,374 unique ids)
score - degree, with which the tag applies to the book
Example lines:
tag,item_id,score
samurai,16416771,0.0344478696960333
samurai,23756807,0.0388265545424338
samurai,24248331,0.0146165607565562


scores/tagdl.csv
--------
The file contains 6,814,898 Tag Genome scores between -0.2 and 1.18 (due to the algorithm design). The scores have been generated with TagDL [Kotkov et al., 2021]. Various strategies can be applied for score normalization if necessary. The file has the csv format and contains the following fields:
tag - tag string (727 unique tags)
item_id - book id (9,374 unique ids)
score - degree, with which the tag applies to the book
Example lines:
tag,item_id,score
samurai,16416771,0.00024017692000000002
samurai,23756807,0.0001886487
samurai,24248331,8.34465e-06


Folder `predictions`
--------
The folder contains predictions of algorithms based on book-tag features. Files from tagdl_predictions_fold_0.txt to tagdl_predictions_fold_9.txt contain predictions of TagDL, while files from glmer_predictions_fold_0.txt to glmer_predictions_fold_9.txt contain predictions of the regression model. The prediction scores in these files correspond to test files in the 10folds folder. The file performance_results_tenfolds.txt contains mean absolute errors for each fold.

References
========
[Kotkov et al., 2021] Kotkov, D., Maslov, A., & Neovius, M. (2021). Revisiting the Tag Relevance Prediction Problem.
[Kotkov et al., 2022] Kotkov, D., Medlar, A., Maslov A., Raj Satyal U., Neovius M., and Glowacka D. (2022). The Tag Genome Dataset for Books. In Proceedings of the 2022 Conference on Human Information Interaction and Retrieval
[Vig et al., 2012] Vig, J., Sen, S., & Riedl, J. (2012). The tag genome: Encoding community knowledge to support novel interaction. ACM Transactions on Interactive Intelligent Systems (TiiS), 2(3), 1-44.
[Wan and McAuley, 2018] Wan, M., and McAuley, J. (2018, September). Item recommendation on monotonic behavior chains. In Proceedings of the 12th ACM conference on recommender systems (pp. 86-94).
[Wan et al., 2019] Wan, M., Misra, R., Nakashole, N., and McAuley, J. (2019). In Proceedings of the 57th Conference of the Association for Computational Linguistics, {ACL} 2019, Florence, Italy, July 28- August 2, 2019, Volume 1: Long Papers

