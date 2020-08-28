# WDO
==========

The [__Wikipedia Diversity Obsevatory (WDO)__](https://meta.wikimedia.org/wiki/Wikipedia_Diversity_Observatory) is a research project whose purpose is to raise awareness on Wikipedia’s current state of content diversity, __(1) providing datasets__, __(2) sites with visualizations and statistics__, and __(3) pointing out solutions to improve__ knowledge coverage and __knowledge inequalities__ among languages and categories relevant to overall diversity (e.g. culture, gender, geography, ethnic groups, sexual orientation, etc.).

You can learn more about the project in this [video](https://www.youtube.com/watch?v=PdqDZ9vRQEw).


## Data: Wikipedia Diversity and Stats Databases
We created a dataset/database table for each Wikipedia language edition in which each article is characterized according to features that can determine whether it belongs to a relevant category for diversity (culture, gender, place, etc.). Categories like gender, sexual orientation, religion or ethnic origin are straightforward, as they can be traced to Wikidata semantic relations structured as properties and items. 

Instead, the relationship from an article as belonging to the language’s related topics requires a more sophisticated method. In this case, we use a variety of features based on the article title, category and links graph structure, among others, to label each article according to the possible relationship with territories where the language is spoken and to the peoples that inhabit them. Then, we introduce all of them into a machine learning classifier to obtain the final selection of articles belonging to a language cultural and geographical context. This collection of articles is called Cultural Context Content (CCC) adn it is the group of articles in a Wikipedia language edition that relates to the editors' geographical and cultural context (places, traditions, language, politics, agriculture, biographies, events, etcetera.).

The method is build with:
- [Python 3](https://www.python.org/download/releases/3.0/) - To manage the data.
- [Sqlite 3](https://www.sqlite.org/) - To store the data.
- [Scikit-learn](https://scikit-learn.org) - To process the data.

The datasets/database tables are generated on a monthly basis at wcdo.wmflabs.org in CSV and SQLite3. You can download the last version in [datasets](http://wcdo.wmflabs.org/datasets/) or [databases](http://wcdo.wmflabs.org/databases/).

These are the scripts that generate the database [wikipedia_diversity.db](https://wcdo.wmflabs.org/databases/wikipedia_diversity.db) we created the following scripts:  

* `wikipedia_diversity.py`, `content_retrieval.py` and `content_selection.py`. they retrieve the data from Wikimedia dumps and databases, process them according to some criteria, and introduce them into the database.

To answer questions on Wikipedia content diversity, it is necessary to compute several statistics based on CCC and other groups of articles. This is the script we used to calculate them:

* `stats_generation.py` computes these statistics and ranks the articles in order to create valuable lists of articles for each Wikipedia language edition. It stores the results in [`stats.db`](https://wcdo.wmflabs.org/databases/stats_production.db) on a monthly basis so it can be used to create tables and graphs.
The list of all the diversity categories and groups of articles is in this Excel file [sets_intersections.xls](https://github.com/marcmiquel/WCDO/blob/wcdo/docs/sets_intersections.xlsx)

## Site(s): Observatory website (WDO) and Meta page (WDO home)
These are the scripts that create the tables and visualizations for the WDO, both the website visualizations and tools and the meta page.

* `dash_apps.py` creates all the tables and visualizations which are available at (wcdo.wmflabs.org)[http://wcdo.wmflabs.org].
It uses [Dash and Plotly](https://dash.plotly.com/).

* `meta_updates.py` presents most of the results through tables in the (WDO meta pages)[https://meta.wikimedia.org/wiki/Wikipedia_Diversity_Observatory], with results for all languages and for each individually. This is done using [Pywikibot](https://www.mediawiki.org/wiki/Manual:Pywikibot) - To post and update mediawiki pages.


## Research: Main papers and presentations
Several papers and talks have been published to explain the usefulness of the Diversity Observatory and the importance of exchanging content across languaeg editions in order to reduce the knowledge inequalities.

* Miquel-Ribé, M. & Laniado, D. (2020). The Wikipedia Diversity Observatory: A Project to Identify and Bridge Content Gaps in Wikipedia. In Proceedings of the International Symposium on Open Collaboration (OpenSym 2020). ACM, New York, NY, USA, 2 pages. https://doi.org/10.1145/3412569.3412866 ([pdf](https://github.com/marcmiquel/WDO/blob/master/research/mmiquel_laniado_diversity_observatory.pdf)).
* Miquel-Ribé, M., & Laniado, D. (2019). Wikipedia Cultural Diversity Dataset: A Complete Cartography for 300 Language Editions. Proceedings of the 13th International AAAI Conference on Web and Social Media. ICWSM. ACM. 2334-0770
* Miquel-Ribé, M., & Laniado, D. (2018). Wikipedia Culture Gap: Quantifying Content Imbalances Across 40 Language Editions. Frontiers in Physics ([pdf](https://github.com/marcmiquel/WDO/blob/master/research/mmiquel_laniado_ccc_gaps.pdf)).
* Miquel Ribé, M. (2017) Identity-based motivation in digital engagement: the influence of community and cultural identity on participation in Wikipedia (Doctoral dissertation, Universitat Pompeu Fabra).

* Presentation Wikipedia Diversity Observatory (WDO) (OpenSym Conference, 2020) 
([Video Youtube](https://www.youtube.com/watch?v=PdqDZ9vRQEw))
([Slides pdf](https://github.com/marcmiquel/WDO/blob/master/research/presentation_wikipedia_diversity_observatory.pdf)). 

## Community
Get involved in WDO development and find tasks to do in [Get involved page](https://meta.wikimedia.org/wiki/Wikipedia_Diversity_Observatory/Get_involved) or you can get in touch at [tools.wdo@tools.wmflabs.org](mailto:tools.wdo@tools.wmflabs.org).

## Copyright
All data, charts, and other content is available under the [Creative Commons CC0 dedication](https://creativecommons.org/publicdomain/zero/1.0/).
