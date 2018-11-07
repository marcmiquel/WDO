# WCDO

The [__Wikipedia Cultural Diversity Obsevatory (WCDO)__](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory) is a research project whose purpose is to raise awareness on Wikipedia’s current state of cultural diversity, __(1) providing datasets__, __(2) sites with visualizations and statistics__, and __(3) pointing out solutions to improve__ intercultural coverage and __knowledge inequalities__ among languages and geographical places.


## Data: Cultural Context Content datasets and WCDO stats
Cultural Context Content is the group of articles in a Wikipedia language edition that relates to the editors' geographical and cultural context (places, traditions, language, politics, agriculture, biographies, events, etcetera.). Therefore, they are articles related to the territories where the language is spoken because it is indigenous or it is official.

The [method](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) to obtain this group of articles is divided into two steps.

* `language_territories_mapping.py` creates the first version of the database language_territories_mapping.csv with the territories that speak a language because it is either official or native.
* `ccc_selection.py` uses this database as a reference, retrieves and processes data from  __Wikidata JSON dump__ and the Wikipedia langauge editions databases __(MySQL replicas)__ in order to create the final CCC dataset.

The method is build with:
- [Python 3](https://www.python.org/download/releases/3.0/) - To manage the data.
- [Sqlite 3](https://www.sqlite.org/) - To store the data.
- [Scikit-learn](https://scikit-learn.org) - To process the data.

The datasets are generated on a monthly basis at wcdo.wmflabs.org in CSV [(more info)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content#Datasets).
One sample of the generated CCC datasets is stored in the [datasets_sample folder](https://github.com/marcmiquel/WCDO/tree/master/datasets_sample), and the historical archive is in [wcdo.wmflabs.org/datasets](http://wcdo.wmflabs.org/datasets/).

In order to be able to answer questions on Wikipedia cultural diversity, it is necessary to compute several statistics based on CCC and other groups of articles.
* `stats_generation.py` computes these statistics and ranks the articles in order to create valuable lists of articles for each Wikipedia language edition. It stores the results in `wcdo_stats.db` on a monthly basis so it can be used to create tables and graphs.

## Site(s): Meta page (WCDO home) and external website (WCDO visualizations)
These are the scripts that create the tables and visualizations for the WCDO, both the meta page and the external website visualizations.

* `meta_updates.py` presents most of the results through tables in the [WCDO meta pages](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory), with results for all languages and for each individually. This is done using [Pywikibot](https://www.mediawiki.org/wiki/Manual:Pywikibot) - To post and update mediawiki pages.

* `dash_apps.py` creates all the tables and visualizations which are available at [wcdo.wmflabs.org](http://wcdo.wmflabs.org).
It uses [Dash](https://plot.ly/products/dash/) and Plotly.

## Research: Main papers and presentations
So far one paper have been published and several talks have been given on the usefulness of a Cultural Context Content dataset and the importance of exchanging content across languaeg editions in order to reduce the knowledge inequalities.
* Miquel-Ribé, M., & Laniado, D. (2018). Wikipedia Culture Gap: Quantifying Content Imbalances Across 40 Language Editions. Frontiers in Physics ([pdf](research_publications/mmiquel_laniado_ccc_gaps.pdf)).
* Miquel Ribé, M. (2017) Identity-based motivation in digital engagement: the influence of community and cultural identity on participation in Wikipedia (Doctoral dissertation, Universitat Pompeu Fabra).
* Presentation Wikipedia Cultural Diversity Observatory (WCDO) (Wikimania 2018, Cape Town) ([pdf](research_publications/project_wcdo_presentation.pdf)). 

## Community
Get involved in WCDO development and find tasks to do in [Get involved page](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Get_involved) or you can get in touch at [tools.wcdo@tools.wmflabs.org](mailto:tools.wcdo@tools.wmflabs.org).

## Copyright
All data, charts, and other content is available under the [Creative Commons CC0 dedication](https://creativecommons.org/publicdomain/zero/1.0/).
