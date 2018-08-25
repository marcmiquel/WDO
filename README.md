# WCDO
======

The [__Wikipedia Cultural Diversity Obsevatory (WCDO)__](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory) is a research project whose purpose is to raise awareness on Wikipedia’s current state of cultural diversity, __(1) providing datasets__, __(2) sites with visualizations and statistics__, and __(3) pointing out solutions to improve__ intercultural coverage and __knowledge inequalities__ among languages and geographical places.


## Data: Cultural Context Content datasets
Cultural Context Content is the group of articles in a Wikipedia language edition that relates to the editors' geographical and cultural context (places, traditions, language, politics, agriculture, biographies, events, etcetera.). Therefore, they are articles related to the territories where the language is spoken because it is indigenous or it is official.

The [method](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) to obtain this group of articles is divided into two steps.

* `language_territories_mapping.py` creates the first version of the database language_territories_mapping.csv with the territories that speak a language because it is either official or native.
* `ccc_selection.py` retrieves and processes data from  __Wikidata JSON dump__ and the Wikipedia langauge editions databases __(MySQL replicas)__ in order to create the final dataset.

They use:
- [Python 3](https://www.python.org/download/releases/3.0/) - To manage the data.
- [Scikit-learn](https://scikit-learn.org) - To process the data.
- [Sqlite 3](https://www.sqlite.org/) - To store the data.

The datasets are generated on a monthly basis at wcdo.wmflabs.org in CSV and this data format.
One sample of the generated CCC datasets is stored in this folder.













## Site: Meta page and Vital article lists

__Built with__


or.... in this github there is a sample.

what are the STATS BY WCDO (intersections, increments, rankings). LINK.
 wcdo_stats.db ->
* stats_generation.py



- [Python](https://vuejs.org/) - The web framework used
- [d3](https://d3js.org/) - Version 4+ for visualizations
- [CrossFilter](https://github.com/crossfilter/crossfilter) - For exploring large multivariate datasets in the browser

* meta_update.py
uses pywikibot, bokeh.

* lists_app.py

uses flask.



## Resarch: Papers and presentations
So far one paper have been published and several talks have been given on the usefulness of a Cultural Context Content dataset and the importance of exchanging content across languaeg editions in order to reduce the knowledge inequalities.
* Miquel-Ribé, M., & Laniado, D. (2018). Wikipedia Culture Gap: Quantifying Content Imbalances Across 40 Language Editions. Frontiers in Physics ([pdf](https://github.com/marcmiquel/WCDO/blob/master/research/mmiquel_laniado_ccc_gaps.pdf)).
* Presentation Wikipedia Cultural Diversity Observatory (WCDO) (Wikimania 2018, Cape Town) ([pdf](https://github.com/marcmiquel/WCDO/blob/master/research/project_wcdo_presentation.pdf)). 

## Community
Get involved in WCDO development and find tasks to do in [Get involved page](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Get_involved) or you can get in touch at [tools.wcdo@tools.wmflabs.org](mailto:tools.wcdo@tools.wmflabs.org).

## Copyright
All data, charts, and other content is available under the [Creative Commons CC0 dedication](https://creativecommons.org/publicdomain/zero/1.0/).
