

# WCDO
The [Wikipedia Cultural Diversity Obsevatory (WCDO)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory) is a research project whose purpose is to raise awareness on Wikipedia’s current state of cultural diversity, (1) providing datasets, (2) sites with visualizations and statistics, and (3) pointing out solutions to improve intercultural coverage and knowledge inequalities among languages and geographical places.



## Data: Cultural Context Content datasets

Cultural Context Content is...

done with:
- Wikidata dumps, MySQL Replicas from Wikipedia language editions


what is CCC. LINK
* ccc_selection.py
* language_territories_mapping.py

uses python, sqlite3
explicar les bases de dades que utilitzen (SQLITE).

you can download the datasets here: LINK.
or.... in this github there is a sample.

what are the STATS BY WCDO (intersections, increments, rankings). LINK.
 wcdo_stats.db ->
* stats_generation.py





## Site: Meta page and Vital article lists

__Built with__


- [Python](https://vuejs.org/) - The web framework used
- [d3](https://d3js.org/) - Version 4+ for visualizations
- [CrossFilter](https://github.com/crossfilter/crossfilter) - For exploring large multivariate datasets in the browser

* meta_update.py
uses pywikibot, bokeh.

* lists_app.py

uses flask.



## Resarch: Papers and presentations

So far one paper have been published and several talks have been given on the usefulness of a Cultural Context Content dataset and the importance of exchanging content across languaeg editions in order to reduce the knowledge inequalities.

- 
- 

* Miquel-Ribé, M., & Laniado, D. (2018). Wikipedia Culture Gap: Quantifying Content Imbalances Across 40 Language Editions. Frontiers in Physics ([pdf]).


* presentation: (wikimania)



## Community
Get involved in WCDO development and find tasks to do in [Get involved page](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Get_involved) or 

or 

Get updates on Wikistats 2.0 development and chat with the project maintainers and community members.
- Chat with community members on [IRC](https://webchat.freenode.net/) server, in the `#wikimedia-analytics` channel.





## Copyright
All data, charts, and other content is available under the [Creative Commons CC0 dedication](https://creativecommons.org/publicdomain/zero/1.0/).
