# -*- coding: utf-8 -*-



def update_top_diversity_articles_interwiki():

    conn = sqlite3.connect(databases_path + top_diversity_production_db); cursor = conn.cursor()
    print ('* update_top_diversity_articles_interwiki')

    for languagecode in wikilanguagecodes:       
        print (languagecode)
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        list_page_ids = set()
        page_ids_qitems = {}        
        query = 'SELECT DISTINCT page_title_original, qitem FROM '+languagecode+'wiki_top_articles_features'

        try:
            for row in cursor.execute(query):
    #            print (row[0],row[1])

                qitem=row[1]
                try:
                    page_id = page_titles_page_ids[qitems_page_titles[qitem]]
                except:
                    continue
                list_page_ids.add(page_id)
                page_ids_qitems[page_id]=qitem
        except:
            print ('This language has no table.')
            continue

        if len(page_ids_qitems) == 0: continue
        print (len(page_ids_qitems))

        page_asstring = ','.join( ['%s'] * len( list_page_ids ) )
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        query = 'SELECT ll_from, ll_lang, ll_title FROM langlinks WHERE ll_from IN (%s);' % page_asstring

        mysql_cur_read.execute(query,list_page_ids)
        rows = mysql_cur_read.fetchall()

        old_page_id = 0
        i=0
        for row in rows:
            i+=1
            page_id=row[0]
            target_lang = row[1].decode('utf-8').replace('-','_')
#            if target_lang == 'be_x_old': target_lang = 'be_tarask'
#            if target_lang == 'zh_min_nan': target_lang = 'nan'

            target_title = row[2]
            qitem = page_ids_qitems[page_id]

            if old_page_id != page_id and old_page_id != 0:
                # TAULA de features (num_interwiki)
                query = 'UPDATE '+languagecode+'wiki_top_articles_features SET num_interwiki = ?, measurement_date = ? WHERE qitem=?'
                cursor.execute(query,(i,measurement_date,old_qitem));
                i=0

            try:
                query = 'INSERT OR IGNORE INTO '+target_lang+'wiki_top_articles_page_titles (measurement_date, page_title_target, generation_method, qitem) VALUES (?,?,?,?)'
                cursor.execute(query, (measurement_date,target_title,'sitelinks',qitem));

                query = 'UPDATE '+target_lang+'wiki_top_articles_page_titles SET measurement_date = ?, page_title_target = ?, generation_method = ? WHERE qitem=?'
                cursor.execute(query, (measurement_date,target_title,'sitelinks',qitem));
            except:
                pass

            old_page_id = page_id
            old_qitem = qitem

        conn.commit()


    for languagecode in wikilanguagecodes:
        print (languagecode)
        try:
            query = 'UPDATE '+languagecode+'wiki_top_articles_page_titles SET measurement_date = "'+measurement_date+'"'
            cursor.execute(query)
            query = 'UPDATE '+languagecode+'wiki_top_articles_features SET measurement_date = "'+measurement_date+'"'
            cursor.execute(query)
            conn.commit()
        except:
            pass




def update_top_diversity_articles_intersections():

    conn4 = sqlite3.connect(databases_path + top_diversity_production_db); cursor4 = conn4.cursor()

    for x in ['mnw', 'nqo', 'ban', 'gcr', 'szy']: wikilanguagecodes.remove(x)

    for languagecode_1 in wikilanguagecodes:
        print ('\n* '+languagecode_1)
        langTime = time.time()
        intersections = list()

        qitems_page_titles = {}
        query = 'SELECT qitem, page_title_target FROM '+languagecode_1+'wiki_top_articles_page_titles WHERE generation_method ="sitelinks";'
        for row in cursor4.execute(query):
            qitem = row[0]
            page_title_target = row[1]
            qitems_page_titles[qitem]=page_title_target
        print ('This language has '+str(len(qitems_page_titles))+' articles.')

        lang_art = set(qitems_page_titles.keys())


        titles = list()
        count_intersections = 0
        for languagecode_2 in wikilanguagecodes:
            languagecode_2_qitems = {}
            query = 'SELECT qitem, country, list_name, position FROM '+languagecode_2+'wiki_top_articles_lists WHERE position <= 100 ORDER BY country, list_name, position ASC'

            count = 0
            list_name = 'initial'
            country = ''
            position = 0

            country_language_qitems = set()

            for row in cursor4.execute(query): 
                qitem = row[0]
                cur_country = row[1]
                cur_list_name = row[2]
                cur_position = row[3]


                # INTERSECTIONS
                # list level
                if cur_list_name != list_name and list_name!='initial':

                    if country != 'all': list_origin = country+'_('+languagecode_2+')'
                    else: list_origin = languagecode_2

                    if position < 100: base = position
                    else: base = 100

                    intersections.append((count,100*count/base, measurement_date,list_origin,list_name,languagecode_1,'wp')) # second field: ca_(ca)
#                    print ((list_origin,list_name,languagecode_1,'wp',count,100*count/base, measurement_date))
                    count = 0

                # country level
                if cur_country != country and country != '':
                    coincident_qitems_all_qitems = len(country_language_qitems.intersection(lang_art))
                    list_origin = ''
                    if country != 'all': 
                        list_origin = country+'_('+languagecode_2+')'
                    else: 
                        list_origin = languagecode_2

                    if len(country_language_qitems) == 0: rel_value = 0
                    else: rel_value = 100*coincident_qitems_all_qitems/len(country_language_qitems)
                    intersections.append((coincident_qitems_all_qitems,rel_value, measurement_date,list_origin,'all_top_ccc_articles',languagecode,'wp'))
                    country_language_qitems = set()

                country_language_qitems.add(qitem)

                # titles
                try:
                    page_title=qitems_page_titles[qitem]
                    count+=1 # for intersections
                except:
                    pass

                position = cur_position
                country = cur_country
                list_name = cur_list_name


            # LAST ITERATION
            # list level
            if list_name!='initial':
                if country != 'all' and country != '': 
                    list_origin = country+'_('+languagecode_2+')'
                else: list_origin = languagecode_2

                if position < 100: base = position
                else: base = 100

                if base != 0:
                    rel_value = 100*count/base
                else:
                    rel_value = 0

                intersections.append((count,rel_value, measurement_date,list_origin,list_name,languagecode_1,'wp')) # second field: ca_(ca)
                # print (list_origin,list_name,languagecode,'wp',count,rel_value, measurement_date)

            # country level
            if country != 'all' and country != '': 
                list_origin = country+'_('+languagecode_2+')'
            else: 
                list_origin = languagecode_2

            coincident_qitems_all_qitems = len(country_language_qitems.intersection(lang_art))
            if len(country_language_qitems) == 0: rel_value = 0
            else: rel_value = 100*coincident_qitems_all_qitems/len(country_language_qitems)
            intersections.append((coincident_qitems_all_qitems,rel_value, measurement_date,list_origin,'all_top_ccc_articles',languagecode,'wp'))

        # INSERT INTERSECTIONS FOR THE LANGUAGE
        # if len(intersections) > 500000 or wikilanguagecodes.index(languagecode_1) == len(wikilanguagecodes)-1:
        query = 'INSERT OR IGNORE INTO wcdo_intersections (abs_value, rel_value, measurement_date, set1, set1descriptor, set2, set2descriptor) VALUES (?,?,?,?,?,?,?)'
        cursor4.executemany(query,intersections);

        query = 'UPDATE wcdo_intersections SET abs_value = ?, rel_value = ?, measurement_date = ? WHERE set1 = ? AND set1descriptor = ? AND set2 = ? AND set2descriptor = ?;'
        cursor4.executemany(query,intersections);
        conn4.commit() 
        count_intersections += len(intersections)

        # print (str(len(intersections))+' intersections inserted')
        # with open('top_diversity_selection.txt', 'a') as f: f.write(str(len(intersections))+' intersections calculated.\n')

        print (str(count_intersections)+' intersections inserted for this language: '+languagecode_1)
        print (str(datetime.timedelta(seconds=time.time() - langTime)))

    conn4.commit()
