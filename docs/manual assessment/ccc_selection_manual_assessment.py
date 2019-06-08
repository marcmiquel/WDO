#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# EXECUTE:
#
import sys
import codecs
import os
import random
from urllib.parse import urljoin, urlencode, quote_plus
import webbrowser


# Rewritten
output_file_name = 'ccc_human_algo_articles.txt'
output_file_name_general1 = open(output_file_name, 'a')
#output_file_name_general1.write('lang'+'\t'+'page_title'+'\t'+'algoritme'+'\t'+'human'+'\n')

output_file_name2 = 'ccc_human_algo_results.txt'
output_file_name_general2 = open(output_file_name2, 'a')


languagecode = 'de'
ccc_df_list_yes=['Elbe-Saale-Bahn', 'Karolinen-Gymnasium_(Frankenthal)', 'Alex_Hofer_(Gleitschirmpilot)', 'Berge_(Band)', 'HSG_Raiffeisen_Bärnbach/Köflach', 'Achim_Ebert', 'Albrecht_I._(Mecklenburg)', 'Hans-Walter_Herrmann', 'Otto_Wanz', 'Pommernwoche', 'Lichtenfels_(Oberfranken)', 'Cesare_Gessler', 'Hamilkar_von_Rawicz-Kosinski', 'Bibliographie_des_Musikschrifttums', 'Christian_Rapp_(Autor)', 'THÜGIDA_&_Wir_lieben_Sachsen', 'Die_Widergänger', 'Johannes_Schenk_(Pianist)', 'Rising_Star_(Fernsehshow)', 'Diefenbach_(bei_Wittlich)', 'Hans_Hauschulz', 'Paulus_Klautendorffer', 'B1_(Künstlergruppe)', 'Hermann_Vogt', '3._Liga_Frauen_(Handball)_2012/13', 'Stetten_(Engen)', 'Liste_der_Festungen_in_Deutschland', 'Altlacher_Hochkopf', 'Georg_von_Eucken-Addenhausen', 'Fallschirmschützenabzeichen_des_Heeres', 'Dieter_Wellmann_(Fechter)', 'Stadttheater_Grein', 'Let’s_Dance_(Staffel_2)', 'Haus_Badstraße_64_(Heilbronn)', 'Ramin_Assadollahi', 'Bahnstrecke_Ludwigslust–Wismar', 'Philipp_Petzschner', 'Neues_von_der_Katze_mit_Hut_(Film)', 'Liste_der_Reichstagsabgeordneten_des_Deutschen_Kaiserreichs_(9._Wahlperiode)', 'Propstei_Bad_Gandersheim', 'Eggersdorf_(Bördeland)', 'Polizeiruf_110:_Bonnys_Blues', 'Goethe_erzählt_sein_Leben', 'Christian_August_Hermann_Marbach', 'Hürfeld_(Sugenheim)', 'Liste_der_Naturdenkmale_in_Endlichhofen', 'Heinrich_Friedrich_Gottlob_Flinsch', 'Schlacht_bei_Waschow', 'Liste_der_Baudenkmäler_in_Ungerhausen', 'Willmars', 'Werner_Stein_(Politiker)', 'Hugo_Hübner', 'Gerhard_Lindner_(General)', 'Pirach_(Gemeinde_St._Pantaleon)', 'Dietmar_Grupp', 'Sender_Freienfeld', 'Heinrich_Kieber', 'Ludwig_Rehn', 'Liste_der_französischen_Gesandten_bei_den_Hansestädten', 'Lambsheimer_Weiher', 'Harry_Goldstein', 'Johannes_Herz', 'Hermann_Brüning', 'Haus_Große_Brüdergasse_1', 'Christian_Friedrich_Tiede', 'Ruch_Chorzów', 'Haus_Ebertplatz_10_(Dresden)', 'Männliflue', 'Silvia_Polla', 'Thomas_Blenke', 'Sulzenauferner', 'Wildhorn', 'Amtsgericht_Plauen', 'Rogner_&_Bernhard', 'Werner_Stewe', 'Meininger', 'Jüdischer_Friedhof_(Burgholzhausen)', 'Johannes_Schmidt_(Ruderer)', 'Adalbert_Wex', 'MHK_Group', 'Richenza_von_Northeim', 'Euroblick', 'Heilangau', 'Regine_Kämper', 'Saint-Martin_VS', 'One_Step_(Lied)', 'Rauchenberg_(Aying)', 'Mindeltal-Radweg', 'Werner_Alpers', 'Synagoge_(Vallendar)', 'Liste_der_Kulturdenkmale_in_Gruna_(Nossen)', 'Johann_Caspar_Ripp', 'Hugo_Hildebrandt_(Ornithologe)', 'Kathy_Weber', 'Auch_Einer', 'Löwenturm', 'Wasserbehälter_(Wüsten)', 'Kurt_Hielscher', 'Richard_Nospers', 'Liste_der_Bischöfe_von_Eichstätt']
ccc_df_list_no=['Simon_Kemboi', 'Audi_Arena', 'Cliff_Battles', 'Albert_Grossman', 'Bistum_Bayombong', 'Pierre_Magnan', 'Kanton_Bourges-4', 'Phil_Terranova', 'Julie_Gavras', 'Lingbo', 'Saint-Prouant', 'Millie_Tomlinson', 'Nationale_Pädagogische_Universität_M._P._Drahomanow', 'Center_for_Organizational_Research_and_Education', 'Mahbouba_Gharbi', 'Abu_z-Zuhur', 'Tenge', 'Dee_Dee_Pierce', 'Moody_Currier', 'Anna_Charlotte_von_Lothringen', 'Lyndon_Rush', 'Bruce_Davison', 'Gymnote_(Q_1)', 'Naomi_Alderman', 'Lac_Saint-Pierre', 'Nocuchich', 'Pilar_Orero', 'Partido_Independiente', 'Gostilizy', 'Stephen_Tenenbaum', 'Gespanschaft_Bjelovar-Bilogora', 'Die_große_Leidenschaft', 'Estância_Velha', 'Jesufrei', 'Motoshi_Iwasaki', 'Der_Hund,_der_„Herr_Bozzi“_hieß', 'Archie_Hamilton,_Baron_Hamilton_of_Epsom', 'Nawal_El_Jack', 'Churandy_Martina', 'Okahandja_(Wahlkreis)', 'Fields_(Oregon)', 'Al-Rashid-Hotel', 'San_Isidro_(Surigao_del_Norte)', 'Brøndby_Strand_Sogn', 'Philogène_Auguste_Joseph_Duponchel', 'Chana_Orloff', 'Departamento_Gracias_a_Dios', 'Picote', 'Uralski_(Swerdlowsk)', 'Diether_Roeder_von_Diersburg_(1882–1918)', 'Festung_Arad', 'Peter_Forster_(Schauspieler)', 'KRISS_Vector', 'M119', 'Uppsala', 'Lajos_Pápai', 'Vescours', 'Charles_M._La_Follette', 'Yoff_(Dakar)', 'Cercal_(Cadaval)', 'Léon_Halévy', 'Charles_Magnette', 'Wasserkeilhebewerk_Fonserannes', 'Pat_Breen', 'Galerie_Koller', 'Kloster_Norraby', 'Liste_der_Einträge_im_National_Register_of_Historic_Places_im_Berkshire_County', 'Evgeny_Agrest', 'George_W._Croft', 'The_Obama_Deception', 'LOL_(^^,)', 'Turret_Cone', 'Cumari_(Goiás)', 'FK_MWD_Rossii_Moskau', 'Ebenezer_Mattoon', 'Tokajer_Gebirge', 'Action_Bronson', 'Conway_Mohamed', 'Plataforma_Oceánica_de_Canarias', 'Ricardo_García_Granados', 'Katharinengang', 'Jelena_Germanowna_Wodoresowa', 'Thomas_Wenski', 'David_Allan_Bromley', 'Alencar_Peak', 'Sainte-Marie-du-Mont_(Manche)', 'André_Schaeffner', 'Kurnool_(Distrikt)', 'Peter_Pan_(Schiff,_2001)', 'Zourafa', 'Kanton_Chevigny-Saint-Sauveur', 'Melbourne_Victory_(Frauenfußball)', 'Die_Rache_des_Ungeheuers', 'John_Carpenter_Carter', 'Palais_des_Sports_de_Ouaga_2000', 'Alexandre_Aubert', 'Karte_und_Gebiet', 'Bartholomeus_van_Bassen', 'Zu_zweit_ist_es_leichter', 'Reblino']


sample=100

binary_list = sample*['c']+sample*['w']
ccc_df_list = ccc_df_list_yes + ccc_df_list_no
samplearticles=dict(zip(ccc_df_list,binary_list))

print ('The articles are ready for the manual assessment.')
ass = random.sample(ccc_df_list, len(ccc_df_list))
ccc_df_list = ass

testsize = 200
CCC_falsepositive = 0
WP_falsenegative = 0

counter = 1
for title in ccc_df_list:

    page_title = title
    wiki_url = urljoin(
        'https://%s.wikipedia.org/wiki/' % (languagecode,),
        quote_plus(page_title.encode('utf-8')))
    translate_url = urljoin(
        'https://translate.google.com/translate',
        '?' + urlencode({
            'hl': 'en',
            'sl': languagecode,
            'u': wiki_url,
        }))
    print (str(counter)+'/'+str(testsize)+' '+translate_url+'\t')
#    webbrowser.open_new(wiki_url)
    webbrowser.open_new(translate_url)

    result = 'OK'
    answer = input()
    if (answer != samplearticles[title]) & (samplearticles[title]=='c'): # c de correct
        CCC_falsepositive = CCC_falsepositive + 1
        print ('CCC false positive')
        result = 'CCC false positive'
    if (answer != samplearticles[title]) & (samplearticles[title]=='w'):  # w de wrong
        WP_falsenegative = WP_falsenegative + 1
        print ('WP false negative')
        result = 'WP false negative'

    output_file_name_general1.write(str(counter)+'\t'+languagecode+'\t'+title+'\t'+samplearticles[title]+'\t'+answer+'\t'+result+'\n')

    counter=counter+1


result = 'WP '+languagecode+'wiki, has these false negatives: '+str(WP_falsenegative)+', a ratio of: '+str((float(WP_falsenegative)/100)*100)+'%.'+'\n'
result = result+'CCC from '+languagecode+'wiki, has these false positives: '+str(CCC_falsepositive)+', a ratio of: '+str((float(CCC_falsepositive)/100)*100)+'%.'+'\n'

print (result)
output_file_name_general2.write(result)