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


languagecode = 'fa'

ccc_df_list_yes=['دهستان_دهسرد', 'دهستان_تشکن', 'امامزاده_قاسم_(آمل)', 'بادام\u200cزار_(ایذه)', 'گورستان_سماوات', 'تل_پیر_گل_سرخ', 'علویان_(مراغه)', 'سید_جعفر_کریمی_دیوکلایی', 'سرپر_(مسجدسلیمان)', 'ازره\u200cمکرمی', 'خانه_ابریشمی', 'چاه_بنیلی', 'بندعثمان_بازار', 'طبیعت_بی\u200cجان_و_باسمه_ژاپنی', 'تا_غروب', 'بهمن\u200cآباد_(اندیکا)', 'سینقان', 'علی_منصور', 'علی_رئیس', 'کلیسای_زور_زور', 'آموزه\u200cهای_صوفیان_از_دیروز_تا_امروز', 'زفر_بن_هذیل', 'خضرآباد_(نیشابور)', 'کیهان_کلهر', 'بردگپ_(دزفول)', 'صالح\u200cآباد_(حاجی\u200cآباد)', 'محمد_صفی\u200cزاده', 'سی_گرنورمحمد', 'دانشکده_فنی_شهید_رجایی_لاهیجان', 'کوه_سبز', 'قروه_درجزین', 'رهمالی', 'اوسیانو_دا_کروز', 'کانی\u200cخواره', 'کره_پا', 'خماجین', 'پشت_قلعه_آبدانان', 'اردشیر_بابکان', 'عباس\u200cآباد_(بردسیر)', 'آسیاب_شماره_۲_موگرم', 'زیرراه_(دشتستان)', 'کهن\u200cآب_(زنجان)', 'دستجرد_(آذرشهر)', 'دوپربرزیان', 'مرغش_(مشهد)', 'قارنه', 'سفارت_عراق_در_تهران', 'شیروکندی', 'دارخاتون', 'خانه_نوریان']
ccc_df_list_no=['سیارک_۱۳۳۰۰۸', 'اندیس_مکی۳', 'اچ\u200cام\u200cاس_اینمن_(کی۵۷۱)', 'لاتکوئر_۲۶', 'پست-راک', 'تیتو_جکسون', 'یولاندا_حدید', 'کلاه_میرحسن', 'فرودگاه_بین\u200cالمللی_برونئی', 'ژیانگیانگ', 'مرکز_تحقیقات_هسته\u200cای_نه\u200cگو_(اسرائیل)', 'مسیح_در_ابولی_توقف_کرد', 'البلاد_القدیم', 'فرودگاه_ریگولت', 'آتالانته_(ابهام\u200cزدایی)', 'خورشیدگرفتگی_۱۰_ژانویه_۱۲۷۵_پیش_از_میلاد', 'نصف\u200cالنهار_۸۱_درجه_غربی', 'برنان', 'سلبریتی_سنچری', 'میلان_میلوتینوویچ', 'گل_شرارت_(فیلم)', 'فرودگاه_آنادولو', 'اچ\u200cام\u200cای\u200cاس_تووومبا', 'قایق_پرنده', 'ماهیچه_کوتاه_بازکننده_انگشتان_پا', 'وولکا_کورچووسکا', 'قطعنامه_۱۹۲۷_شورای_امنیت', 'اچ\u200cام\u200cای\u200cاس_آردنت_(پی_۸۷)', 'کالاتایود', 'دانشگاه_جمهوریت_سیواس٬_ترکیه', 'گوسفند_سنگسری', 'نورمتانفرین', 'هایدزویل،_کالیفرنیا', 'سرصافی', 'بالگردگاه_اضطراری_دانشگاه_علم_و_بهداشت', 'مینگوره', 'روبی_جرینز', 'مک_ری_(آرکانزاس)', 'پائولیستا', 'آئرولینئاس_ماس', 'سیارک_۲۵۹۰۷', 'Select_(فراخوان_سیستمی)', 'القصرین', 'بلک_فارست،_کلورادو', 'آلگرداس_گریماس', 'ایرویوز_ایرلینک', 'فریاس', 'آندریا_دی_استفانو', 'ال\u200cتی\u200cوی_ای-۷_کرسر_۲', 'جواشان']

sample=50

binary_list = sample*['c']+sample*['w']
ccc_df_list = ccc_df_list_yes + ccc_df_list_no
samplearticles=dict(zip(ccc_df_list,binary_list))

print ('The articles are ready for the manual assessment.')
ass = random.sample(ccc_df_list, len(ccc_df_list))
ccc_df_list = ass

testsize = 100
CCC_falsepositive = 0
WP_falsenegative = 0

falsepositives=[]
falsenegatives=[]

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

    answer = input()
    if (answer != samplearticles[title]) & (samplearticles[title]=='c'): # c de correct
        CCC_falsepositive = CCC_falsepositive + 1
        falsepositives.append(title)
        print ('CCC false positive')
    if (answer != samplearticles[title]) & (samplearticles[title]=='w'):  # w de wrong
        WP_falsenegative = WP_falsenegative + 1
        falsenegatives.append(title)
        print ('WP false negative')

    counter=counter+1

result = 'WP '+languagecode+'wiki, has these false negatives: '+str(WP_falsenegative)+', a ratio of: '+str((float(WP_falsenegative)/100)*100)+'%.'+'\n'
result = result+'CCC from '+languagecode+'wiki, has these false positives: '+str(CCC_falsepositive)+', a ratio of: '+str((float(CCC_falsepositive)/100)*100)+'%.'+'\n'
print (result)