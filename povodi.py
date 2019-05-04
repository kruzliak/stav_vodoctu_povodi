from bs4 import BeautifulSoup
import requests
import psycopg2
from datetime import datetime
import schedule
import time
import sys
import re  
reload(sys)  
sys.setdefaultencoding('utf8')
def job():
    i=0
    a=[]
    # pripojeni do DB
    try:
        conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='postgres'")
    except:
        print "I am unable to connect to the database"
    cur = conn.cursor()

    # povodi Ohre, Odra,Labe,Vltava
    urls =['http://sap.poh.cz/portal/SaP/cz/pc/Prehled.aspx','https://www.pod.cz/portal/SaP/cz/pc/Prehled.aspx','http://www.pvl.cz/portal/SaP/cz/pc/Prehled.aspx','http://www.pla.cz/portal/sap/cz/PC/Prehled.aspx']
    for url in urls:

        r = requests.get(url, timeout=(3.05, 27))
        soup = BeautifulSoup(r.text, "html.parser")
        tr=soup.find('tr')
        if tr == None:
            r = requests.get(url, timeout=(3.05, 27))
            time.sleep(30)
            r = requests.get(url, timeout=(3.05, 27))    
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        spans=soup.find_all('span',id="PosledniAktualizaceDatumLbl")
        cur.execute("select date from povod_time where url='"+url+"'")
        date=cur.fetchall()
        if date!=[]:
            if url=='http://sap.poh.cz/portal/SaP/cz/pc/Prehled.aspx':
            
                date_time=datetime.strptime(str(date[0][0]), '%d.%m.%Y %H:%M:%S')
                spans_time=datetime.strptime(spans[0].text, '%d.%m.%Y %H:%M:%S')
            else:
                date_time=datetime.strptime(str(date[0][0]), '%d.%m.%Y %H:%M')
                spans_time=datetime.strptime(spans[0].text, '%d.%m.%Y %H:%M')
    #kdyz v DB nic neni
        if date==[]:
            print('nove')
            sql_t="""INSERT INTO povod_time VALUES ('"""+url+"""','"""+spans[0].text+"""')"""
            cur.execute(sql_t)
            table=soup.find('table')
            rows = table.find_all('tr')
            for row in rows:
                stack=[]
                cols = row.find_all('td')
                i=0
                href=''
                h=''
                q=''
                for col in cols:
                    stav_h=''
                    cas_h=''
                    stav_q=''
                    cas_q=''

                    if i==0:
                        hrefs=col.find_all('a', href=True)
                        href=url.replace('Prehled.aspx','')+hrefs[0]['href']
                    stack.append(col.text.strip())
                    if i == 7:
                        h=stack[3].split('\n')
                        q=stack[4].split('\n')
                        if len(h)==3:
                            del(h[1])
                        if h==[u'\u2014']:
                            stav_h='null'
                            cas_h='null'
                        elif h==[u'']:
                            stav_h='null'
                            cas_h='null'
                        else:
                            stav_h=h[0]
                            cas_h=h[1]
                        if q==[u'\u2014']:
                            stav_q='null'
                            cas_q='null'
                        elif q==[u'']:
                            stav_q='null'
                            cas_q='null'
                        else:
                            stav_q=q[0]
                            cas_q=q[1]

                        sql="""INSERT INTO stav_povod VALUES ( '"""+stack[0]+"""','"""+stack[1]+"""',"""+stav_h.replace(',','.')+""",'"""+cas_h.replace('( ','').replace(' )','')[0:5]+"""',"""+stav_q.replace(',','.')+""",'"""+cas_q.replace('( ','').replace(' )','')[0:5]+"""','"""+href+"""')"""



                        cur.execute(sql.replace("'null'",'null'))
                    
                    i=i+1
                    
    #kdyz v DB je starsi datum
        elif date_time<spans_time:
            print('ano')
            sql_t="UPDATE povod_time set date='"+spans[0].text+"' where url='"+url+"'"
            cur.execute(sql_t)
            table=soup.find('table')
            rows = table.find_all('tr')
            for row in rows:
                stack=[]
                cols = row.find_all('td')
                i=0
                href=''
                h=''
                q=''
                for col in cols:
                    stav_h=''
                    cas_h=''
                    stav_q=''
                    cas_q=''

                    if i==0:
                        hrefs=col.find_all('a', href=True)
                        href=url.replace('Prehled.aspx','')+hrefs[0]['href']
                    stack.append(col.text.strip())
                    if i == 7:
                        h=stack[3].split('\n')
                        q=stack[4].split('\n')
                        if len(h)==3:
                            del(h[1])
                        if h==[u'\u2014']:
                            stav_h='null'
                            cas_h='null'
                        elif h==[u'']:
                            stav_h='null'
                            cas_h='null'
                        else:
                            stav_h=h[0]
                            cas_h=h[1]
                        if q==[u'\u2014']:
                            stav_q='null'
                            cas_q='null'
                        elif q==[u'']:
                            stav_q='null'
                            cas_q='null'
                        else:
                            stav_q=q[0]
                            cas_q=q[1]

                        sql="""INSERT INTO stav_povod VALUES ( '"""+stack[0]+"""','"""+stack[1]+"""',"""+stav_h.replace(',','.')+""",'"""+cas_h.replace('( ','').replace(' )','')[0:5]+"""',"""+stav_q.replace(',','.')+""",'"""+cas_q.replace('( ','').replace(' )','')[0:5]+"""','"""+href+"""')"""
                        sql_del="delete from stav_povod where meno='"+stack[0]+"' and rieka='"+stack[1]+"'"
                        
                        cur.execute(sql_del)
                        cur.execute(sql.replace("'null'",'null'))
                    
                    i=i+1
                    
    #kdyz v DB je stejny datum
        else:
            print('ne')
    # povodi Morava
    url='http://www.pmo.cz/portal/sap/cz/menu.htm'
    r = requests.get(url, timeout=(3.05, 27))
    r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text, "html.parser")
    spans=soup.find_all('td',align='right',valign='top')
    cur.execute("select date from povod_time where url='"+url+"'")
    date=cur.fetchall()
    url_ps=['http://www.pmo.cz/portal/sap/cz/prehled_tab_1_chp.htm','http://www.pmo.cz/portal/sap/cz/prehled_tab_2_chp.htm','http://www.pmo.cz/portal/sap/cz/prehled_tab_3_chp.htm']
    for url_p in url_ps:
        r_p = requests.get(url_p, timeout=(3.05, 27))
        r_p.encoding = 'cp1250'
        soup_p = BeautifulSoup(r_p.text, "html.parser")
        if date!=[]:
                date_time=datetime.strptime(str(date[0][0]).replace('\xc2\xa0',' '), '%d.%m.%Y %H:%M')
                spans_time=datetime.strptime(str(spans[0].text[29:len(spans[0].text)]).replace('\xc2\xa0',' '), '%d.%m.%Y %H:%M')
        if date==[]:
            print('nove')
            sql_t="""INSERT INTO povod_time VALUES ('"""+url+"""','"""+spans[0].text[29:len(spans[0].text)]+"""')"""
            cur.execute(sql_t)
            table=soup_p.find('table')
            rows = table.find_all('tr')
            j=0
            for row in rows:
                stack=[]
                i=0
                cols = row.find_all('td',nowrap='')
                if j>4:
                    for col in cols:
                        stav_h=''
                        cas_h=''
                        stav_q=''
                        cas_q=''
                        if i==0:
                            hrefs=col.find_all('a', href=True)
                            href=hrefs[0]['href']
                            url_h="http://www.pmo.cz/portal/sap/cz/mereni_"+href.split("'")[1]+".htm"
                        stack.append(col.text.strip())
                        if i == 7:  
                            h=str(stack[3]).split('\xc2\xa0')
                            q=str(stack[4]).split('\xc2\xa0')
                            if len(h)==3:
                                del(h[1])
                            if h==['-']:
                                stav_h='null'
                                cas_h='null'
                            elif h==['']:
                                stav_h='null'
                                cas_h='null'
                            else:
                                stav_h=h[0]
                                cas_h=h[1]
                            if q==['-']:
                                stav_q='null'
                                cas_q='null'
                            elif q==['']:
                                stav_q='null'
                                cas_q='null'
                            else:
                                stav_q=q[0]
                                cas_q=q[1]
                            sql="""INSERT INTO stav_povod VALUES ( '"""+str(stack[0])+"""','"""+stack[1]+"""',"""+stav_h.replace(',','.')+""",'"""+cas_h.replace('(','').replace(')','')[0:5]+"""',"""+stav_q.replace(',','.')+""",'"""+cas_q.replace('(','').replace(')','')[0:5]+"""','"""+url_h+"""')"""

                            cur.execute(sql.replace("'null'",'null'))
                        i=i+1
                j=j+1
                
        elif date_time<spans_time:
            print('ano')
            sql_t="UPDATE povod_time set date='"+spans[0].text[29:len(spans[0].text)]+"' where url='"+url+"'"
            cur.execute(sql_t)
            table=soup_p.find('table')
            rows = table.find_all('tr')
            j=0
            for row in rows:
                stack=[]
                i=0
                cols = row.find_all('td',nowrap='')
                if j>4:
                    for col in cols:
                        stav_h=''
                        cas_h=''
                        stav_q=''
                        cas_q=''
                        if i==0:
                            hrefs=col.find_all('a', href=True)
                            href=hrefs[0]['href']
                            url_h="http://www.pmo.cz/portal/sap/cz/mereni_"+href.split("'")[1]+".htm"
                        stack.append(col.text.strip())
                        if i == 7:  
                            h=str(stack[3]).split('\xc2\xa0')
                            q=str(stack[4]).split('\xc2\xa0')
                            if len(h)==3:
                                del(h[1])
                            if h==['-']:
                                stav_h='null'
                                cas_h='null'
                            elif h==['']:
                                stav_h='null'
                                cas_h='null'
                            else:
                                stav_h=h[0]
                                cas_h=h[1]
                            if q==['-']:
                                stav_q='null'
                                cas_q='null'
                            elif q==['']:
                                stav_q='null'
                                cas_q='null'
                            else:
                                stav_q=q[0]
                                cas_q=q[1]
                            sql_del="delete from stav_povod where meno='"+stack[0]+"' and rieka='"+stack[1]+"'"
                            sql="""INSERT INTO stav_povod VALUES ( '"""+str(stack[0])+"""','"""+stack[1]+"""',"""+stav_h.replace(',','.')+""",'"""+cas_h.replace('(','').replace(')','')[0:5]+"""',"""+stav_q.replace(',','.')+""",'"""+cas_q.replace('(','').replace(')','')[0:5]+"""','"""+url_h+"""')"""

                            cur.execute(sql_del)
                            cur.execute(sql.replace("'null'",'null'))
                        i=i+1
                j=j+1
                
        else:
            print('ne')


    conn.commit()
    print('horovo')

schedule.every(1).minutes.do(job)
while 1:
    schedule.run_pending()
    time.sleep(1)
