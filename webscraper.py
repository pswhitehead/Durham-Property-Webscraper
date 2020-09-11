####Import the libraries we need####
import numpy as np
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import re
from selenium.webdriver.firefox.options import Options
import psycopg2
from psycopg2 import sql

####Write the functions that will do the scraping####

##To query what we need to scrape yet##
def findsql():    
    #Set up a connection to the PostgresSQL server we'll write data to
    conn = psycopg2.connect(host="localhost",database="durhamprop", user="postgres", password="postgres")
    cur = conn.cursor()
    
    #Write an sql query
    query = sql.SQL("SELECT * FROM {table} WHERE {key} = 2 LIMIT 1").format(
        table=sql.Identifier('scrapeprog'),
        key=sql.Identifier('status'))
    
    #execute query
    cur.execute(query)
    return cur.fetchall()

##To make sql calls to insert and change data##
def callsql(command, data = None):
    #Set up a connection to the PostgresSQL server we'll write data to
    conn = psycopg2.connect(host="localhost",database="durhamprop", user="postgres", password="postgres")
    cur = conn.cursor()
    
    #execute the query
    if data == None:
        cur.execute(command)
    else:
        cur.execute(command, data)
    conn.commit()  

##To scrape the data from each property##
####Import the libraries we need####
import numpy as np
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import re
from selenium.webdriver.firefox.options import Options
import psycopg2
from psycopg2 import sql

####Write the functions that will do the scraping####

##To query what we need to scrape yet##
def findsql():    
    #Set up a connection to the PostgresSQL server we'll write data to
    conn = psycopg2.connect(host="localhost",database="durhamprop", user="postgres", password="postgres")
    cur = conn.cursor()
    
    #Write an sql query
    query = sql.SQL("SELECT * FROM {table} WHERE {key} = 2 LIMIT 1").format(
        table=sql.Identifier('scrapeprog'),
        key=sql.Identifier('status'))
    
    #execute query
    cur.execute(query)
    return cur.fetchall()

##To make sql calls to insert and change data##
def callsql(command, data = None):
    #Set up a connection to the PostgresSQL server we'll write data to
    conn = psycopg2.connect(host="localhost",database="durhamprop", user="postgres", password="postgres")
    cur = conn.cursor()
    
    #execute the query
    if data == None:
        cur.execute(command)
    else:
        cur.execute(command, data)
    conn.commit()  

##To scrape the data from each property##
def getpropertydata(i):
    #try to scrape data
    try:
        #Setp up the website we'll scrape data from
        url = ["https://property.spatialest.com/nc/durham/#/property/"+str(i)]

        #Set up the Selenium web driver and then call the URL
        options = Options()
        options.headless = True #I don't want to open a physical window each tme
        driver = webdriver.Firefox(options=options, executable_path="C:\\TOOLBOXES\\geckodriver.exe")
        driver.get(url[0])
        time.sleep(2) #Because we are loading the page, I wait a few seconds before the next command

        if driver.current_url == url[0]: #check the status isn't 404  or redirected before we scrape the info
            #Parse the html
            soup_ID = BeautifulSoup(driver.page_source, 'html.parser')

            #Find the key info (ki)
            ki0 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_overview').findAll('span')[9]))[0] #landuse
            ki1 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_overview').findAll('span')[13]))[0] #subdiv
            ki2 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_overview').findAll('span')[21]))[0] #saledate
            ki3 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_overview').findAll('span')[23]))[0] #saleprice

            #Find the buildinfo (bi)
            bi0 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[-1]))[0] #value
            bi1 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[4]))[0] #year built
            bi2 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[13]))[0] #area
            bi3 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[15]))[0] #bathrooms
            bi4 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[19]))[0] #bedrooms
            bi5 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[8]))[0] #use
            bi6 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_buildings').findAll('span')[6]))[0] #builduse

            #Find the assessment details (ad)
            ad0 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'LandDetails').findAll('td')[0]))[0] #fair market value
            ad1 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'LandDetails').findAll('td')[1]))[0] #land assessed value
            ad2 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'LandDetails').findAll('td')[2]))[0] #acres
            ad3 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_overview').findAll('span')[-3]))[0] #improvement
            ad4 = re.findall(r'>(.*?)<', str(soup_ID.find(id = 'default_overview').findAll('span')[-1]))[0] #totalfmv

            #write sql code to insert
            sqltemp_ki = """INSERT INTO keyinfo(landuse,subdiv,saledate,saleprice,id)
                 VALUES(%s,%s,%s,%s,%s);"""

            sqltemp_bi = """INSERT INTO buildinfo(value,year,area,bathrooms,bedrooms,use,builduse,id)
                 VALUES(%s,%s,%s,%s,%s,%s,%s,%s);"""

            sqltemp_ad = """INSERT INTO assessment(fmv,lav,acres,improvement,totalfmv,id)
                 VALUES(%s,%s,%s,%s,%s,%s);"""

            #insert data in to sql
            callsql(sqltemp_ki, [ki0,ki1,ki2,ki3,i])
            callsql(sqltemp_bi, [bi0,bi1,bi2,bi3,bi4,bi5,bi6,i])
            callsql(sqltemp_ad, [ad0,ad1,ad2,ad3,ad4,i])

        driver.quit()
        #this was a success, so update the sql server
        callsql(sqlsuccess.format(sql.Literal(str(i))))
    
    except:
        driver.quit() 
        #this was not a success, so update the server
        callsql(sqlfail.format(sql.Literal(str(i)))) 
        print(i)


####Now we are ready to run the script####

##Define the calls for if we sucessfully/un scrape data##

#Update the ID status to 1 = success
sqlsuccess = sql.SQL("UPDATE scrapeprog SET status = 1 WHERE id = ({})")

#Update the ID status to 0 = fail
sqlfail = sql.SQL("UPDATE scrapeprog SET status = 0 WHERE id = ({})")

##Run this in a while loop until it cant run no more!##
while findsql()[0][1] == 2: #i.e. while status is pending = 2
    j = findsql()[0] #where the 0th index is the id, and the 1st is the status
    try:
        getpropertydata(j[0]) 
    except:
        print(j[0]) #just a nice visual as we go
