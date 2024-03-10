import requests
import pandas as pd
from metapub import PubMedFetcher
from metapub import CrossRefFetcher
import os
import sys
from multiprocessing import Process
from multiprocessing import Pool
import time
import threading
from metapub import FindIt
import json
import numpy as np
import requests
try: 
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup


keywords = ["observational"]

for ki in range(len(keywords)):

    keyword=keywords[ki]
    print(keyword)

    fetch = PubMedFetcher()

    month_lengths = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    for year in range(2022, 2023):
        for month in range(1, 13):

                month_i = month

                if month < 10:
                    month = "0"+str(month)
                
                print(month)
                pmids = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/01[MDAT] : '+str(year)+'/'+str(month)+'/8[MDAT] AND human NOT meta-analysis NOT systematic review NOT review NOT real-world NOT retrospective NOT administrative data NOT cohort NOT case-control NOT cross-sectional NOT electronic health records NOT population-based NOT registry NOT electronic health records NOT administrative data NOT claims database NOT epidemiological NOT Pragmatic trial NOT Non-interventional',retmax=10000000)

                pmids2 = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/9[MDAT] : '+str(year)+'/'+str(month)+'/16[MDAT] AND human NOT meta-analysis NOT systematic review NOT review NOT real-world NOT retrospective NOT administrative data NOT cohort NOT case-control NOT cross-sectional NOT electronic health records NOT population-based NOT registry NOT electronic health records NOT administrative data NOT claims database NOT epidemiological NOT Pragmatic trial NOT Non-interventional',retmax=10000000)

                pmids3 = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/17[MDAT] : '+str(year)+'/'+str(month)+'/24[MDAT] AND human NOT meta-analysis NOT systematic review NOT review NOT real-world NOT retrospective NOT administrative data NOT cohort NOT case-control NOT cross-sectional NOT electronic health records NOT population-based NOT registry NOT electronic health records NOT administrative data NOT claims database NOT epidemiological NOT Pragmatic trial NOT Non-interventional',retmax=10000000)

                pmids4 = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/25[MDAT] : '+str(year)+'/'+str(month)+'/'+str(month_lengths[month_i])+'[MDAT] AND human NOT meta-analysis NOT systematic review NOT review NOT real-world NOT retrospective NOT administrative data NOT cohort NOT case-control NOT cross-sectional NOT electronic health records NOT population-based NOT registry NOT electronic health records NOT administrative data NOT claims database NOT epidemiological NOT Pragmatic trial NOT Non-interventional',retmax=10000000)

                pmids_tot = np.concatenate((pmids, pmids2, pmids3, pmids4))


                abstracts = []
                titles = []
                dois = []
                doi_links = []
                journals = []
                dates = []
                authors = []
                cits = []
                aos = []
                years = []
                affiliations = []

                for mpi in range(len(pmids_tot)):

                    base_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmids_tot[mpi]}/"
                    response = requests.get(base_url) 

                    aos.append(pmids_tot[mpi])

                    html = response.text
                    parsed_html = BeautifulSoup(html, features="lxml")

                    try:
                        doi_links.append(parsed_html.body.find('a', attrs={'class':'id-link'})['href'])
                    except:
                        doi_links.append("None")

                    try:
                        dois.append(parsed_html.body.find('a', attrs={'class':'id-link'}).text)
                    except:
                        dois.append("None")

                    try:
                        titles.append(parsed_html.body.find('h1', attrs={'class':'heading-title'}).text.replace("[", "").replace("]", ""))
                    except:
                        titles.append("None")

                    try:
                        journals.append(parsed_html.body.find('button', attrs={'class':'journal-actions-trigger trigger'}).text)
                    except:
                        journals.append("None")

                    try:
                        dates.append(parsed_html.body.find('span', attrs={'class':'cit'}).text[:8])
                    except:
                        dates.append("None")

                    try:
                        years.append(parsed_html.body.find('span', attrs={'class':'cit'}).text[:4])
                    except:
                        years.append("None")


                    affs_1 = []

                    for ain in range(1, 100, 1):
                        try:
                            affs_1.append(parsed_html.body.find('li', attrs={'data-affiliation-id':f'affiliation-{ain}'}).text.lstrip().strip()[1:])
                        except:
                            break

                    affiliations.append(affs_1)

                    try:
                        abstracts.append(parsed_html.body.find('div', attrs={'class':'abstract-content selected'}).text.replace("  ", " ").replace("  ", " ").replace("  ", " ").replace("  ", " "))
                    except:
                        abstracts.append("None")


                    authors_1 = []

                    try:
                        au_arr = "".join(filter(lambda x: not x.isdigit(), parsed_html.body.find('div', attrs={'class':'authors-list'}).text.replace("\n", "").replace("  ", ""))).split(",")
                        for i in range(len(au_arr)):
                            au_arr[i].replace("\xa0", "").lstrip().strip()
                            authors_1.append(au_arr[i].replace("\xa0", ""))
                    except:
                        authors_1.append("None")
                    
                    authors.append(authors_1)

                    cits_1 = []

                    try:
                        citations = parsed_html.body.find_all('a', attrs={'class':'docsum-title'})
                        for i in citations:
                            cits_1.append(i.text.lstrip().strip().replace("[", "").replace("]", ""))
                    except:
                        cits_1.append("None")

                    cits.append(cits_1)


                if 1 > 0:


                    links={}
                    for ti in range(len(aos)):
                        try:
                            links[ti] = '''"'''+"https://pubmed.ncbi.nlm.nih.gov/"+aos[ti].lstrip().strip()+'''"'''
                        except:
                            links[ti] = '''"'''+"None"+'''"'''
                    Link = pd.DataFrame(list(links.items()),columns = ['id','PMID Link'])

                    doi_ls={}
                    for ti in range(len(doi_links)):
                        try:
                            doi_ls[ti] = '''"'''+doi_links[ti].lstrip().strip()+'''"'''
                        except:
                            doi_ls[ti] = '''"'''+"None"+'''"'''
                    Doi_Link = pd.DataFrame(list(doi_ls.items()),columns = ['id','DOI Link'])

                    lpmids={}
                    for ti in range(len(aos)):
                        try:
                            lpmids[ti] = '''"'''+aos[ti].lstrip().strip()+'''"'''
                        except:
                            lpmids[ti] = '''"'''+"None"+'''"'''
                    Pmid = pd.DataFrame(list(lpmids.items()),columns = ['id','PMID'])

                    ds={}
                    for ti in range(len(dois)):
                        #links[ti] = "https://pubmed.ncbi.nlm.nih.gov/"+pmid+"/"
                        try:
                            ds[ti] = '''"'''+dois[ti].lstrip().strip()+'''"'''
                        except:
                            ds[ti] = '''"'''+"None"+'''"'''
                    Doi = pd.DataFrame(list(ds.items()),columns = ['id','DOI'])

                    ts = {}
                    for ti in range(len(titles)):
                        try:
                            ts[ti] = '''"'''+str(titles[ti].lstrip().strip())+'''"'''
                        except:
                            ts[ti] = '''"'''+"None"+'''"'''

                    Title = pd.DataFrame(list(ts.items()),columns = ['id','Title'])


                    abst = {}
                    for ti in range(len(abstracts)):
                        try:
                            abst[ti] = '''"'''+str(abstracts[ti].lstrip().strip().replace("\n", ""))+'''"'''
                        except:
                            abst[ti] = '''"'''+"None"+'''"'''

                    Abstract = pd.DataFrame(list(abst.items()),columns = ['id','Abstract'])


                    auths = {}
                    for ti in range(len(authors)):
                        try:
                            auths[ti] = '''"'''+str(authors[ti])+'''"'''
                        except:
                            auths[ti] = '''"'''+"None"+'''"'''
                    Author = pd.DataFrame(list(auths.items()),columns = ['id','Authors'])

                    dict_affs = {}
                    for ti in range(len(affiliations)):
                        try:
                            dict_affs[ti] = '''"'''+str(affiliations[ti])+'''"'''
                        except:
                            dict_affs[ti] = '''"'''+"None"+'''"'''
                    Affs_df = pd.DataFrame(list(dict_affs.items()),columns = ['id','Affiliations'])

                    years_d = {}
                    for ti in range(len(years)):
                        try:
                            years_d[ti] = '''"'''+str(years[ti])+'''"'''
                        except:
                            years_d[ti] = '''"'''+"None"+'''"'''
                    Year = pd.DataFrame(list(years_d.items()),columns = ['id','Year'])

                    date_times = {}
                    for ti in range(len(dates)):
                        try:
                            date_times[ti] = '''"'''+str(dates[ti].lstrip().strip())+'''"'''
                        except:
                            date_times[ti] = '''"'''+"None"+'''"'''
                    Dates = pd.DataFrame(list(date_times.items()),columns = ['id','Date'])

                    journals_d = {}
                    for ti in range(len(journals)):
                        try:
                            journals_d[ti] = '''"'''+str(journals[ti].lstrip().strip())+'''"'''
                        except:
                            journals_d[ti] = '''"'''+"None"+'''"'''
                    Journal = pd.DataFrame(list(journals_d.items()),columns = ['id','Journal'])


                    citations_d = {}
                    for ti in range(len(cits)):
                        try:
                            citations_d[ti] = '''"'''+str(cits[ti])+'''"'''
                        except:
                            citations_d[ti] = '''"'''+"None"+'''"'''
                    Citation = pd.DataFrame(list(citations_d.items()),columns = ['id','Citations'])



                    data_frames = [Link, Doi_Link, Pmid, Doi, Title, Abstract, Author, Affs_df, Year, Dates, Journal, Citation]
                    from functools import reduce
                    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['id'],
                                                                how='outer'), data_frames)



                    df_merged.to_csv(f"scraped_files_v3/{keyword}__{year}-{month_i}.csv")


