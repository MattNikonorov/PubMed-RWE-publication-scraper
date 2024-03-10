
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

CR = CrossRefFetcher()
start_time = time.time()

keywords = ['real-world', 'observational']

for ki in range(len(keywords)):

    keyword=keywords[ki]
    print(keyword)

    fetch = PubMedFetcher()

    month_lengths = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    for year in range(2010, 2023):
        for month in range(1, 12):

            month_i = month

            if month < 10:
                month = "0"+str(month)
            
            print(month)
            pmids = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/01[MDAT] : '+str(year)+'/'+str(month)+'/8[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

            pmids2 = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/9[MDAT] : '+str(year)+'/'+str(month)+'/16[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

            pmids3 = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/17[MDAT] : '+str(year)+'/'+str(month)+'/24[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

            pmids4 = fetch.pmids_for_query(keyword+' '+str(year)+'/'+str(month)+'/25[MDAT] : '+str(year)+'/'+str(month)+'/'+str(month_lengths[month_i])+'[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

            pmids_tot = np.concatenate((pmids, pmids2, pmids3, pmids4))
            
            affs = []
            d_times = []
            u_times = []
            aos = []
            dls = []

            for mpi in range(len(pmids_tot)):
                            print(mpi)
                            affs.append([])
                            try:
                                publ = fetch.article_by_pmid(pmids_tot[mpi])

                                try:
                                    src = FindIt(pmid=publ.pmid)
                                    dls.append(src.url)
                                except:
                                    e5 = 1

                                try:
                                    work = CR.article_by_doi(publ.doi)
                                    u_times.append((work.created)['timestamp'])
                                    d_times.append(((work.created)['date-time']).split("T")[0])
                                except:
                                    e3 = 1

                                try:
                                    def get_pubmed_data(pmid, api_key=None): 
                                        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi" 
                                        params = {"db": "pubmed", "id": pmid, "retmode": "xml", "rettype": "abstract","api_key": api_key } 
                                        response = requests.get(base_url, params=params) 
                                        if(response.status_code == 200): 
                                            return response.text 
                                        else: 
                                            print(f"Error: {response.status_code}") 
                                            return None 
                                            

                                    def parse_affiliations(xml_data):
                                        from xml.etree import ElementTree 
                                        affiliations = [] 
                                        root = ElementTree.fromstring(xml_data) 
                                        print(root.findall(".//AffiliationInfo"))
                                        for affil_elem in root.findall(".//AffiliationInfo/Affiliation"): 
                                            affiliations.append(affil_elem.text) 
                                        return affiliations 
                                            

                                    fpm = publ.pmid  # Replace with the PubMed ID you want to fetch api_key = "your_api_key"  # Optional, replace with your NCBI API key if you have 

                                    xml_data = get_pubmed_data(fpm, "1338d5c8c2520afb1bdbc236e006eb56b909") 
                                    print(xml_data)
                                    if xml_data: 
                                        affiliations = parse_affiliations(xml_data) 
                                        print("Affiliations:") 
                                        for affil in affiliations: 
                                            affs[-1].append(affil)
                                
                                except:
                                    e4 = 1


                            except Exception as e:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                print(exc_type, fname, exc_tb.tb_lineno)
                                #aos.append("nothing")
                                print("error")
                                publ = "None"
                                dls.append("None")
                                u_times.append("None")
                                d_times.append("None")

                            aos.append(publ)
                



            if 1 > 0:


                    links={}
                    for ti in range(len(aos)):
                        try:
                            links[ti] = '''"'''+"https://pubmed.ncbi.nlm.nih.gov/"+aos[ti].pmid+'''"'''
                        except:
                            links[ti] = '''"'''+"None"+'''"'''
                    Link = pd.DataFrame(list(links.items()),columns = ['pmid','PMID Link'])

                    doi_links={}
                    for ti in range(len(dls)):
                        try:
                            doi_links[ti] = '''"'''+dls[ti]+'''"'''
                        except:
                            doi_links[ti] = '''"'''+"None"+'''"'''
                    Doi_Link = pd.DataFrame(list(doi_links.items()),columns = ['pmid','DOI Link'])

                    lpmids={}
                    for ti in range(len(aos)):
                        try:
                            lpmids[ti] = '''"'''+aos[ti].pmid+'''"'''
                        except:
                            lpmids[ti] = '''"'''+"None"+'''"'''
                    Pmid = pd.DataFrame(list(lpmids.items()),columns = ['pmid','PMID'])

                    dois={}
                    for ti in range(len(aos)):
                        #links[ti] = "https://pubmed.ncbi.nlm.nih.gov/"+pmid+"/"
                        try:
                            dois[ti] = '''"'''+aos[ti].doi+'''"'''
                        except:
                            dois[ti] = '''"'''+"None"+'''"'''
                    Doi = pd.DataFrame(list(dois.items()),columns = ['pmid','DOI'])

                    titles = {}
                    for ti in range(len(aos)):
                        try:
                            titles[ti] = '''"'''+str(aos[ti].title)+'''"'''
                        except:
                            titles[ti] = '''"'''+"None"+'''"'''

                    Title = pd.DataFrame(list(titles.items()),columns = ['pmid','Title'])


                    abstracts = {}
                    for ti in range(len(aos)):
                        try:
                            abstracts[ti] = '''"'''+str(aos[ti].abstract)+'''"'''
                        except:
                            abstracts[ti] = '''"'''+"None"+'''"'''

                    Abstract = pd.DataFrame(list(abstracts.items()),columns = ['pmid','Abstract'])


                    authors = {}
                    for ti in range(len(aos)):
                        try:
                            authors[ti] = '''"'''+str(aos[ti].authors)+'''"'''
                        except:
                            authors[ti] = '''"'''+"None"+'''"'''
                    Author = pd.DataFrame(list(authors.items()),columns = ['pmid','Authors'])

                    dict_affs = {}
                    for ti in range(len(affs)):
                        try:
                            dict_affs[ti] = '''"'''+str(affs[ti])+'''"'''
                        except:
                            dict_affs[ti] = '''"'''+"None"+'''"'''
                    Affs_df = pd.DataFrame(list(dict_affs.items()),columns = ['pmid','Affiliations'])

                    years = {}
                    for ti in range(len(aos)):
                        try:
                            years[ti] = '''"'''+str(aos[ti].year)+'''"'''
                        except:
                            years[ti] = '''"'''+"None"+'''"'''
                    Year = pd.DataFrame(list(years.items()),columns = ['pmid','Year'])

                    date_times = {}
                    for ti in range(len(d_times)):
                        try:
                            date_times[ti] = '''"'''+str(d_times[ti])+'''"'''
                        except:
                            date_times[ti] = '''"'''+"None"+'''"'''
                    Dates = pd.DataFrame(list(date_times.items()),columns = ['pmid','Date Published Online'])

                    unix_times = {}
                    for ti in range(len(u_times)):
                        try:
                            unix_times[ti] = '''"'''+str(u_times[ti])+'''"'''
                        except:
                            unix_times[ti] = '''"'''+"None"+'''"'''
                    Dates_unix = pd.DataFrame(list(unix_times.items()),columns = ['pmid','Date Published Online (UNIX)'])


                    journals = {}
                    for ti in range(len(aos)):
                        try:
                            journals[ti] = '''"'''+str(aos[ti].journal)+'''"'''
                        except:
                            journals[ti] = '''"'''+"None"+'''"'''
                    Journal = pd.DataFrame(list(journals.items()),columns = ['pmid','Journal'])


                    citations = {}
                    for ti in range(len(aos)):
                        try:
                            citations[ti] = '''"'''+str(aos[ti].citation)+'''"'''
                        except:
                            citations[ti] = '''"'''+"None"+'''"'''
                    Citation = pd.DataFrame(list(citations.items()),columns = ['pmid','Citation'])



                    data_frames = [Link, Doi_Link, Pmid, Doi, Title, Abstract, Author, Affs_df, Year, Dates, Dates_unix, Journal, Citation]
                    from functools import reduce
                    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['pmid'],
                                                                how='outer'), data_frames)



                    df_merged.to_csv(f"csv_files/{keyword}__{year}-{month_i}.csv")

                    print("--- %s seconds ---" % (time.time() - start_time))

