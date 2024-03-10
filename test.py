from metapub import PubMedFetcher
fetch = PubMedFetcher()
from time import sleep

month_lengths = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

for year in range(2010, 2023):
    for month in range(1, 12):

        pmids_tot = []

        month_i = month

        if month < 10:
            month = "0"+str(month)
        
        print(month)
        pmids = fetch.pmids_for_query('epidemiological '+str(year)+'/'+str(month)+'/01[MDAT] : '+str(year)+'/'+str(month)+'/8[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

        pmids_tot += pmids

        pmids2 = fetch.pmids_for_query('epidemiological '+str(year)+'/'+str(month)+'/9[MDAT] : '+str(year)+'/'+str(month)+'/16[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

        pmids_tot += pmids2

        pmids3 = fetch.pmids_for_query('epidemiological '+str(year)+'/'+str(month)+'/17[MDAT] : '+str(year)+'/'+str(month)+'/24[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

        pmids_tot += pmids3

        pmids4 = fetch.pmids_for_query('epidemiological '+str(year)+'/'+str(month)+'/25[MDAT] : '+str(year)+'/'+str(month)+'/'+str(month_lengths[month_i])+'[MDAT] AND human NOT meta-analysis NOT review',retmax=10000000)

        pmids_tot += pmids4

        
