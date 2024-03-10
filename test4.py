from metapub import PubMedFetcher
fetch = PubMedFetcher()
article = fetch.article_by_pmid('31823482')
print(article.journal)


                    if(response.status_code == 200): 
                            f2 = open("response.html", "w")
                            f2.write(response.text)
                            f2.close()
                    else: 
                        print(f"Error: {response.status_code}") 