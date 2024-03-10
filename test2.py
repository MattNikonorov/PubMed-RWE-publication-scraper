import requests 
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
        

pmid = "37013442"  # Replace with the PubMed ID you want to fetch api_key = "your_api_key"  # Optional, replace with your NCBI API key if you have 

xml_data = get_pubmed_data(pmid, "1338d5c8c2520afb1bdbc236e006eb56b909") 
if xml_data: 
    affiliations = parse_affiliations(xml_data) 
    print("Affiliations:") 
    for affil in affiliations: 
        print(affil) 
        