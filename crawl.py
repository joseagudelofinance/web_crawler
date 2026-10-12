from urllib.parse import urlsplit, urljoin
from bs4 import BeautifulSoup, Tag

def normalize_url(url:str) ->str:
    removed_slash = ""
    if url[-1] == "/":
        removed_slash = url[:-1]
    else:
        removed_slash = url

    split_url = urlsplit(removed_slash)
    normalized_url = split_url.hostname + split_url.path
    return normalized_url

def get_heading_from_html(html:str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    if soup.find("h1") != None:
        h_tag = soup.find("h1")
    elif soup.find("h2") != None:
        h_tag =  soup.find("h2")
    else:
        h_tag = ""
    return h_tag.get_text(strip=True) if isinstance(h_tag, Tag) else ""

def get_first_paragraph_from_html(html: str) -> str:
    
    soup = BeautifulSoup(html, 'html.parser')
    main_tag = soup.find("main")

    if isinstance(main_tag, Tag):
        p_tag = main_tag.find("p")
    else:
        p_tag = soup.find("p")
    return p_tag.get_text(strip=True) if isinstance(p_tag, Tag) else ""

def get_urls_from_html(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    urls_tag = soup.find_all("a")
    urls_list = []

    for link in urls_tag:
        url_to_join = link.get("href")
        joined_url = urljoin(base_url, url_to_join)
        urls_list.append(joined_url)
    return urls_list
    
    
def get_images_from_html(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    imgs_tag = soup.find_all("img")
    imgs_list = []

    for link in imgs_tag:
        url_to_join = link.get("src")
        joined_url = urljoin(base_url, url_to_join)
        imgs_list.append(joined_url)
    return imgs_list

def extract_page_data(html: str, page_url: str):
    soup = BeautifulSoup(html, 'html.parser')
    conso_dict = {}

    dict_url = page_url
    dict_heading = get_heading_from_html(html)
    dict_first_paragrapgh = get_first_paragraph_from_html(html)
    dict_outgoing_links = get_urls_from_html(html, page_url)
    dict_image_urls = get_images_from_html(html, page_url)

    conso_dict["url"] = dict_url
    conso_dict["heading"] = dict_heading
    conso_dict["first_paragraph"] = dict_first_paragrapgh
    conso_dict["outgoing_links"] = dict_outgoing_links
    conso_dict["image_urls"] = dict_image_urls

    return conso_dict




    

