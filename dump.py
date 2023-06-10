#from threading import Thread
import json
import re
from html2text import html2text as htt
import wikitextparser as wtp
import unicodedata

import bz2
import urllib.request
import os
import concurrent.futures


def dewiki(text):
    """
    For cleaning up ariticle text
    """

    #text = wtp.parse(text).plain_text()  # wiki to plaintext 
    text = wtp.parse(text) 
    #text = htt(text)  # remove any HTML
    #text = text.replace('\\n',' ')  # replace newlines
    #text = re.sub('\s+', ' ', text)  # replace excess whitespace
    return text


def analyze_chunk(text):
    """
    Do we have an article? between <page> and </page>
    """

    try:
        if '<redirect title="' in text:  # this is not the main article
            return None
        if '(disambiguation)' in text:  # this is not an article
            return None
        else:
            title = text.split('<title>')[1].split('</title>')[0]
            title = htt(title)
            if ':' in title:  # most articles with : in them are not articles we care about
                return None
        serial = text.split('<id>')[1].split('</id>')[0]
        content = text.split('</text')[0].split('<text')[1].split('>', maxsplit=1)[1]
        content = dewiki(content)
        return {'title': title.strip(), 'text': content.strip(), 'id': serial.strip()}
    except Exception as oops:
        print(oops)
        return None

def slugify(value, allow_unicode=False):
    """
    For cleaning up file names
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')

def save_article(article, savedir):
    doc = analyze_chunk(article)
    if doc:
        print('SAVING:', doc['title'])
        #filename = doc['id'] + '.json'
        filename = slugify(doc['title'])  + '.txt'
        with open(savedir + filename, 'w', encoding='utf-8') as outfile:
            #json.dump(doc, outfile, sort_keys=True, indent=1, ensure_ascii=False)
            outfile.write(doc['text'] + '\n')


def process_file_text(filename, savedir):
    """
    Main Loop
    """

    article = ''
    #with open(filename, 'r', encoding='utf-8') as infile:
    with bz2.open(filename, 'rt') as infile:
        for line in infile:
            if '<page>' in line:
                article = ''
            elif '</page>' in line:  # end of article
                #Thread(target=save_article, args=(article, savedir)).start()
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
                    executor.submit(save_article, article, savedir)
                
                #save_article(article, savedir)
                
                #DEBUG for 1 article
                exit()
                #End DEBUG
            else:
                article += line
                

####### START HERE ######

if __name__ == '__main__':

    
    url = "https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2"
    filename = "simplewiki-latest-pages-articles.xml.bz2"

    if os.path.exists(filename):
        print(f"The file '{filename}' exists.")
    else:
        print(f"The file '{filename}' does not exist. Downloading")
        urllib.request.urlretrieve(url, filename)
        print("Download complete!")                

    #wiki_xml_file = 'F:/simplewiki-20210401/simplewiki-20210401.xml'  # update this
    #wiki_xml_file = '/share/simplewiki-latest-pages-articles.xml'  # update this
    json_save_dir = '/share/out/'
 
    process_file_text(filename, json_save_dir)
