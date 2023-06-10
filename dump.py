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
import argparse


def dewiki(text):
    """
    For cleaning up ariticle text
    """

    text = wtp.parse(text).plain_text()  # wiki to plaintext 
    #text = wtp.parse(text) 
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
        os.makedirs(savedir, exist_ok=True)
        os.makedirs(savedir + filename[0], exist_ok=True)
        with open(savedir + filename[0] + '/' + filename, 'w', encoding='utf-8') as outfile:
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
                
                #with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
                with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
                    executor.submit(save_article, article, savedir)
                
                #save_article(article, savedir)
                
                #DEBUG for 1 article
                #exit()
                #End DEBUG
            else:
                article += line
                

####### START HERE ######

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str, required=False, help="location of wikimedia download (default is latest simple wikipedia)", default="https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2")
    parser.add_argument('--out', type=str, required=False, help="output directory (default is ./out/)", default="./out/")
    parser.add_argument('-F', action='store_true', required=False, help="force download")
    args = parser.parse_args()
    
    url = args.url
    filename = os.path.basename(url).split('/')[-1]
    save_dir = args.out

    if os.path.exists(filename) and not args.F:
        print(f"The file '{filename}' exists.")
    else:
        print(f"The file '{filename}' is downloading")
        urllib.request.urlretrieve(url, filename)
        print("Download complete!")                
    
    process_file_text(filename, save_dir)
