import shutil
from urllib.request import urlopen
import os
from spacy.en import English

nlp = English()

def download(src, target):
    r = urlopen(src)
    dir = os.path.dirname(target)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(target, 'wb') as f:
        shutil.copyfileobj(r, f)

def preprocess(text):
    result = []
    for line in text.split('\n'):        
        parsed = nlp(line)
        for sent in parsed.sents:
            tokens = [(token.text.lower(), token.pos, token.tag) for token in sent if token.text.strip() != '']
            result.append([token + (i+1,) for i, token in enumerate(tokens)])
    return result

def postprocess(data):
    return data