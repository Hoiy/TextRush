import luigi
from lib.util import download, preprocess
import os
import subprocess

class Download(luigi.Task):
    src = luigi.Parameter()
    dst = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dst)

    def run(self):
        download(self.src, self.dst)

class DownloadGloVe(luigi.Task):
    def requires(self):
        return Download('http://nlp.stanford.edu/data/glove.6B.zip', 'data/glove.6B.zip')
    
    def output(self):
        return luigi.LocalTarget('data/glove.6B.300d.txt')
    
    def run(self):
        unzip = subprocess.Popen(['unzip', '-d', 'data', self.input().path], stdout = subprocess.PIPE)
        unzip.wait()
        
class QuoteCorpus(luigi.Task):
    def requires(self):
        return Download('https://github.com/alvations/Quotables/raw/master/author-quote.txt', 'data/quote.txt')

    def output(self):
        return luigi.LocalTarget('data/quote-corpus.txt')

    def run(self):
        with self.input().open('r') as i, self.output().open('w') as o:
            for line in i:
                o.write(line.split('\t')[1])

class Quote2Corpus(luigi.Task):
    def requires(self):
        return Download('https://storage.googleapis.com/raw-corpus/kaggle_most_popular_quotes.json', 'data/quote2.json')

    def output(self):
        return luigi.LocalTarget('data/quote2-corpus.txt')

    def run(self):
        import json
        data = json.load(open(self.input().path))
        with self.output().open('w') as o:    
            for line in data:
                line = line['text'].strip()
                if len(line) <=2:
                    continue
                if line[0] == "\u201c" or line[0] == "\u201d":
                    line = line[1:]
                if line[-1] == "\u201c" or line[-1] == "\u201d":
                    line = line[:-2]
                print(line, file=o)

class EnWikiCorpus(luigi.Task):
    wiki2text_exec = luigi.Parameter('./wiki2text')

    def requires(self):
        return Download('https://dumps.wikimedia.org/enwiki/20170820/enwiki-20170820-pages-articles1.xml-p10p30302.bz2', 'data/enwiki.bz')

    def output(self):
        return luigi.LocalTarget('data/enwiki-corpus.txt')

    def run(self):
        o = open(self.output().path, 'w')
        bunzip = subprocess.Popen(['bunzip2', '-c', self.input().path], stdout = subprocess.PIPE)
        wiki2text = subprocess.Popen([self.wiki2text_exec], stdin = bunzip.stdout, stdout = subprocess.PIPE)
        grep = subprocess.Popen(['grep', '-v', '^='], stdin = wiki2text.stdout, stdout = o)
        grep.wait()

def preprocess_file(src, dst):
    result = []
    with open(src, 'r') as i:
        for line in i:
            result += preprocess(line)

    import pickle
    pickle.dump(result, open(dst, 'wb'))


class QuoteTokens(luigi.Task):
    def requires(self):
        return QuoteCorpus()

    def output(self):
        return luigi.LocalTarget('data/quote-tokens.pkl')

    def run(self):
        preprocess_file(self.input().path, self.output().path)

class Quote2Tokens(luigi.Task):
    def requires(self):
        return Quote2Corpus()

    def output(self):
        return luigi.LocalTarget('data/quote2-tokens.pkl')

    def run(self):
        preprocess_file(self.input().path, self.output().path)


class EnWikiTokens(luigi.Task):
    def requires(self):
        return EnWikiCorpus()

    def output(self):
        return luigi.LocalTarget('data/enwiki-tokens.pkl')

    def run(self):
        preprocess_file(self.input().path, self.output().path)

class PrepareTrainingData(luigi.Task):
    def requires(self):
        yield QuoteTokens()
        yield EnWikiTokens()
        yield Quote2Tokens()
        yield DownloadGloVe()
