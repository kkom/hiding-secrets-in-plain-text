import gzip
import json
import urllib.request

path_template = "http://storage.googleapis.com/books/ngrams/books/" + \
                "googlebooks-eng-us-all-{gram}-20120701-{prefix}.gz"

with open('prefixes', 'r') as f:
  prefixes = json.load(f)
  
gram = '5gram'
prefix = prefixes[gram][10]
path = path_template.format(**locals())

print("Reading from: {path}".format(**locals()))

(i,I) = (1,10000)
with urllib.request.urlopen(path) as f:
    with gzip.GzipFile(fileobj=f) as g:
        for line in g:
            if i % J == 0:
                l = line.decode("utf-8").split()
                print("{i}: {l}".format(**locals()))
            i += 1
