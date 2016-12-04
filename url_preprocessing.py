# delete file calls fro history
import sys

all_urls = []
with open(sys.argv[1]) as hist:
    for line in hist:
        if ('file://' in line) or (line == 'url'):
            continue
        all_urls.append(line)

with open('clean_history.txt', 'w') as hist_out:
    for url in all_urls:
        hist_out.write(url)
        