import nltk
from nltk.tree import Tree
import sys
import html2text
import requests, json
import collections

all_ent = {}
iterator = 0
all_objects = []
path = sys.argv[1]

with open(path + "historyTextBig.tsv") as history_file:
    for line in history_file:
        record = line.split("\t")
        record[1] = record[1].decode('utf-8')
        if len(record[1]) > 10:
            all_objects.append(record)


class Webpage():
    def __init__(self):
        self.url = ""
        self.content = ""
        self.entities = []
        self.cnt = []

    def flush_content(self):
        self.content = ""

    def build_vec(self):
        print '!'
        global all_ent
        short_ents = []
        for ent in self.entities:
            short_ents.append(all_ent[ent])
            print 'cntrl' + str(all_ent[ent])
        self.entities = short_ents
        yield self


def extract_pages(all_objects):
    return_objects = []
    for object in all_objects:
        key, content = object
        # p_ents = (''.join(key.split('.'))).split(':')
        """
        # Filtering webpages
        url_ent = ''
        for p_ent in p_ents:
            if ('www' in p_ent) or ('http' in p_ent) or ('/' in p_ent) or (len(p_ent) < 4):
                continue
            if ('de' in p_ent) or ('com' in p_ent) or ('.nl' in p_ent):
                break
            else:
                url_ent = p_ent
                break
        url_ent = url_ent.split('-')
        if url_ent != [''] :
            content = content + ''.join(url_ent)
            print content[-100:]
        """
        webpage = Webpage()
        webpage.url = key
        webpage.content = content
        if webpage.url and webpage.content:
            return_objects.append(webpage)
    return return_objects


def parse_entities(all_objects):
    return_objects = []
    #nltk.download('all')
    i = 0
    for object in all_objects:
        if i%100==0:
            print 'progress: ' + str(i*100.0/float(len(all_objects))) + '%'
        i += 1
        webpage = object
        tokens = nltk.word_tokenize(webpage.content)
        tagged = nltk.pos_tag(tokens)
        chunked = nltk.chunk.ne_chunk(tagged, binary=True)
        prev = None
        continuous_chunk = []
        current_chunk = []
        for chunk in chunked:
            if type(chunk) == Tree:
                current_chunk.append(" ".join([token for token, pos in chunk.leaves()]))
            elif current_chunk:
                named_entity = " ".join(current_chunk)
                if named_entity not in continuous_chunk:
                    continuous_chunk.append(named_entity)
                    current_chunk = []
            else:
                continue
        #print continuous_chunk
        webpage.entities = continuous_chunk
        webpage.flush_content()
        return_objects.append(webpage)
    return return_objects


def get_entity_ids(all_objects):
    return_objects = []
    i = 0
    for object in all_objects:
        if i%10==0:
            print 'progress: ' + str(i*100.0/float(len(all_objects))) + '%'
        i+=1
        webpage = object
        key, entity_strings = webpage.url, webpage.entities

        s_url = 'http://10.149.0.127:9200/freebase/label/_search'

        ids = set()
        surfaces = set()
        for e in entity_strings:
            try:
                query = json.dumps({
                    "query": {
                        "match": {
                            "label": e
                        }
                    }
                })
                response = requests.post(s_url, data=query).json()
                hits = response.get('hits', {}).get('hits', [])
                best_id = None

                if hits:
                    fid_counts = collections.Counter(hit['_source']['resource'] for hit in hits)
                    best_id, count = fid_counts.most_common(1)[0]
                    best_id = best_id.replace('fbase:m.', '/m/')
                ids.add(str(best_id))

                reverse_query = json.dumps({"query": {"match": {"resource": best_id}}})
                surface_form = requests.post(s_url, data=reverse_query).json()
                surfaces.add(str(surface_form))
            except requests.exceptions.ConnectionError as e:
                ids.add(str(e))

        if ids:
            # webpage.url = url
            webpage.entities = surfaces
            return_objects.append(webpage)
    return return_objects


def build_cnt_set(all_objects):
    return_objects = []
    for object in all_objects:
        website = object
        this_set = {}
        for e in website.entities:
            if e in this_set:
                cnt = this_set[e]
                this_set[e] = cnt +1
            else:
                this_set[e] = 1
        website.entities = [k for k in this_set.iterkeys()]
        website.cnt = [v for v in this_set.itervalues()]
        return_objects.append(website)
    return return_objects


def ent_dictionary(all_objects):
    global all_ent
    global iterator
    for object in all_objects:
        webpage = object
        for entity in webpage.entities:
            if entity in all_ent:
                continue
            all_ent[entity] = iterator
            iterator += 1


def produce(all_objects):
    return_objects = []
    global all_ent
    for object in all_objects:
        webpage = object

        short_ents = []
        for ent in webpage.entities:
            short_ents.append(all_ent[ent])
            # print 'cntrl' + str(all_ent[ent])
        webpage.entities = short_ents

        tuples = zip(webpage.entities, webpage.cnt)
        all_tps = ""
        for tps in tuples:
            all_tps = all_tps + ', '+ str(tps[0]) + '/' + str(tps[1])
            #print all_tps
        all_tps = all_tps[2:]
        return_objects.append(str(webpage.url) + "\t" + all_tps + '\n')
    return return_objects


def save_results(all_objects):
    with open('11_12_DocumentEntityVectors.tsv', 'wb') as ents_out:
        for object in all_objects:
            ents_out.write(object)
    global all_ent
    ent_list = [[k, v] for k, v in all_ent.iteritems()]
    sorted(ent_list, key=lambda ent: ent_list[1])
    with open('11_12_EntityLabelMap.tsv', 'wb') as dict_out:
        for ent in ent_list:
            dict_out.write(str(ent[0] + '\n'))


def main(all_objects):
    all_objects = extract_pages(all_objects)
    print "Parsing entities.."
    all_objects = parse_entities(all_objects)
    print 'Getting ids of: ' + str(len(all_objects)) + ' objects.'

    return

    all_objects = get_entity_ids(all_objects)
    all_objects = build_cnt_set(all_objects)
    print 'Building dictionary..'
    ent_dictionary(all_objects)
    print "Producing output.."
    all_objects = produce(all_objects)
    save_results(all_objects)
    print "Success"


if __name__ == "__main__":
    main(all_objects)



