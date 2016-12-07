from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models
import gensim
import pyLDAvis.gensim as genPyLDAvis
import pyLDAvis
from langdetect import detect

# loading english pages that have more than 100 char
data = []
with open("historyText.tsv") as history_file:
    for line in history_file:
        record = line.split("\t")
        record[1] = record[1].decode('utf-8')
        if len(record[1]) > 100 and detect(record[1]) == 'en':
            data.append(record)


# creating a text only array
doc_set = []

for record in data:
    doc_set.append(record[1])

print("Reading completed")


tokenizer = RegexpTokenizer(r'\w+')

# create English stop words list
en_stop = get_stop_words('en')

# create p_stemmer of class PorterStemmer
p_stemmer = PorterStemmer()

# list for tokenized documents in loop
texts = []

# loop through document list
for i in doc_set:

    # clean and tokenize document string
    raw = i.lower()
    tokens = tokenizer.tokenize(raw)

    # remove stop words from tokens
    stopped_tokens = [i for i in tokens if not i in en_stop]

    # stem tokens
    stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]

    # add tokens to list
    texts.append(stemmed_tokens)


# turn our tokenized documents into a id <-> term dictionary
dictionary = corpora.Dictionary(texts)

print("Creating Dictionary")

# convert tokenized documents into a document-term matrix
corpus = [dictionary.doc2bow(text) for text in texts]



# generate LDA model
# print("Training Model")
# model = gensim.models.ldamodel.LdaModel(corpus, num_topics=12, id2word = dictionary, passes=20)
# model.save('lda.model')

# loading the model from local disc
print("Loading Model")
model =  models.LdaModel.load('lda.model')

# prepare model for visualization
vis = genPyLDAvis.prepare(model,corpus=corpus,dictionary=dictionary)

# Spin a webserver and visualize
pyLDAvis.show(vis)
