import csv
import sys
import numpy as np
import re
import thread
from scipy import sparse
from scipy.sparse import lil_matrix
from sklearn.preprocessing import normalize
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler


def get_enitity_mapping():
    id_to_entity = {}
    with open('EntityLabelMap.tsv') as entity_map:
        reader = csv.reader(entity_map, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        cnt = 0
        for row in reader:
            id_to_entity[cnt] = row
            cnt += 1
    return id_to_entity


def get_entities_vec():
    all_entities = []
    with open('DocumentEntityVectors.tsv') as ents:
        entities = ents.readlines()
        for row in entities:

            url, entities = re.split(r'\t+', row)
            ent_list = entities.split(',')
            target_list = []
            for ent in ent_list:
                if ent == '\n':
                    continue
                k = int(ent.split('/')[0].rstrip('\n'))
                v = int(ent.split('/')[1].rstrip('\n'))
                target_list.append([k, v])
            all_entities.append([url, target_list])
    return all_entities


def build_vectors(all_entities, lilx, lily):
    positions = []
    data = []
    print lilx
    print lily
    for entity in all_entities:
        this_position = []
        this_data = []
        for tuple in entity[1]:
            this_position.append(tuple[0])
            this_data.append(tuple[1])
        positions.append(this_position)
        data.append(this_data)

    sparse_entities = lil_matrix((lilx, lily))
    sparse_entities.rows = positions
    sparse_entities.data = data
    sparse_entities.tocsr()
    return sparse_entities


def main():

    print "Loading data.."
    id_to_entity = get_enitity_mapping()
    all_entities = get_entities_vec()

    print "Entities: " + str(len(id_to_entity))
    print "Websites: " + str(len(all_entities))

    sparse_entities = build_vectors(all_entities, len(all_entities), len(id_to_entity))

    print "Start clustering.."

    sparse_entities = StandardScaler(with_mean=False).fit_transform(sparse_entities)

    db = DBSCAN(eps=7, min_samples=5, metric="precomputed").fit(sparse_entities)
    core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
    core_samples_mask[db.core_sample_indices_] = True
    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    print('Estimated number of clusters: %d' % n_clusters_)#


    import matplotlib.pyplot as plt

    # Black removed and is used for noise instead.
    unique_labels = set(labels)
    colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))

    target_entities = []
    for k, col in zip(unique_labels, colors):
        if k == -1:
            # Black used for noise.
            col = 'k'

        class_member_mask = (labels == k)
        this_class = []
        idx = -1
        while True:
            try:
                idx = class_member_mask.tolist().index(True, idx+1)
                this_class.append(idx)
            except ValueError:
                break
        target_entities.append(this_class)
    print (target_entities)


if __name__ == "__main__":
    main()

