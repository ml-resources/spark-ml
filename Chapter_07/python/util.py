import numpy as np
# function to get the categorical feature mapping for a given variable column
#path = "../data/hour_noheader.csv"
path = "../data/hour_noheader_5.csv"
def get_mapping(rdd, idx):
    x = rdd.map(lambda fields: fields[idx]).distinct()

    print "x.zipWithIndex(): " + str(x.zipWithIndex())
    return x.zipWithIndex().collectAsMap()

def extract_features(record,cat_len, mappings):
    cat_vec = np.zeros(cat_len)
    i = 0
    step = 0
    for field in record[2:9]:
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)

    num_vec = np.array([float(field) for field in record[10:14]])
    return np.concatenate((cat_vec, num_vec))

def extract_label(record):
    print record[-1]
    return float(record[-1])

def extract_features_dt(record):
    x = np.array(map(float, record[2:14]))
    return np.array(map(float, record[2:14]))

def squared_error(actual, pred):
    return (pred - actual)**2

def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2
