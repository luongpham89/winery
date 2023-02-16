import pandas as pd
def read_index(collection):
    try:
        f = open(f".{collection}.index", "r")
        index = int(f.read())
        f.close()
    except:
        index = 0
    return index

def write_index(collection, index):
    try:
        f = open(f".{collection}.index", "w")
        f.write(str(index))
        f.close()
    except:
        pass

def read_collection_from_index(collection, col, index):
    return pd.DataFrame(list(collection.find({col: {"$gt": int(index)}}).sort(col).limit(1000)))

