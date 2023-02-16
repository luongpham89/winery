import pandas as pd
from bson.objectid import ObjectId
from functions import (
    read_collection_from_index, 
    read_index,
    write_index
)
def run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS):
    try:
        _COL = 'market_offer'
        _OUTPUT_COL = f'{_COL}_{COLLECTION_PROCESSED_SUFFIEXS}'
        _col = db[_COL]
        _col_processed = output_db[_OUTPUT_COL]
        # Get data from MongoDB into a pandas DataFrame
        _df = read_collection_from_index(_col, 'updatedAt', read_index(_COL))
        if _df.shape[0]:
            # Process the ObjectId data type
            _df["_id"] = _df["_id"].apply(lambda x: str(x))
            _df['createdAt_Date'] = pd.to_datetime(_df['createdAt'], unit='s')
            _df['updatedAt_Date'] = pd.to_datetime(_df['updatedAt'], unit='s')
            # Number col
            cols = ['price']
            _df[cols] = _df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
            
            # process ObjectId data type
            _df['_id'] = _df['_id'].apply(lambda x: ObjectId(x))
            
            # Push the df to the database
            try:
                _col_processed.insert_many(_df.to_dict('records'), ordered=False)
                write_index(_COL, _df['updatedAt'].max())
            except:
                pass
    except:
        pass