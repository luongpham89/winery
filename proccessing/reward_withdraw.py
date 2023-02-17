import os
import pandas as pd
from bson.objectid import ObjectId
from functions import (
    read_collection_from_index, 
    read_index,
    write_index
)
def run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS, logger):
    logger.info(f"Starting to process {__name__} data")
    try:
        _COL = 'reward_withdraw'
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
            cols = ['amount', 'netAmount']
            _df[cols] = _df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
            
            _df['amount_number'] = _df['amount'].apply(lambda x: x / 1e18)
            _df['netAmount_number'] = _df['netAmount'].apply(lambda x: x / 1e18)
            
            # process ObjectId data type
            _df['_id'] = _df['_id'].apply(lambda x: ObjectId(x))
            
            # Push the df to the database
            for row in _df.to_dict(orient='records'):
                try:
                    _col_processed.replace_one({'_id': row.get('_id')}, row, upsert=True)
                except:
                    pass
            write_index(_COL, _df['updatedAt'].max())
    except:
        pass