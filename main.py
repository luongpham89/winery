import pandas as pd
from pymongo import MongoClient
# from pymongoarrow.api import write
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from sshtunnel import SSHTunnelForwarder
from proccessing import (
    asset_transaction,
    asset_volume_day,
    asset_tracking_hour,
    asset_tracking_day,
    bids,
    children_summary,
    customer,
    gallery_launchpad,
    gallery_launchpad_transaction,
    market_rankings,
    market_auction,
    market_offer,
    market_transaction,
    nft,
    nft_gallery,
    parent_summary,
    ref_ranks,
    reward_history,
    reward_withdraw,
    router
)
# import logging
# logging.basicConfig(filename = "main.log",
#                     filemode = "w",
#                     level = logging.DEBUG)
# logger = logging.getLogger()

# load environment variables
from dotenv import load_dotenv
load_dotenv()

# Schedule the task to run every hour
scheduler = BlockingScheduler()

COLLECTION_PROCESSED_SUFFIEXS = os.getenv("COLLECTION_PROCESSED_SUFFIEXS", 'processed')
JOB_INTERVAL = int(os.getenv("JOB_INTERVAL", 0))
SSH_TUNNEL = int(os.getenv("SSH_TUNNEL", 0))
MONGO_USER = os.getenv("MONGO_USER", "")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
MONGO_DB = os.getenv("MONGO_DB", "")
MONGO_HOST = os.getenv("MONGO_HOST", "")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
OUTPUT_MONGO_USER = os.getenv("OUTPUT_MONGO_USER", "")
OUTPUT_MONGO_PASSWORD = os.getenv("OUTPUT_MONGO_PASSWORD", "")
OUTPUT_MONGO_DB = os.getenv("OUTPUT_MONGO_DB", "")
OUTPUT_MONGO_HOST = os.getenv("OUTPUT_MONGO_HOST", "")
OUTPUT_MONGO_PORT = int(os.getenv("OUTPUT_MONGO_PORT", 27017))
SSH_USER = os.getenv("SSH_USER", "")
SSH_PASSWORD = os.getenv("SSH_PASSWORD", "")
SSH_HOST = os.getenv("SSH_HOST", "")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
MONGODB_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}/"
MONGODB_OUTPUT_URI = f"mongodb://{OUTPUT_MONGO_USER}:{OUTPUT_MONGO_PASSWORD}@{OUTPUT_MONGO_HOST}/"

def processing():
    if SSH_TUNNEL:
        server = SSHTunnelForwarder(
            ssh_address_or_host=(SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(MONGO_HOST, MONGO_PORT)
        )

        server.start()
        # set up MongoDB client
        client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@127.0.0.1:{server.local_bind_port}/")
        output_client = MongoClient(f"mongodb://{OUTPUT_MONGO_USER}:{OUTPUT_MONGO_PASSWORD}@127.0.0.1:{server.local_bind_port}/")
    else:
        # set up MongoDB client
        client = MongoClient(MONGODB_URI)
        output_client = MongoClient(MONGODB_OUTPUT_URI)

    db = client[MONGO_DB]
    output_db = output_client[MONGODB_OUTPUT_URI]

    asset_transaction.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    return
    asset_volume_day.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    asset_tracking_hour.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    asset_tracking_day.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    bids.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    children_summary.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    customer.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    gallery_launchpad.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    gallery_launchpad_transaction.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    market_rankings.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    market_auction.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    market_offer.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    market_transaction.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    nft.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    nft_gallery.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    parent_summary.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    ref_ranks.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    reward_history.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    reward_withdraw.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)
    router.run(db, output_db, COLLECTION_PROCESSED_SUFFIEXS)

    # MongoDB client
    client.close()
    output_client.close()
    if SSH_TUNNEL:
        server.stop()
processing()
# Schedule the task to run
# scheduler.add_job(processing, "interval", minutes=JOB_INTERVAL)

# Start the scheduler
# scheduler.start()