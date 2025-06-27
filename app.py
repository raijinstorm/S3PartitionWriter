from typing import Any
from uuid import uuid4
# from app.util.aws import AWS_REGION_DEDAULT
from collections import defaultdict
import os
import gzip
import json
import logging
import argparse
from filelock import FileLock

AWS_REGION_DEFAULT = "us-east-1"
logging.basicConfig(level=logging.INFO)

class S3PartitionWriter:
    def __init__(self, bucket: str, prefix: str, region: str = AWS_REGION_DEFAULT) -> None:
        self.folder_path = os.path.join(bucket, prefix)
        self.manifest_path = os.path.join(self.folder_path, "manifest.json")
        self.region = region
        os.makedirs(self.folder_path, exist_ok=True)
        try:
            with open(self.manifest_path, "x", encoding="utf-8") as f:
                f.write("{}")
            logging.info(f"manifest.json initialized")
        except FileExistsError:
            logging.warning("File manifest.json already exists")
    
    def write_events(self, events: list[dict[str, Any]]) -> None:
        manifest_updates = defaultdict(list)
            
        groups = defaultdict(list)
        for event in events:
            groups[event["event_date"]].append(event)
            
        for event_date, group_data in groups.items():
            date_folder_path = os.path.join(self.folder_path, f"event_date={event_date}")
            os.makedirs(date_folder_path, exist_ok=True)
            
            unique_id = uuid4()
            filename = f"event_{unique_id}.json.gz"
            fullpath = os.path.join(date_folder_path, filename )
            
            with gzip.open(fullpath, "wt", encoding="utf-8") as f:
                json.dump(group_data, f)
                
            manifest_updates[event_date] = manifest_updates.get(event_date, 0) + len(group_data)
        
            logging.info(f"Wrote {len(group_data)} events to {fullpath}")
        
        lock = FileLock(self.manifest_path)
        try:
            with lock:
                try:
                    with open(self.manifest_path, "r", encoding="utf-8") as f:
                        manifest = json.load(f)
                except json.JSONDecodeError:
                    logging.warning("manifest.json invalid JSON, it has been reseted")
                    manifest = {}
                    
                for event_date, cnt in manifest_updates.items():
                    manifest[event_date] = manifest.get(event_date, 0) + cnt
                    
                with open(self.manifest_path, "wt", encoding="utf-8") as f:
                    json.dump(manifest, f)
            logging.info("manifest.json updated")
        except json.JSONDecodeError:
            logging.warning("manifest.json was corrupted")
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=False, default="data")
    args = parser.parse_args()
    
    sample_events = [
        {"event_id": 1, "data": "log data A", "event_date": "2023-10-26"},
        {"event_id": 2, "data": "log data B", "event_date": "2023-10-27"},
        {"event_id": 3, "data": "log data C", "event_date": "2023-10-26"},
        {"event_id": 4, "data": "log data D", "event_date": "2023-10-28"},
        {"event_id": 5, "data": "log data E", "event_date": "2023-10-27"},
    ] 
    
    writer = S3PartitionWriter(bucket=args.bucket, prefix=args.prefix)

    writer.write_events(sample_events)



