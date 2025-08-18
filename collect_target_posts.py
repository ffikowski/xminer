import tweepy
import time
import pandas as pd
import numpy as np
import requests
import yaml
from pathlib import Path
import json

with open("config/config.yml", "r") as file:
    config = yaml.safe_load(file)

creds = config["twitter_credentials"]

bearer_token = creds["bearer_token"]

input_file = "data/politicians4.xlsx"
output_file = Path("data/target_posts.jsonl")
done_file = Path("data/completed_ids.txt")

client = tweepy.Client(bearer_token=bearer_token)

