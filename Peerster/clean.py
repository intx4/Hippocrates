import os 
import glob
from pathlib import Path

for f in Path('./gossip').iterdir():
    if ".go" not in f.name and "hw3" not in f.name and f.is_dir()==False:
        os.remove(f)