import os

TABBY_ROOT = os.environ.get("TABBY_ROOT", os.path.expanduser("~/.tabby"))

TABBY_DATASET_FILEPATTERN = os.path.join(TABBY_ROOT, "dataset/*.jsonl")
