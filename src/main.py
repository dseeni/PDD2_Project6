import os
import shutil
from src.push_pipeline import pipeline_coro

cwd = os.getcwd()
dirs = os.listdir(cwd)

if 'output_data' in dirs:
    shutil.rmtree('output_data')
pipeline_coro()
