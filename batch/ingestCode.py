import os
import subprocess
import json
from datetime import datetime

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)
    return {"last_index": -1, "added_dirs": []}

def save_state(state_file, state):
    with open(state_file, 'w') as f:
        json.dump(state, f)

def get_directories(local_base_dir):
    return [d for d in os.listdir(local_base_dir) if os.path.isdir(os.path.join(local_base_dir, d))]

def create_hdfs_directory(hdfs_path):
    cmd = ['hdfs', 'dfs', '-mkdir', '-p', hdfs_path]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Successfully created HDFS directory {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to create HDFS directory {hdfs_path}. Error: {e.stderr.decode()}")

def add_to_hdfs(local_base_dir, hdfs_base_dir, state_file):
    state = load_state(state_file)
    directories = get_directories(local_base_dir)

    if not directories:
        print("No directories to process.")
        return

    for _ in directories:
        next_index = (state["last_index"] + 1) % len(directories)
        dir_name = directories[next_index]
        
        # Skip if directory is already added
        if dir_name in state["added_dirs"]:
            state["last_index"] = next_index
            continue
        
        dir_path = os.path.join(local_base_dir, dir_name)

        # Get current date and time
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        
        hdfs_path = os.path.join(hdfs_base_dir, year, month, day, hour)
        create_hdfs_directory(hdfs_path)
        
        full_hdfs_path = os.path.join(hdfs_path, dir_name)
        
        cmd = ['hdfs', 'dfs', '-put', dir_path, full_hdfs_path]
        
        try:
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"Successfully added {dir_path} to HDFS at {full_hdfs_path}")
            state["added_dirs"].append(dir_name)
            state["last_index"] = next_index
            save_state(state_file, state)
            return  # Exit after successfully adding one directory
        except subprocess.CalledProcessError as e:
            print(f"Failed to add {dir_path} to HDFS. Error: {e.stderr.decode()}")
            state["last_index"] = next_index
            save_state(state_file, state)

# Example usage:
local_base_dir = '/home/itversity/data'
hdfs_base_dir = '/user/itversity/casestudy/rawdata'
state_file = '/home/itversity/itversity-material/caseStudy/state_file.json'
add_to_hdfs(local_base_dir, hdfs_base_dir, state_file)
