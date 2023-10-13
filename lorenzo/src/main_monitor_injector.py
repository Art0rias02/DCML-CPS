import csv
import os.path
import time
import psutil
from tqdm import tqdm
import json
import os.path
import random
import threading

from prof.LoadInjector import LoadInjector, current_ms

def read_injectors(json_object, inj_duration: int = 2, verbose: bool = True):
    """
    Method to read a JSON object and extract injectors that are specified there
    :param inj_duration: number of subsequent observations for which the injection takes place
    :param json_object: the json object or file containing a json object
    :param verbose: True is debug information has to be shown
    :return: a list of available injectors
    """
    try:
        json_object = json.loads(json_object)
    except ValueError:
        if os.path.exists(json_object):
            with open(json_object) as f:
                json_object = json.load(f)
        else:
            print("Could not parse input %s" % json_object)
            json_object = None
    json_injectors = []
    if json_object is not None:
        # Means it is a JSON object
        json_injectors = []
        for job in json_object:
            job["duration_ms"] = inj_duration
            new_inj = LoadInjector.fromJSON(job)
            if new_inj is not None and new_inj.is_valid():
                # Means it was a valid JSON specification of an Injector
                json_injectors.append(new_inj)
                if verbose:
                    print('New injector loaded from JSON: %s' % new_inj.get_name())

    return json_injectors

def main(max_n_obs, out_filename, obs_interval_sec, injectors):
    # Checking of out_filename already exists: if yes, delete
    if os.path.exists(out_filename):
        os.remove(out_filename)

    # Monitoring Loop
    print(f'Monitoring for {max_n_obs} times')
    obs_count = 0
    for obs_count in tqdm(range(max_n_obs), desc='Monitor Progress Bar'):
        start_time = time.time()
        # CPU Data
        cpu_t_p = psutil.cpu_times_percent(interval=0.1, percpu=False)._asdict()
        # Disk Data
        disk_usage = psutil.disk_usage('/')._asdict()
        cpu_t_p.update(disk_usage)
        cpu_t_p['time_s'] = time.time()

        # Writing on the command line and as a new line of a CSV file
        with open(out_filename, "a", newline="") as csvfile:
            # Create a CSV writer using the field/column names
            writer = csv.DictWriter(csvfile, fieldnames=cpu_t_p.keys())
            if obs_count == 0:
                # Write the header row (column names)
                writer.writeheader()
            writer.writerow(cpu_t_p)
        print(cpu_t_p)

        # Sleeping to synchronize to the obs-interval
        exe_time = time.time() - start_time

        if exe_time < obs_interval_sec:
            time.sleep(obs_interval_sec - exe_time)
        else:
            print('Warning: execution of the monitor took too long (%.3f sec)' % (exe_time - obs_interval_sec))
        
        obs_count += 1

if __name__ == '__main__':
    # General variables
    inj_filename = 'output_folder/inj_info.csv'
    inj_json = 'prof/input_folder/injectors_json.json'
    time_step_sec = 1

    # Extracting definitions of injectors from input JSON
    injectors = read_injectors(inj_json, inj_duration=4*time_step_sec*1000)

    main(200, 'monitored_data.csv', time_step_sec)