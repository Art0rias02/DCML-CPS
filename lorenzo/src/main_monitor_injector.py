import csv
import os.path
import time
import psutil
from tqdm import tqdm
import json
import os.path
import random
from typing import List

from prof.LoadInjector import LoadInjector

def read_injectors(json_object, inj_duration: int = 2, verbose: bool = True, n_inj: int = -1) -> List[LoadInjector]:
    """
    Method to read a JSON object and extract injectors that are specified there
    :param inj_duration: number of subsequent observations for which the injection takes place
    :param json_object: the json object or file containing a json object
    :param verbose: True is debug information has to be shown
    :param n_inj: -1 means the same injectors as in the json file, otherwise it is the number of injectors needed and are randomly inserted from the ones specified in the json
    :return: a list of available injectors
    """
    try:
        json_object = json.loads(json_object)
    except ValueError:
        if os.path.exists(json_object):
            with open(json_object) as f:
                json_object = json.load(f)
        else:
            print(f"Could not parse input {json_object}")
            json_object = None
    if json_object is None:
        raise json.JSONDecodeError("Unable to parse JSON")
    
    n_inj_parsed=len(json_object)
    if n_inj != -1 and n_inj < n_inj_parsed:
        raise ValueError("Param n_inj can't be lower than the number of injectors specified in the JSON file (n_inj can only be >= or exactly -1)")

    json_injectors = []
    for job in json_object:
        job["duration_ms"] = inj_duration
        new_inj = LoadInjector.fromJSON(job)
        if new_inj is not None and new_inj.is_valid():
            # Means it was a valid JSON specification of an Injector
            json_injectors.append(new_inj)
            if verbose:
                print(f'New injector loaded from JSON: {new_inj.get_name()}')
    
    inj_difference = n_inj-n_inj_parsed if n_inj != -1 else 0
    inj_to_add = []
    while inj_difference > 0:
        job = random.choice(json_object)
        new_inj = LoadInjector.fromJSON(job)
        if new_inj is not None and new_inj.is_valid():
            # Means it was a valid JSON specification of an Injector
            inj_to_add.append(new_inj)
            inj_difference -= 1
            if verbose:
                print(f'New injector loaded from JSON: {new_inj.get_name()}')

    return json_injectors+inj_to_add

def monitor_system() -> dict:
    """
    Method to monitor system
    :return: dictionary with informations about the system
    """
    ret_dict = {}

    ret_dict.update(psutil.cpu_times_percent(interval=0.1, percpu=False)._asdict())
    ret_dict.update(psutil.disk_usage('/')._asdict())
    ret_dict.update(psutil.cpu_stats()._asdict())
    ret_dict.update(psutil.swap_memory()._asdict())
    ret_dict.update(psutil.virtual_memory()._asdict())
    ret_dict.update(psutil.disk_io_counters()._asdict())
    ret_dict['time_s'] = time.time()

    return ret_dict

def main(max_n_obs: int, out_filename: str, obs_interval_sec: float, obs_per_inj: int, obs_between_inj: int, injectors: List[LoadInjector]) -> None:
    """
    Method to perform monitoring during various load tests
    :param out_filename: filename (as CSV) to log data read from monitoring the system
    :param obs_interval_sec: lenght in seconds for each observation
    :param obs_per_inj: number of observations during each injection
    :param obs_between_inj: number of observations between each injection, marked as 'rest' in the log file
    :param injectors: list of LoadInjectors to use for testing the system
    :return: nothing is returned
    """

    # Checking of out_filename already exists: if yes, delete
    if os.path.exists(out_filename):
        os.remove(out_filename)

    # Variable setup
    inj_now = None
    obs_left_to_change = obs_between_inj

    # Monitoring Loop
    print(f'Monitoring for {max_n_obs} times')
    for obs_count in tqdm(range(max_n_obs), desc='Monitor Progress Bar'):
        if obs_left_to_change<=0 and inj_now is None:
            # Start next Injection
            obs_left_to_change = obs_per_inj
            inj_now = injectors.pop(0)
            inj_now.inject()
        elif obs_left_to_change<=0 and inj_now is not None:
            # Pause from Injections
            inj_now = None
            obs_left_to_change = obs_between_inj

        start_time = time.time()
        data_to_log = monitor_system()
        data_to_log['injector'] = 'rest' if inj_now is None else inj_now.get_name()

        # Writing as a new line of a CSV file
        with open(out_filename, "a", newline="") as csvfile:
            # Create a CSV writer using the field/column names
            writer = csv.DictWriter(csvfile, fieldnames=data_to_log.keys())
            if obs_count == 0:
                # Write the header row (column names)
                writer.writeheader()
            writer.writerow(data_to_log)

        # Sleeping to synchronize to the obs-interval
        exe_time = time.time() - start_time

        if exe_time < obs_interval_sec:
            time.sleep(obs_interval_sec - exe_time)
        else:
            print('Warning: execution of the monitor took too long (%.3f sec)' % (exe_time - obs_interval_sec))

        obs_left_to_change -= 1

if __name__ == '__main__':
    # General variables
    inj_json = 'prof/input_folder/injectors_only_cpu.json'
    time_step_sec = 0.2
    obs_per_inj = 50
    obs_between_inj = 30
    n_injectors = 5

    # Extracting definitions of injectors from input JSON
    injectors = read_injectors(inj_json, 
                            inj_duration=obs_per_inj*time_step_sec*1000,
                            n_inj=n_injectors)
    random.shuffle(injectors)

    main(max_n_obs=n_injectors*(obs_per_inj+obs_between_inj)+obs_between_inj, 
        out_filename='output_folder/monitored_data.csv', 
        obs_interval_sec=time_step_sec,
        obs_per_inj=obs_per_inj,
        obs_between_inj=obs_between_inj,
        injectors=injectors)