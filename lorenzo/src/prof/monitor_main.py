import csv
import os.path
import time

import psutil
from tqdm import tqdm


def main_monitor(max_n_obs, out_filename, obs_interval_sec):
    """
    Main function for monitoring
    :param obs_interval_sec: seconds in between two observations
    :param out_filename: name of the output CSV file
    :param max_n_obs: maximum number of observations
    :return: no return
    """

    # Checking of out_filename already exists: if yes, delete
    if os.path.exists(out_filename):
        os.remove(out_filename)

    # Monitoring Loop
    print('Monitoring for %d times' % max_n_obs)
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
        end_time = time.time()
        exe_time_s = time.time() - start_time
        sleep_s = obs_interval_sec - exe_time_s

        if exe_time_s < obs_interval_sec:
            time.sleep(obs_interval_sec - exe_time_s)
        else:
            print('Warning: execution of the monitor took too long (%.3f sec)' % (exe_time_s - obs_interval_sec))
        obs_count += 1


if __name__ == "__main__":
    """
    Entry point for the Monitor
    """
    main_monitor(5, 'output_folder/test_csv.csv', 1.0)