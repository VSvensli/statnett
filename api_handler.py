import os
import sys
import requests
import time
import threading
from dataclasses import dataclass
from datetime import datetime
import logging
import pandas as pd


@dataclass
class APIConfig:
    url: str
    save_path: str
    missing_data_queue_save_path:str
    retry_delay: int
    pull_delay:int
    check_missing_delay:int


def pull_data(config:APIConfig, retries:int=10, data_row_limit:int=5, end_time:str = None) -> pd.DataFrame:
    if end_time is None:
        response = requests.get(f'{config.url}?limit={data_row_limit}')
    else:
        # One-liner to add one minute some 'end_time' since API starts one minute late
        end_time = (datetime.strptime(end_time,'%Y-%m-%dT%H:%M') + pd.Timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M")
        response = requests.get(f'{config.url}?end={end_time}&limit={data_row_limit}&timezone=UTC')

    if response.ok is True:
        records = response.json()['records']
        data = pd.DataFrame(records)
        data['Minutes1UTC'] = pd.to_datetime(data['Minutes1UTC'], format='%Y-%m-%dT%H:%M:%S')
        data = data.drop(columns=['Minutes1DK'])
        return data

    if retries == 0:
        logging.error(f'Failed to access API')
        raise ConnectionError('Failed to access API')

    logging.warning(f'Bad connection! Attempt{config.retry_counter}, retrying in {config.retry_delay} second(s)')
    time.sleep(config.retry_delay)
    pull_data(config=config, retries=retries - 1, data_row_limit=data_row_limit)


def aggregate_data(data:pd.DataFrame) -> pd.DataFrame:
    data_agg = data.agg({'CO2Emission':          ['sum', 'mean'],
                         'ProductionGe100MW':    ['std', 'mean'],
                         'ProductionLt100MW':    ['std', 'mean'],
                         'SolarPower':           ['std', 'mean'],
                         'OffshoreWindPower':    ['std', 'mean'],
                         'OnshoreWindPower':     ['std', 'mean'],
                         'Exchange_Sum':         ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK1_DE':      ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK1_NL':      ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK1_NO':      ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK1_SE':      ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK1_DK2':     ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK2_DE':      ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_DK2_SE':      ['sum', 'mean', 'std', 'min', 'max'],
                         'Exchange_Bornholm_SE': ['sum', 'mean', 'std', 'min', 'max']})
    data_agg['TimeUTC'] = data['Minutes1UTC'][0]
    data_agg['AggrigatePeriodMinutes'] = data.shape[0]
    return data_agg


def get_last_data_entry(config:APIConfig, lock:threading.Lock) -> pd.DataFrame:
    filenames = os.listdir(config.save_path)
    datetime_str=[]
    for filename in filenames:
        if filename.endswith('.json'):
            datetime_str.append(filename)

    if len(datetime_str) == 0:
        return None
    
    datetime_str = sorted(datetime_str)
    last_data_entry_filename = f'aggregatedData/{datetime_str[-1]}'
    with lock:
        with open(last_data_entry_filename,'r') as f:
            last_data_entry = f.read()
    
    return pd.read_json(last_data_entry)


def save_aggregated_data(config:APIConfig, data:pd.DataFrame, lock:threading.Lock) -> None:
    filename = f'{str(data["TimeUTC"][0]).replace(" ","T")}Z.json'
    file_path = os.path.join(config.save_path, filename)
    with lock:
        try:
            with open(file_path, 'w') as f:
                f.write(data.to_json())
            logging.info(
                f'Data saved to aggrigated data from {data["TimeUTC"][0]} UTC...')
        except:
            logging.error(f'Could not save {filename}')
            # Add additional error handeling


def run(config:APIConfig, lock:threading.Lock) -> None:
    logging.info('Starting run..')
    last_data_entry = get_last_data_entry(config, lock)
    if last_data_entry is None:
        last_data_entry_time = datetime.now()
        logging.info(f'Could not find last entry. Starting to collect from {last_data_entry_time}')
    else:
        last_data_entry_time = datetime.utcfromtimestamp(last_data_entry['TimeUTC'][0]/1000)
        logging.info(f'Last found entry is from {last_data_entry_time}')

    while True:
        if event.is_set():
            break
        try:
            data = pull_data(config, retries=10, data_row_limit=5)
        except:
            logging.error('Something went wrong connecting to API') 
            time.sleep(10)
            continue
        
        time_delta = abs(last_data_entry_time - data['Minutes1UTC'][0])

        if time_delta > pd.Timedelta(minutes=1) and last_data_entry is not None:
            logging.warning(f'Missing entry dates logged to {config.missing_data_queue_save_path}')
            with lock:
                with open(config.missing_data_queue_save_path,'a') as f:
                    for t in range(int(time_delta.seconds/60)):
                        missing_time = last_data_entry_time + pd.Timedelta(minutes=(t+1))
                        f.write(f'{missing_time.strftime("%Y-%m-%dT%H:%M")}\n')

        if time_delta != pd.Timedelta(minutes=0):
            last_data_entry_time = data['Minutes1UTC'][0]
            data_agg = aggregate_data(data)
            save_aggregated_data(config, data_agg, lock)

        time.sleep(config.pull_delay)

def run_handle_missing(config:APIConfig, lock:threading.Lock) -> None:
    while True:
        if event.is_set():
            break

        if not os.path.isfile(config.missing_data_queue_save_path):
            time.sleep(config.check_missing_delay)
            continue

        with lock:
            with open(config.missing_data_queue_save_path, 'r') as f:
                missing_times = f.read() 
        missing_times = missing_times.splitlines()
        if len(missing_times) == 0:
            logging.info(f'No missing entries. Sleeping for {config.check_missing_delay} seconds...')
            time.sleep(config.check_missing_delay)
            continue

        failed_attempts = []
        for missing_time in missing_times:
            try:
                data = pull_data(config, end_time = missing_time)
            except:
                logging.error('Something went wrong connecting to API while adding missing files')
                time.sleep(config.check_missing_delay)
                failed_attempts.append(f'{missing_time}\n') 
                continue

            data_agg = aggregate_data(data)
            save_aggregated_data(config, data_agg, lock)
            time.sleep(1)

        with lock:
            with open(config.missing_data_queue_save_path, 'w') as f:
                if len(failed_attempts) > 0:
                    f.write(failed_attempts)
                    logging.warning('Not all missing entries added')
                else:
                    logging.info('All missing entries added successfully!')
                    pass


if __name__ == '__main__':
    config = APIConfig(
        url='https://api.energidataservice.dk/dataset/PowerSystemRightNow',
        save_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'aggregatedData'),
        missing_data_queue_save_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'missing_data_queue.csv'),
        pull_delay=10,
        retry_delay=1,
        check_missing_delay=10)

    if not os.path.exists(config.save_path):
        logging.info(f'Making dir {config.save_path}')
        os.makedirs(config.save_path)

    logging.basicConfig(filename='logs.log',
                        filemode='a',
                        format="%(asctime)s | %(levelname)s |  %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%SZ",
                        level=logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s |  %(message)s |  thread= %(threadName)s', datefmt='%Y-%m-%dT%H:%M:%SZ"')
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)

    lock = threading.Lock()
    event = threading.Event()

    t1 = threading.Thread(target=run, args=(config, lock,), daemon=True, name='PullLive')
    t2 = threading.Thread(target=run_handle_missing, args=(config, lock,), daemon=True,name='PullMissing')

    t1.start()
    t2.start()
    logging.info('Starting collection thread')
    logging.info('Starting missing entry handel thread')

    is_running = True
    while is_running:
        for line in sys.stdin:
            if line.rstrip() == 'exit':
                is_running = False
                event.set()
                break

    t1.join()
    t2.join()
    logging.info('Program terminated...')
