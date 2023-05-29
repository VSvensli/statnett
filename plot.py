import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def pull_local_data(data_dir:str):
    files = os.listdir(data_dir)
    for file in files:
        if not file.endswith('.json'):
            file.remove()
    
    if len(files) == 0:
        print(f'No data files in {data_dir}')
        return
        
    files = sorted(files)
    
    keys = pd.read_json(os.path.join(data_dir,files[0])).keys()
    aggregate_functions = pd.read_json(os.path.join(data_dir,files[0])).index.values
    number_of_files = len(files)
    
    data_agg_dict = {}
    for key in keys:
        data_agg_dict[key] = {}
        for aggregate_function in aggregate_functions:
            data_agg_dict[key][aggregate_function] = np.zeros(number_of_files)
    
    for i, file in enumerate(files):
        data_agg = pd.read_json(os.path.join(data_dir,file))
        for key in keys:
            for aggregate_function in aggregate_functions:
                data_agg_dict[key][aggregate_function][i] = data_agg[key][aggregate_function] 
        
    timesUTC = np.array(data_agg_dict['TimeUTC']['sum']/1000, dtype ='datetime64[s]')
    return data_agg_dict, timesUTC


def plot_mean(data:dict, keywords:list[str], ylable:str = None):
    for i, keyword in enumerate(keywords):
        std = data[keyword]['std']
        mean = data[keyword]['mean']
        plt.fill_between(timesUTC, mean+std, mean-std, alpha=0.5, color='C'+str(i), label='_nolegend_')
        plt.plot(timesUTC,mean, linestyle='-',c='C'+str(i),label = keyword)
    plt.legend()
    plt.xlabel('time UTC')
    plt.ylabel(ylable)
    plt.show()


if __name__ == '__main__':
    data_dict, timesUTC = pull_local_data(data_dir='aggregatedData/')

    keywords = ['Exchange_DK1_DE','Exchange_DK1_NL','Exchange_DK1_NO']
    plot_mean(data_dict, keywords, ylable='exchange [MW]')
    
    keywords = ['ProductionGe100MW','SolarPower','OffshoreWindPower']
    plot_mean(data_dict, keywords, ylable='energy production [MW]')
plt.show()