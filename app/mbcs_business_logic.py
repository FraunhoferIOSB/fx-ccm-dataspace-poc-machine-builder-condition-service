import requests

import pandas as pd
import numpy as np
from io import StringIO
import base64


# result template, see confluence for further detail
def get_result_template():
    return {
        'Component_1': 
        {
            'Status': 0, 
            'CS-Serivce': 0,
        },
        'Component_2': 
        {
            'Status': 0, 
            'CS-Serivce': 0,
        },
        'M': 0,
    }

# transform payloads into a format which we would use
def csvblob2dataframe(data):
    # transform encoded data into a string
    data_utf8 = data.encode(encoding='utf-8')
    str_data  = base64.b64decode(data_utf8).decode('utf-8')

    # create dataframe as if we read it from a local file
    strio_data = StringIO(str_data) # 'open file' <- input/output wrapper
    df = pd.read_csv(strio_data, usecols=['Time', 'Sensor1', 'DQ1'])
    return df

def evaluate_usecase(df_gen):
    # create placeholder for the results
    mb_analysis_result = get_result_template()

    # --- transformations (fft) ---
    signal_sensor1 = df_gen['Sensor1'].values[1:].astype(float)
    sf = np.fft.fft(signal_sensor1)
    sf = np.abs(sf/len(signal_sensor1))[0:len(signal_sensor1)//2+1]
    sf[1::] = 2*sf[1::]

    # corresponding freq axis:
    f_axg = 8000*np.arange(len(sf))/(2*(len(sf)-1))


    # --- check for error code 0 ---
    f_0_region = sf[np.logical_and(f_axg > 490, f_axg < 510)]
    f_0 = np.max(f_0_region)

    mb_analysis_result['Component_1']['CS-Serivce'] = bool(np.logical_and(bool(f_0 > 0.01), not bool(f_0 > 0.012)))
    mb_analysis_result['Component_1']['Status']     = bool(f_0 > 0.012)
    # --- check for error code 1 ---
    f_1_region = sf[np.logical_and(f_axg > 690, f_axg < 710)]
    f_1 = np.max(f_1_region)
    mb_analysis_result['Component_2']['CS-Serivce'] = bool(np.logical_and(bool(f_1 < 0.003), not bool(f_1 < 0.0015)))
    mb_analysis_result['Component_2']['Status']   = bool(f_1 < 0.0015)

    # --- check for error code ---
    motion_c = df_gen['DQ1'].values[1:].astype(float)

    # flank analysis:
    flanks_motion = np.abs(np.diff(motion_c)) > 0
    flanks_sensor = np.abs(signal_sensor1) > 2*3*np.std(signal_sensor1)


    # combine segments:
    # group regoions where oscilation leads to many peaks within the region of interest
    f_ = np.where(flanks_sensor)[0]     
    s_ = np.where(np.diff(f_) > 100)[0] # ignore noise

    # todo: make this a loop
    new_flank = np.zeros(len(flanks_sensor))
    new_flank[f_[0]:f_[s_[0]]]       = True
    new_flank[f_[s_[0]+1]:f_[s_[1]]] = True # this can be looped over
    new_flank[f_[s_[1]+1]:f_[-1]]    = True


    flank_consistency = np.logical_and(flanks_motion, new_flank[:-1])

    # check how many regions exist in the left part:
    n_consistent_flanks = np.where(np.diff(flank_consistency))[0]
    mb_analysis_result['M']   = bool(len(n_consistent_flanks) < 6)

    # return the result json:
    return mb_analysis_result

