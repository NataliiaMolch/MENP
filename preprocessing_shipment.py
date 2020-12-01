import os
import csv
import pandas as pd
from datetime import datetime
import numpy as np
import time as t
""""""""""""""""""""""""""""""""""""""
""" Creating second features' table"""
""""""""""""""""""""""""""""""""""""""
# df = pd.read_csv(path + "combined_csv.csv")
# df.sort_values(by = 'user_id', inplace = True)
# for order in ['order_created_at','order_completed_at', 'shipment_starts_at', 'shipped_at']:
#     df[order] = pd.to_datetime(df[order])
# df['shipment_starts_at_month'] = pd.DatetimeIndex(df['shipment_starts_at']).month

# ad = pd.read_csv('misc/addresses.csv')
# dff = ad.merge(right = df, left_on = 'id', right_on = 'ship_address_id')
# dff.drop_duplicates(subset='shipment_id', keep= 'first', inplace=True)
# dff = dff.drop(['id', 'ship_address_id' , 'user_id', 'order_created_at', 'dw_kind', 'platform'], axis = 1)
# dff = dff.drop(dff[dff['order_completed_at_month'] >= 7].index, axis = 0)
# dff['delivery_time'] = dff['shipped_at'] - dff['shipment_starts_at']

"""
'monster' shipments + cleaning
"""
dff = pd.read_csv("monster.csv")
# dff = dff.drop(['order_completed_at_month', 'shipped_at', 's.order_state', 'shipment_starts_at', 'order_completed_at', 'order_id'], axis = 1)
# fix = datetime(2020, 1, 2, 0, 0 , 0, 0)
# tmp = dff['delivery_time'] + fix
# tmp = pd.DatetimeIndex(tmp).minute - fix.minute
# dff['delivery_time'] = tmp

# tmpp = dff['phone_id'].unique()
# np.random.shuffle(tmpp)
# dff = dff.loc[dff['phone_id'].isin(tmpp[0:900000])]

prdf = pd.read_csv('sample_submission.csv', sep = ';')
unic = prdf['Id'].unique()
dff = dff.loc[dff['phone_id'].isin(unic)]

columns = [
    'phone_id',
    'current_month',
    'freq_m', #last month = 1/n_shipm
    'freq_av', #for the whole time
    'n_shipm_m', #last month = len(sub_df[month])
    'n_shipm_av', #all time
    'shipment_time_m',
    'shipment_time_av',
    'average_rate_m',
    'average_rate_av',
    'promo_total_m',
    'promo_total_av',
    'per_cent_canceled_m',
    'per_cent_canceled_av',
    'change_of_city',
    'change_of_retailer',
    'weight',
    'os'
]
df_ = pd.DataFrame(columns=columns)
alpha = 0.2
for phone_id in dff.phone_id.unique():
    sub_df = dff.loc[dff['phone_id'] == phone_id]
    sub_df.sort_values(by = 'shipment_starts_at_month', inplace = True)
    
    start_month = sub_df.iloc[0]['shipment_starts_at_month']
    end_month = sub_df.iloc[-1]['shipment_starts_at_month']
    
    canceled = 0.
    freq_av_prev = 0.
    n_shipm_av_prev = 0.
    shipment_time_av_prev = 0.
    average_rate_av_prev = 0.
    promo_total_av_prev = 0.
    per_cent_canceled_av_prev = 0.
    weight_prev = 0.
    row = {}
    for m in range(start_month, end_month + 1):
        sub_sub_df = sub_df.loc[sub_df['shipment_starts_at_month'] == m]
        canceled += sub_sub_df['shipment_state'].isnull().sum()
        
        all_ = len(sub_sub_df)
        sub_sub_df.dropna(axis = 0)
        all__ = len(sub_sub_df)
        if sub_sub_df.empty:
            tmp2 = per_cent_canceled_av_prev *alpha if all_ == 0 else per_cent_canceled_av_prev*alpha + canceled/float(all__)
            row = {
                'phone_id' : phone_id,
                'current_month' : m + 1,
                'freq_m' : 0, 
                'freq_av' : freq_av_prev , 
                'n_shipm_m' : 0, 
                'n_shipm_av' : n_shipm_av_prev,
                'shipment_time_m' : 30*24,
                'shipment_time_av' : shipment_time_av_prev,
                'average_rate_m' : 0,
                'average_rate_av' : average_rate_av_prev,
                'promo_total_m' : 0,
                'promo_total_av' : promo_total_av_prev,
                'per_cent_canceled_m' : canceled if canceled != 0 else 0,
                'per_cent_canceled_av' : tmp2,
                'change_of_city': 0,
                'change_of_retailer': 0,
                'weight' : weight_prev,
                'os' : 5
            }
        else:
            tmp1 = 0 if all_ == 0 else canceled/float(all_)
            tmp2 = per_cent_canceled_av_prev*alpha if all__ == 0 else per_cent_canceled_av_prev*alpha + canceled/float(all__)
            row = {
                'phone_id' : phone_id,
                'current_month' : m + 1,
                'freq_m' : 1./all__, #last month = 1/n_shipm
                'freq_av' : freq_av_prev*alpha + 1./all__, #for the whole time
                'n_shipm_m' : all__, #last month = len(sub_df[month])
                'n_shipm_av' : n_shipm_av_prev + all__, #all time
                'shipment_time_m' : np.mean(sub_sub_df['delivery_time']),
                'shipment_time_av' : np.mean(sub_sub_df['delivery_time']) + alpha*shipment_time_av_prev,
                'average_rate_m' : np.mean(sub_sub_df['rate']),
                'average_rate_av' : alpha*average_rate_av_prev + np.mean(sub_sub_df['rate']),
                'promo_total_m' : np.mean(sub_sub_df['promo_total']),
                'promo_total_av' : np.sum(sub_sub_df['promo_total']) + alpha*promo_total_av_prev,
                'per_cent_canceled_m' : tmp1,
                'per_cent_canceled_av' : tmp2,
                'change_of_city': len(sub_sub_df['s.city_name'].unique()),
                'change_of_retailer': len(sub_sub_df.retailer.unique()),
                'weight' : np.mean(sub_sub_df['total_weight']) - weight_prev ,
                'os' : sub_sub_df['os'].iloc[0]
            }
            
            freq_av_prev = row['freq_av']
            n_shipm_av_prev = row['n_shipm_av']
            shipment_time_av_prev = row['shipment_time_av']
            average_rate_av_prev = row['average_rate_av']
            promo_total_av_prev = row['promo_total_av']
            weight_prev = row['weight']
            per_cent_canceled_av_prev = row['per_cent_canceled_av']
        
        df_ = df_.append(row, ignore_index = True)
        
print(df_.head(5))
df_.to_csv('test_data.csv')