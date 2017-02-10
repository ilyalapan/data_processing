import json
import requests
import time
import os

url = 'https://blockchain.info/ru/rawblock/'
starting_hash = '00000000000000000255f3265753f2347320a95b92e0c0c08149d838105f3729'

maximum_requests = 20  # do not go up to 30000 just in case

time_limit = 8  # in hours

# search through requests logs to see how many requests were performed in the last 8 hours
logs = []
if os.path.isfile('logs'):
    f_logs = open('logs', 'r')
    all_logs = f_logs.readlines()
    current_time = time.time()
    limit_timestamp = current_time - time_limit * 60 * 60 * 1000
    for log in all_logs:
        if str.split(log, ',')[0] > limit_timestamp:
            logs.append(log)

f_logs = open('logs', 'w')
if logs:
    for log_item in logs:
        f_logs.write(str(log_item))  # write only relevant logs in to the file to keep it small

# get the hash that script stopped at last time, or start from the beginning
if os.path.isfile('current_hash'):
    f_current_hash = open('current_hash', 'r')
    current_hash = f_current_hash.read()
    if not current_hash:
        current_hash = starting_hash
    f_current_hash.close()
else:
    current_hash = starting_hash


for i in range(len(logs), maximum_requests):  # to stay in max requests range

    r = requests.get(url+current_hash)
    block = json.loads(r.text)
    block_timestamp_str = str(block['time'])
    block_index_str = str(block['block_index'])
    f_logs.write(str(time.time())+'\n')  # log the time when the request was made
    f = open('data/'+block_index_str+"__"+block_timestamp_str, "w+")
    for transaction in block['tx']:
        total_input = 0
        for bitcoin_input in transaction['out']:
            total_input += bitcoin_input['value']
        f.write(str(transaction['time']) + ',' + str(transaction['tx_index']) + ',' + str(total_input)
                + ','+block_index_str + ',' + block_timestamp_str+'\n')
    current_hash = block['prev_block']
    print(str(i)+'th block finished')
    f.close()

# record the hash the script stopped at
f_current_hash = open('current_hash', 'w')
f_current_hash.write(current_hash)
f_current_hash.close()

f_logs.close()
