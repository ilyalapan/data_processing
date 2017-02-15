import ujson
import requests
import time
import os
import cProfile

url = 'https://blockchain.info/ru/rawblock/'
starting_hash = '00000000000000000255f3265753f2347320a95b92e0c0c08149d838105f3729'

maximum_requests = 28000  # do not go up to 30000, otherwise I get baned
time_limit = 8  # in hours

requests_per_log = 20


# this is the time format I was asked to keep my data in
def format_timestamp(unix_time):
    return time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime(unix_time))


# search through requests logs to see how many requests were performed in the last 8 hours
def get_relevant_logs():
    logs = []
    if os.path.isfile('logs'):
        f_logs = open('logs', 'r')
        all_logs = f_logs.readlines()
        current_time = time.time()
        limit_timestamp = current_time - time_limit * 60 * 60
        for log in all_logs:
            if float(str.split(log, ',')[0]) > limit_timestamp:
                logs.append(log)
    return logs


def calc_total_input(transaction):
    total_input = 0
    for bitcoin_input in transaction['out']:
        total_input += bitcoin_input['value']
    return total_input


def parse_transactions_in_block(block):
    block_string = ''
    for transaction in block['tx']:
        block_string += str(transaction['time']) + ',' + str(transaction['tx_index']) \
                        + ',' + str(calc_total_input(transaction)) + '\n'
    return block_string


def main():
    running = True
    while running:
        logs = get_relevant_logs()

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

        if not os.path.exists('data'):
            os.makedirs('data')

        print str(len(logs)*requests_per_log) + 'requests in last 8 hours'
        print 'starting scraping'

        for i in range(len(logs)*requests_per_log, maximum_requests):  # to stay in max requests range
            attempts = 0
            #try 10 times, after 10nth time stop
            while attempts < 10:
                try:
                    r = requests.get(url+current_hash)
                except:
                    print url+current_hash
                    print attempts
                    attempts += 1
                else:
                    break
            if attempts > 9:
                print "had an issue here, skipping"
                continue
            try:
                block = ujson.loads(r.text)
            except ValueError:
                print r.text
                print url+current_hash
                print attempts
                continue

            block_timestamp_str = format_timestamp(block['time'])
            block_index_str = str(block['block_index'])

            # only log each 20th request
            if i % 20 == 0:
                f_logs.write(str(time.time())+'\n')  # log the time when the request was made
            f = open('data/'+block_index_str+"__"+block_timestamp_str, "w+")
            block_string = parse_transactions_in_block(block)
            current_hash = block['prev_block']
            f.write(block_string)
            f.close()

            f_current_hash = open('current_hash', 'w')
            # record the hash the script stopped at
            f_current_hash.write(current_hash)
            f_current_hash.close()

        f_logs.close()
        if block['time'] < 1451600000:
            running = False

    print "finished scarping"

if __name__ == "__main__":
    main()
