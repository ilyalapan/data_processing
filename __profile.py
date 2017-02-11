import start_scraping
import start_scraping_2
import os
import shutil
import cProfile

if os.path.exists('data'):
    shutil.rmtree('data')

open('logs', 'w').close()
open('current_hash', 'w').close()
cProfile.run('start_scraping.main()')

if os.path.exists('data'):
    shutil.rmtree('data')

open('logs', 'w').close()
open('current_hash', 'w').close()
cProfile.run('start_scraping_2.main()')
