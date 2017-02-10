import os
from matplotlib import pyplot
data = []
for data_entry in os.listdir('data'):
    if data_entry.startswith('.'): continue
    with open('data/'+data_entry) as f:
        for transaction in f.readlines():
            value = int(transaction.split(',')[2])
            value = value - int(value/100)*100
            data.append(value)
pyplot.hist(data)
pyplot.show()
