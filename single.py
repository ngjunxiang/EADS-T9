import pandas as pd

headers = ['userid', 'expected-time-to-completion',
           'memory', 'priority', 'arrival-time']

df = pd.read_csv('./input/single.csv', skiprows=1, names=headers)
