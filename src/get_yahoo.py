# %% import dependencies

import os, sys
import argparse, json, pandas
from datetime import datetime

path = os.path.abspath('../src')
sys.path.insert(0, path)
from local import yahoo

# %% set up directories

data_dir = os.path.abspath('../data')

# %% parse arguments

parser = argparse.ArgumentParser(
    prog='get_yahoo',
    description='Get yahoo data from tickers',
)

parser.add_argument('-i', '--index', default='1', type=str)

args, unknown = parser.parse_known_args()

# %% get stock data

# Get tickers from Russell 1000
src = os.path.join(data_dir, 'iwb', f'iwb-{args.index}.csv')
df = pandas.read_csv(src, index_col=0)

tstart = datetime.now()
N = len(df)

for n, row in enumerate(df.iloc):
    
    symbol = row['ticker']
    symbol = symbol.replace('.', '-')
    print(f'[{n+1} of {N}]', symbol)

    # save chart
    chart = yahoo.get_chart(symbol)

    if chart['chart']['error']:
        continue

    dst = os.path.join(data_dir, 'chart', f'chart-{symbol.lower()}.json')
    with open(dst, 'w') as f:
        json.dump(chart['chart']['result'][0], f, indent=4)
        print('Saved:', dst)

    # save summary
    summary = yahoo.get_summary(symbol)

    if summary['quoteSummary']['error']:
        continue

    dst = os.path.join(data_dir, 'summary', f'summary-{symbol.lower()}.json')
    with open(dst, 'w') as f:
        json.dump(summary['quoteSummary']['result'][0], f, indent=4)
        print('Saved:', dst)


    # need to update for rate-limiting
    # save earnings
    earnings = yahoo.get_earnings(symbol)

    dst = os.path.join(data_dir, 'earnings', f'earnings-{symbol.lower()}.json')
    with open(dst, 'w') as f:
        json.dump(earnings, f, indent=4)
        print('Saved:', dst)

    
    tend = datetime.now()
    delta = tend - tstart
    avg = delta / (n+1)
    eta = avg * (N - n - 1)
    print(f'now: {tend} | eta: {eta} | delta: {delta} | avg: {avg}')

# %%

print('-- Complete --')

