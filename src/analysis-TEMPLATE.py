

#%%
import yahoo as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# %%

amzn = yf.load_chart('AMZN')
amzn_sum = yf.get_summary('AMZN')

# when Did 
pdd = yf.load_chart('PDD')
pdd_sum = yf.get_summary('PDD')

#%% 

# get data
x, y = pdd['date'], pdd['mean']
xmin, xmax = x.min(), x.max()
ymin, ymax = y.min(), y.max()

# plot
fig, ax = plt.subplots()
ax.plot(x, y)

ax.set_xlim(xmin, xmax)

plt.show()

#%% get PDD's CAGR
pdd_age = (xmax - xmin).days / 365
pdd_range = y.iloc[-1] / y.iloc[0]
pdd_cagr = pdd_range**(1/pdd_age)
pdd_cagr

