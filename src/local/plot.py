import matplotlib.pyplot as plt
import numpy as np

plt.style.use('ggplot')
dcolors = plt.rcParams['axes.prop_cycle'].by_key()['color']
dcolors

# get data
def series(dataseries,
            xlim=None,
            ylim=None,
            **plot_kwargs):

    '''
    Plot time series
    '''

    x, y = dataseries.index, dataseries
    
    # get xmin and ymin
    xmin, xmax = x.min(), x.max()

    # plot
    fig, ax = plt.subplots()
    ax.plot(x, y, **plot_kwargs)

    # reset xlim to data range
    if not xlim:
        xlim = xmin, xmax
        
    ax.set_xlim(*xlim)

    if ylim:
        ax.set_ylim(*ylim)
        

    return fig, ax
