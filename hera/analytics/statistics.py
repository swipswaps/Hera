import matplotlib.pyplot as plt



def calcDist2d(data, x, y, bins, normalization="max_normalized"):
    """
    Calculates the mean of u,v,w,T and the fluctuations u',v',w',T'.

    Parameters
    ----------
    data : Pandas of th data.
    x: str or numpy array or series
    y: str or numpy array or series
    bins: number of bins for hist2d.
    normalization:  Normalize method to make 1 the maximum value.
                    density or y_normalized or max_normalized

    :return:
        Default value is None.


    Returns
    -------
    TurbulenceCalculator
        The object himself.
    """

    tmpfig = plt.figure()
    if data is None:
        xdata = x;
        ydata = y
    else:
        xdata = data[x];
        ydata = data[y]

    M, x_vals, y_vals, _ = plt.hist2d(xdata, ydata, bins=bins)
    plt.close(tmpfig)

    if normalization == "density":
        square_area = (x_vals[1] - x_vals[0]) * (y_vals[1] - y_vals[0])
        M = M / square_area
        y_normalized = False;
        max_normalized = False

    elif normalization == "y_normalized":
        M = M / M.sum(axis=1)[:, None]
        M[np.isnan(M)] = 0

    elif normalization == "max_normalized":
        M = M / M.max()

    else:
        raise ValueError("The normaliztion must be either density,y_normalized or max_normalized")

    x_mid = (x_vals[1:] + x_vals[:-1]) / 2
    y_mid = (y_vals[1:] + y_vals[:-1]) / 2

    return x_mid, y_mid, M.T