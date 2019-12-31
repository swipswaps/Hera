import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns #; sns.set()


class PresentationLayer(object):


    def _calculate_hist(self,data,x,y,bins,normalization="density"):
        """
            Calculate the distrubtion.
            in future version should be moved to a specialied analysis of a mean data.

        :return:
            x_mid - the x axis
            y_mid - the y axis
            M     - normalized denstiy
        """
        tmpfig = plt.figure()
        newax = plt.subplot(111)
        if data is None:
            xdata = x; ydata = y
        else:
            xdata = data[x]; ydata = data[y]

        M, x_vals, y_vals, newax = plt.hist2d(xdata, ydata, bins=bins)
        plt.close(tmpfig)

        if normalization=="density":
            square_area = (x_vals[1]-x_vals[0]) * (y_vals[1]-y_vals[0])
            M = M / square_area
            y_normalized = False; max_normalized = False
        elif normalization=="y_normalized":
            M = M / M.sum(axis=1)[:, None]
            M[np.isnan(M)] = 0
        elif normalization=="max_normalized":
            M = M / M.max()
        else:
            raise ValueError("The normaliztion must be either density,y_normalized or max_normalized")

        x_mid = (x_vals[1:] + x_vals[:-1]) / 2
        y_mid = (y_vals[1:] + y_vals[:-1]) / 2
        return x_mid,y_mid,M.T


    def fig_hist2d_full(self, data = None, y = None, x = "time_within_day", ax = None, ax_props = None,cax_props=None, bins = 30,normalization="density", **kwargs):
        """

            plot_hist2d_windVec2(data=..,y="WD",month=3,hour=1)
                        query field is month,hour

                        querystr = ["month==3","hour==1"] ==> "month==3 and hour==1"

            plot_hist2d_windVec2(data=..,y="WD",month=3)
                        query field is month
                        querystr = ["month==3"]


            :param data:
            :param y:
            :param x:
            If data is None (default) then y,x are interpreted as vector data; else interpreted as names of columns in data.

            :param ax:
            :param ax_props : dict with axis functions and parameters. { funcname : {parameters}}
            :param bins: the number of bins in the histogram, default is 30
            :param levels: the number of height levels in the contour plot, default is 20

            :param max_normalized: whether or not the density will be normalized to make 1 the maximumum value
            :param mode either 'contour' for contour lines,
                               'fill'    for a full contour
                                'both'   for both.
            :param kwargs:
                    all the rest are used as query (use the and predicate on all the fields).
                    month=1
                    day = 2


            :return:
            """

        # kwargs_used_fields = ["axis", "fig", "bins", "levels"]
        # queryfield = [x for x in kwargs.key() if x not in kwargs_used_fields]
        # querystr = " and ".join(["{fieldname}=={fieldvalue}".format(fieldname=fieldname, fieldvalue=kwargs[fieldname]) for fieldname in queryfield])
        #
        # month_data = data.assign(year=data.index.year) \
        #     .assign(month=data.index.month) \
        #     .assign(day=data.index.day) \
        #     .assign(hour=data.index.hour) \
        #     .query(querystr)

        x_mid, y_mid, M = self._calculate_hist(data=data, x=x, y=y, bins=bins, normalization=normalization)

        if ax is None:
            fig, ax = plt.subplots()
        else:
            plt.sca(ax)

        #mycmap = kwargs.get("cmap",[[find how to get hte default cmap]])
        # here we blend.

        CS = plt.contourf(x_mid, y_mid, M,**kwargs)

        if not ax_props is None:
            for func in ax_props:
                getattr(ax, func)(**ax_props[func])

        if not cax_props is None:
            for func in cax_props:
                getattr(CS, func)(**cax_props[func])

        return ax,CS

    def fig_hist2d(self, data = None, y = None, x = "time_within_day", ax = None, ax_props = None, bins = 30,normalization="density", **kwargs):
        """

            plot_hist2d_windVec2(data=..,y="WD",month=3,hour=1)
                        query field is month,hour

                        querystr = ["month==3","hour==1"] ==> "month==3 and hour==1"

            plot_hist2d_windVec2(data=..,y="WD",month=3)
                        query field is month
                        querystr = ["month==3"]


            :param data:
            :param y:
            :param x:
            If data is None (default) then y,x are interpreted as vector data; else interpreted as names of columns in data.

            :param ax:
            :param ax_props : dict with axis functions and parameters. { funcname : {parameters}}
            :param bins: the number of bins in the histogram, default is 30
            :param levels: the number of height levels in the contour plot, default is 20

            :param max_normalized: whether or not the density will be normalized to make 1 the maximumum value
            :param mode either 'contour' for contour lines,
                               'fill'    for a full contour
                                'both'   for both.
            :param kwargs:
                    all the rest are used as query (use the and predicate on all the fields).
                    month=1
                    day = 2


            :return:
            """

        # kwargs_used_fields = ["axis", "fig", "bins", "levels"]
        # queryfield = [x for x in kwargs.key() if x not in kwargs_used_fields]
        # querystr = " and ".join(["{fieldname}=={fieldvalue}".format(fieldname=fieldname, fieldvalue=kwargs[fieldname]) for fieldname in queryfield])
        #
        # month_data = data.assign(year=data.index.year) \
        #     .assign(month=data.index.month) \
        #     .assign(day=data.index.day) \
        #     .assign(hour=data.index.hour) \
        #     .query(querystr)

        x_mid, y_mid, M = self._calculate_hist(data=data, x=x, y=y, bins=bins, normalization=normalization)

        if ax is None:
            fig, ax = plt.subplots()
        else:
            plt.sca(ax)

        CS = plt.contour(x_mid, y_mid, M,**kwargs)

        if not ax_props is None:
            for func in ax_props:
                getattr(ax, func)(**ax_props[func])

        return ax


    def fig_frequency_bar(self, data, category, grouped_by = "hour", axis = None, ax_props = None, cat_list = None, **kwargs):
        """

            plot_hist2d_windVec2(data=..,y="WD",month=3,hour=1)
                        query field is month,hour

                        querystr = ["month==3","hour==1"] ==> "month==3 and hour==1"

            plot_hist2d_windVec2(data=..,y="WD",month=3)
                        query field is month
                        querystr = ["month==3"]


            :param data:
            :param category:
            :param grouped_by:
            :param axis:
            :param ax_props: dict with axis functions and parameters. { funcname : {parameters}}
            :param cat_list:

            :param kwargs:
                    all the rest are used as query (use the and predicate on all the fields).
                    month=1
                    day = 2


            :return:
            """

        # kwargs_used_fields = ["axis", "fig", "bins", "levels"]
        # queryfield = [x for x in kwargs.key() if x not in kwargs_used_fields]
        # querystr = " and ".join(["{fieldname}=={fieldvalue}".format(fieldname=fieldname, fieldvalue=kwargs[fieldname]) for fieldname in queryfield])
        #
        # month_data = data.assign(year=data.index.year) \
        #     .assign(month=data.index.month) \
        #     .assign(day=data.index.day) \
        #     .assign(hour=data.index.hour) \
        #     .query(querystr)

        if axis is None:
            fig, axis = plt.subplots()
        else:
            plt.sca(axis)

        data_grouped = data.groupby([grouped_by, category])[category].count().unstack(category).fillna(0)
        if not cat_list is None:
            for cat in cat_list:
                if cat not in data_grouped.columns:
                    data_grouped[cat] = 0
            data_grouped = data_grouped[cat_list]

        data_grouped = data_grouped.div(data_grouped.sum(axis=1), axis=0)

        data_grouped.plot(kind='bar', stacked=True, ax = axis)

        if not ax_props is None:
            for func in ax_props:
                getattr(axis, func)(**ax_props[func])

    def fig_scatter_func(self, x, y, data = None, func_dict = None, axis = None, ax_props = None, SelfCorrelation = None, **kwargs):
        """

            plot_hist2d_windVec2(data=..,y="WD",month=3,hour=1)
                        query field is month,hour

                        querystr = ["month==3","hour==1"] ==> "month==3 and hour==1"

            plot_hist2d_windVec2(data=..,y="WD",month=3)
                        query field is month
                        querystr = ["month==3"]

            :param x:
            :param y:
            :param data:
            If data is None (default) then y,x are interpreted as vector data; else interpreted as names of columns in data.

            :param func_dict: dictionary of functions to plot against the data. Must be of the format {..., index:(lambda function, legend of function), ...}
            :param axis:
            :param ax_props: dict with axis functions and parameters. { funcname : {parameters}}
            :param SelfCorrelation: whether or not to plot the self correlation test of the data (scatter plot of data in reverse order of y values)

            :param kwargs:
                    all the rest are used as query (use the and predicate on all the fields).
                    month=1
                    day = 2


            :return:
            """

        # kwargs_used_fields = ["axis", "fig", "bins", "levels"]
        # queryfield = [x for x in kwargs.key() if x not in kwargs_used_fields]
        # querystr = " and ".join(["{fieldname}=={fieldvalue}".format(fieldname=fieldname, fieldvalue=kwargs[fieldname]) for fieldname in queryfield])
        #
        # month_data = data.assign(year=data.index.year) \
        #     .assign(month=data.index.month) \
        #     .assign(day=data.index.day) \
        #     .assign(hour=data.index.hour) \
        #     .query(querystr)

        if axis is None:
            fig, axis = plt.subplots()
        else:
            plt.sca(axis)

        if data is None:
            xdata = x; ydata = y
        else:
            xdata = data[x]; ydata = data[y]

        sns.scatterplot(x=xdata, y=ydata, ax = axis, zorder=2)
        if SelfCorrelation:
            sns.scatterplot(x=np.array(xdata), y=np.array(ydata)[::-1], ax = axis, zorder=1)

        if not func_dict is None:
            # z = np.arange(min(xdata) - 1, max(xdata) + 1, 0.01)
            # for fun in func_dict:
            #     axis.plot(z, np.array([func_dict[fun][0](zi) for zi in z]),
            #              label=func_dict[fun][1])
            z1 = np.append(np.arange(min(xdata) - 1, 0, 0.01), -0.00001)
            z2 = np.arange(0.00001, max(xdata) + 1, 0.01)
            for fun in func_dict:
                lines = axis.plot(z1, np.array([func_dict[fun][0](zi) for zi in z1]),
                         label=func_dict[fun][1])
                axis.plot(z2, np.array([func_dict[fun][0](zi) for zi in z2]), color = lines[0].get_color())

        if not ax_props is None:
            for func in ax_props:
                getattr(axis, func)(**ax_props[func])
