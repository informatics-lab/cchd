# cchd
An example of a Climate Change Hazard Dashboard (CCHD).

## IMPORTANT!
The point of this repo is to show **how to make a dashboard**. 

No scientific rigour has been undertaken in preparing the data shown on the dashboard beyond the analytical and statistical techniques included in the repo's code. No further inference should be made from the contents of the dashboard beyond the fact that such a dashboard may be constructed.

## Dashboard

This repo demonstrates an approach for producing a dashboard that makes it easier for users to explore some data. In this case, the dashboard allows us to explore the impacts of climate change on temperatures in the US.

### What's shown

This dashboard shows mean absolute temperature and anomaly temperature over each mainland US State for the years 1860-2100, with data from the climate future scenarios A1B and E1. Anomalies are calculated from three 30-year climatic periods 1971-2000, 1981-2010, 1991-2020.

On top of this, data is synthesized to demonstrate the concepts of vulnerability to a hazard and risk to population from vulnerability to the hazard. This synthesized data is generated from data already in the dashboard. It naïvely uses the area of the US state as a proxy for population (an assumption starkly shown to be very poor by Alaska) and multiplies that by the future offset in years from now and a random number between 0.8 and 1.2.

This synthesized data is of no value beyond demonstrating the art of the possible.

### Technologies used

There are a variety of different Python dashboarding technologies available. For the sake of making a choice, this dashboard makes use of [Holoviz](https://holoviz.org/index.html) technologies, particularly:

* [Panel](https://panel.pyviz.org/) (for making the dashboard app)
* [hvPlot](https://hvplot.pyviz.org/) (for making the plots shown on the dashboard)
* [Param](https://param.pyviz.org/) (for defining the interactivity parameters)
* [Colorcet](https://colorcet.pyviz.org/) (for selecting colormaps to make the dashboard's data more intuitive)

Implicitly also used are:

* [GeoViews](http://geoviews.org/) (for plotting the geographic data - hvPlot hands off to GeoViews)
* [bokeh](https://bokeh.org/) (for actually making the plot and the interactive app)

## Using it

Panel makes it very easy to create a dashboard from a Panel app in a Python script or a Jupyter notebook. Thus you can run the app with the following commands:

```bash
$ cd cchd
$ panel serve climate_change_risk_dashboard_eg.ipynb --show
```

This will open the app in a new tab in your default browser.

Note that your local Python environment will need to provide at least the required library dependencies of the app. For example:

```bash
$ conda create -n cchd -c conda-forge python
$ conda activate cchd
$ conda install -c conda-forge panel hvplot geoviews colorcet pandas geopandas
```

Note: the realtime dashboard uses both dask distributed and files from the [Iris sample data repository](https://github.com/SciTools/iris-sample-data) directly, rather than using the (provided) pre-processed version of this data. This adds extra dependencies:

```bash
$ conda activate cchd
$ conda install -c conda-forge distributed iris-sample-data
```