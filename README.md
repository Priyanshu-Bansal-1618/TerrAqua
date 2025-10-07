1. Data Retrieval
A Python script (data_retriever.py) is developed to fetch groundwater level data from the CGWB India WRIS portal API.

The API restricts queries to 15-day intervals; the script splits the requested date range accordingly and performs parallel querying using joblib for fast data acquisition.

The script consolidates data chunks, removes duplicates, and saves the cleaned data in CSV format for further use.


2. Data Analysis and Visualization
An interactive Jupyter notebook (STL_demonstration.ipynb) is used to analyze the downloaded DWLR groundwater level data.

The data undergoes preprocessing including time parsing, filtering by measurement times, and anomaly detection.

The STL (Seasonal and Trend decomposition using Loess) method decomposes the groundwater level time series into trend, seasonal, and residual components.

Residuals are statistically analyzed for anomalies using a 3-sigma threshold approach.

Visualizations include raw groundwater level time series, decomposed components, and plots marking detected anomalies.
