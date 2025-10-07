data_retriever.py

Fetches groundwater level data from the CGWB WRIS portal using parallelized API requests, handling date splitting and efficient aggregation.
This tackles the problem of limited amount of data retriveal(1000 at a time at max). 

Cleans and saves the data to CSV format for further analysis, optimizing for multi-core systems.



STL_demonstration.ipynb

Loads and preprocesses DWLR groundwater data, applying STL decomposition to separate trend, seasonal, and residual components.

Detects anomalies in groundwater level time series, visualizing both the raw and decomposed data for insightful pattern recognition.
