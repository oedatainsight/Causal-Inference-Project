import pandas as pd

# Read in chunks
chunk_size = 10000
chunks = []
for chunk in pd.read_csv('/Users/oziedokobi/Desktop/BAyesalabproject/faers_xml_2024q3/XML/FAERSdataMerge/faersDataLightGBM.csv', chunksize=chunk_size):
    chunk['route'] = chunk['route'].fillna('Unknown')  # Perform processing here
    chunks.append(chunk)

# Combine processed chunks
data_cleaned = pd.concat(chunks)


data_cleaned.loc[:, 'route'] = data_cleaned['route'].fillna('Unknown')


import dask.dataframe as dd

# Load data with Dask
ddf = dd.read_csv('/Users/oziedokobi/Desktop/BAyesalabproject/faers_xml_2024q3/XML/FAERSdataMerge/faersDataLightGBM.csv')

# Perform transformations
ddf['route'] = ddf['route'].fillna('Unknown')

# Compute the result (trigger processing)
data_cleaned = ddf.compute()

data_cleaned['primaryid'] = data_cleaned['primaryid'].astype('int32')
data_cleaned['route'] = data_cleaned['route'].astype('category')