

import pandas as pd

# Load your cleaned dataset
data = pd.read_csv('/Users/oziedokobi/Desktop/BAyesalabproject/faers_xml_2024q3/XML/cleaned_dataset.csv')

# Create a binary indicator for reactions
data['reaction_flag'] = 1

# Pivot the data to create the reaction matrix
reaction_matrix = data.pivot_table(
    index=['primaryid', 'caseid', 'drugname'],  # Rows
    columns='pt',                               # Columns
    values='reaction_flag',                     # Values
    aggfunc='max',                              # Aggregate using max (1 if reaction exists)
    fill_value=0                                # Fill missing reactions with 0
).reset_index()


# Save to a CSV for use in BayesiaLab or other tools
# Randomize the rows and limit to 20,000 rows
reaction_matrix_sampled = reaction_matrix.sample(n=10000, random_state=42)  # Set random_state for reproducibility