import pandas as pd

# Define chunk size for memory efficiency
chunk_size = 10000


chunks = []

for chunk in pd.read_csv('/kaggle/input/cleaned-dataset/cleaned_dataset.csv', chunksize=chunk_size):
    # Create a binary indicator for reactions
    chunk['reaction_flag'] = 1
    
    # Pivot 
    chunk_pivot = chunk.pivot_table(
        index=['primaryid', 'caseid', 'drugname'],  #
        columns='pt', 
        aggfunc='size',  
        fill_value=0  
    ).reset_index()
    chunks.append(chunk_pivot)


reaction_matrix = pd.concat(chunks, ignore_index=True)

reaction_matrix_sampled = reaction_matrix.sample(n=20000, random_state=42)

reaction_matrix_sampled.to_csv('reaction_matrix_sampled.csv', index=False)
print("Sampled reaction matrix saved as 'reaction_matrix_sampled.csv'")