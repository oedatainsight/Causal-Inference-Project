# coding: utf-8
# author: Jing Li
# date: 2019/04/01
import os
import pandas as pd
import warnings
from datetime import datetime

# local directory to save files.
data_dir = "faers_ascii_2024q3/ASCII"
directoryPath = os.getcwd() + '/' + data_dir

data_merge_dir = "FAERSdataMerge"  # directory to save merged file
merge_dir_path = os.getcwd() + '/' + data_merge_dir
if not os.path.isdir(data_merge_dir):
    os.makedirs(data_merge_dir)

merge_file_name = "faersDataLightGBM.csv"

# ignore warnings
warnings.filterwarnings('ignore')


def mergeData(data_dir):
    # merge all files(DEMO, DRUG, REAC/OUTC) in data_dir
    print("Merge files.\t" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    demo = pd.DataFrame(columns=['primaryid', 'caseid', 'age', 'sex', 'wt'])
    drug = pd.DataFrame(columns=['primaryid', 'caseid', 'role_cod', 'drugname'])
    reac = pd.DataFrame(columns=['primaryid', 'caseid', 'pt'])
      # Initialize outc DataFrame

    outc = pd.DataFrame(columns=['primaryid', 'caseid', 'outc_cod'])
    for filename in os.listdir(directoryPath):
        if "DEMO" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            demo_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='skip')
            demo_df = demo_df[1:]
            demo = pd.concat([demo, demo_df], ignore_index=True)
        if "DRUG" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            drug_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='skip')
            drug_df = drug_df[1:]
            drug = pd.concat([drug, drug_df], ignore_index=True)
        if "OUTC" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            outc_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='skip')
            outc_df = outc_df[1:]
            outc = pd.concat([outc, outc_df], ignore_index=True)
        if "REAC" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            reac_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='skip')
            reac_df = reac_df[1:]
            reac = pd.concat([reac, reac_df], ignore_index=True)
            
    # merge files based on primary report id and case id
    demo_drug_df = pd.merge(demo, drug, on=('primaryid', 'caseid'), how='inner')
    # demo_drug_outc_df = pd.merge(demo_drug_df, outc, on=('primaryid', 'caseid'), how='inner')
    demo_drug_reac_df = pd.merge(demo_drug_df, reac, on=('primaryid', 'caseid'), how='inner')

    # save file
    # demo_drug_outc_df.to_csv(merge_dir_path + '/' + merge_file_name, header=True, index=False)
    demo_drug_reac_df.to_csv(merge_dir_path + '/' + merge_file_name, header=True, index=False)
    print("Merge files done!\t" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def main():
    mergeData(data_dir)


if __name__ == '__main__':
    main()
