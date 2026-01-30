import requests
import json
import os

def download_file():
    dataset_doi = "doi:10.7910/DVN/OVHAW8"
    api_url = f"https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId={dataset_doi}"
    
    print(f"Querying API: {api_url}")
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    
    files = data['data']['latestVersion']['files']
    target_file = None
    
    # Check for .tab or .csv
    for f in files:
        label = f['label'].lower()
        if 'curated-solubility-dataset' in label and (label.endswith('.csv') or label.endswith('.tab')):
            target_file = f
            break
            
    if not target_file:
        raise ValueError("Could not find curated-solubility-dataset file in the dataset")
        
    file_id = target_file['dataFile']['id']
    download_url = f"https://dataverse.harvard.edu/api/access/datafile/{file_id}"
    
    print(f"Found file: {target_file['label']} (ID: {file_id})")
    print(f"Downloading from: {download_url}")
    
    os.makedirs("download", exist_ok=True)
    file_content = requests.get(download_url)
    file_content.raise_for_status()
    
    # Check if it's tab or comma
    # Usually .tab is TSV, but let's check content or just save it.
    # We'll save as 'dataset.csv' for DVC consistency, but verify in build script.
    
    with open("download/dataset.csv", "wb") as f:
        f.write(file_content.content)
    print("Download complete: download/dataset.csv")

if __name__ == "__main__":
    download_file()
