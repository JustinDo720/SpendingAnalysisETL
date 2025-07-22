import requests 
from typing import Dict, Any, List 

"""
get_uploaded_files()
    - Find all the uploaded files 

get_summary(upload_id)
    - Access the summary endpoint for each 


Local Endpoints:
    - All Uploaded Files:
        - http://127.0.0.1:8000/uploads/
    - Summary Endpoint:
        - http://127.0.0.1:8000/uploads/<int:upload_id>/summary/
"""

BASE_URL = 'http://127.0.0.1:8000'

def get_summary(upload_id: int) -> Dict[str, Any]:
    try:
        r = requests.get(BASE_URL + f'/uploads/{upload_id}/summary')
        r.raise_for_status()

        # If our request went through...
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f'Failed to fetch summary for upload ID: {upload_id}')
        return {} 

def get_uploaded_files() -> List[int]:
    try:
        r = requests.get(BASE_URL + '/uploads/')
        r.raise_for_status() 
        data = r.json() 
        uploaded_files_id = [int(d['id']) for d in data['uploaded_files']]
        return uploaded_files_id
    except requests.RequestException as e:
        print(f'Failed to fetch uploaded files...')
        return [] 
