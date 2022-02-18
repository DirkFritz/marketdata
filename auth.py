import requests


r = requests.get(f"https://localhost:5000/v1/api/sso/validate",verify=False)
print(r.json())

    
r = requests.post(f"https://localhost:5000/v1/api/iserver/reauthenticate",verify=False)
print(r.json())