import requests

r = requests.get(f"https://localhost:5000/v1/api/iserver/accounts",verify=False)
print(r.json())
