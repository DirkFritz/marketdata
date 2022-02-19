import requests
import time
import urllib3
urllib3.disable_warnings()

timeout_cnt = 0
while timeout_cnt < 10:
    r = requests.get(f"https://localhost:5000/v1/api/sso/validate",verify=False)
    print(r.status_code)
    if(r.status_code != 200):
        timeout_cnt = timeout_cnt +1
    else:
        print(r.json())

    time.sleep(60)