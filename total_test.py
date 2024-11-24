import requests

events_store_url = 'http://127.0.0.1:8020'
recommendations_url = 'http://127.0.0.1:8000'

# headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
# params = {"user_id": 1291248, "k": 3}

# resp = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
# if resp.status_code == 200:
#     online_recs = resp.json()
# else:
#     online_recs = None
#     print(f"status code: {resp.status_code}")

# print(online_recs)


user_id = 1291250
event_item_ids = [7144, 16299, 5907, 18135]
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

for event_item_id in event_item_ids:
    resp = requests.post(events_store_url + "/put", 
                         headers=headers, 
                         params={"user_id": user_id, "item_id": event_item_id})
                         
params = {"user_id": 1291250, 'k': 10}
resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

recs_offline = resp_offline.json()["recs"]
recs_online = resp_online.json()["recs"]
recs_blended = resp_blended.json()["recs"]

print(recs_offline)
print(recs_online)
print(recs_blended)