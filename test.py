import subprocess
import requests
import random
import time

NODES_CFG = {
    0: ("127.0.0.1", 15501, 0.8),
    1: ("127.0.0.1", 15502, 0.8),
    2: ("127.0.0.1", 15503, 0.8)
}

def start_process(idx):
    return subprocess.Popen(['python3', 'main.py', str(idx)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def end_test(instances, status):
    for proc in instances:
        proc.terminate()
    if not status:
        exit(1)

def verify_state(node_id, expected_data, expected_ts):
    (host, port, _) = NODES_CFG[node_id]
    response = requests.get(f"http://{host}:{port}/snapshot").json()
    if response['data'] != expected_data:
        print("Data inconsistency detected", node_id)
        print("Retrieved:", response['data'])
        print("Expected data:", expected_data)
        return False
    if response['cur_ts'] != expected_ts:
        print("Timestamp vector inconsistency detected")
        print("Retrieved:", response['cur_ts'])
        print("Expected:", expected_ts)
        return False
    return True

# Start three node processes
nodes = [start_process(0), start_process(1), start_process(2)]
time.sleep(1.5)

try:
    rid = random.randint(0, len(nodes) - 1)
    (h, p, _) = NODES_CFG[rid]
    addr = f"http://{h}:{p}/update"
    current_kv = {'k': 'v'}
    current_ts = {str(rid): 1}
    requests.patch(addr, json=current_kv, headers={'Content-Type': 'application/json'})
    time.sleep(1)
    for i in range(3):
        if not verify_state(i, current_kv, current_ts):
            end_test(nodes, False)

    rid = random.randint(0, len(nodes) - 1)
    (h, p, _) = NODES_CFG[rid]
    addr = f"http://{h}:{p}/update"
    current_kv['k2'] = 'v2'
    current_ts[str(rid)] = current_ts.get(str(rid), 0) + len(current_kv)
    requests.patch(addr, json=current_kv, headers={'Content-Type': 'application/json'})
    time.sleep(1)
    for i in range(3):
        if not verify_state(i, current_kv, current_ts):
            end_test(nodes, False)

    rid = random.randint(0, len(nodes) - 1)
    (h, p, _) = NODES_CFG[rid]
    addr = f"http://{h}:{p}/update"
    current_kv['k2'] = ''
    current_kv['k'] = 'v2'
    current_ts[str(rid)] = current_ts.get(str(rid), 0) + len(current_kv)
    requests.patch(addr, json=current_kv, headers={'Content-Type': 'application/json'})
    time.sleep(1)
    del current_kv['k2']
    for i in range(3):
        if not verify_state(i, current_kv, current_ts):
            end_test(nodes, False)

    (h0, p0, _) = NODES_CFG[0]
    a0 = f"http://{h0}:{p0}/update"
    (h1, p1, _) = NODES_CFG[1]
    a1 = f"http://{h1}:{p1}/update"
    current_ts['0'] = current_ts.get('0', 0) + 1
    current_ts['1'] = current_ts.get('1', 0) + 1
    new_kv = {'k3': 'v3'}
    requests.patch(a0, json=new_kv, headers={'Content-Type': 'application/json'})
    new_kv['k3'] = 'v4'
    requests.patch(a1, json=new_kv, headers={'Content-Type': 'application/json'})
    time.sleep(1.5)
    current_kv['k3'] = 'v4'
    for i in range(3):
        if not verify_state(i, current_kv, current_ts):
            end_test(nodes, False)

    print("passed")
except Exception as err:
    print("Error detected:", err)
    end_test(nodes, False)
