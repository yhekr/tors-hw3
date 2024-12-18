import sys
import threading
from flask import Flask, request, jsonify
from dataclasses import dataclass, asdict
import json
import requests
import copy

@dataclass
class Record:
    f_key: str
    f_val: str
    f_op: str
    f_src: int
    f_ts: dict

NODES_CFG = {
    0: ('127.0.0.1', 15501, 0.8),
    1: ('127.0.0.1', 15502, 0.8),
    2: ('127.0.0.1', 15503, 0.8)
}

g_heartbeat_duration = 40
g_shutdown_flag = False
g_node_id = -1
g_peers = dict()
g_heartbeat_event = threading.Event()
g_store_lock = threading.Lock()
g_store = {
    'blacklist': [],
    'log': [],
    'data': {},
    'data_ts': {},
    'cur_ts': {},
    'hb_timer': None
}

def setup_timer():
    with g_store_lock:
        g_store['hb_timer'] = threading.Timer(g_heartbeat_duration, g_heartbeat_event.set)
        g_store['hb_timer'].start()

def step_clock():
    with g_store_lock:
        if g_node_id not in g_store['cur_ts']:
            g_store['cur_ts'][g_node_id] = 1
        else:
            g_store['cur_ts'][g_node_id] += 1

def hb_loop():
    setup_timer()
    while not g_shutdown_flag:
        try:
            if g_heartbeat_event.wait(timeout=0.5):
                g_heartbeat_event.clear()
                data_payload = None
                with g_store_lock:
                    data_payload = json.dumps([asdict(entry) for entry in g_store['log']], ensure_ascii=False, indent=4)
                for peer_id in g_peers.keys():
                    if peer_id != g_node_id and str(peer_id) not in g_store['blacklist']:
                        (host, port, _) = g_peers[peer_id]
                        addr = f"http://{host}:{port}/merge"
                        threading.Thread(target=dispatch, args=(addr, data_payload,), daemon=True).start()
                setup_timer()
        except:
            pass

def dispatch(addr, data_payload):
    requests.put(addr, data=data_payload, headers={'Content-Type': 'application/json', 'Node': str(g_node_id)}, timeout=5)

def is_more_recent(entry: Record):
    if entry.f_key not in g_store['data_ts']:
        return True
    cur_newer = False
    entry_newer = False
    for node_id, t in entry.f_ts.items():
        if node_id not in g_store['data_ts'][entry.f_key] or g_store['data_ts'][entry.f_key][node_id] < t:
            entry_newer = True
        elif g_store['data_ts'][entry.f_key][node_id] > t:
            cur_newer = True
    cur_nodes = set(g_store['data_ts'][entry.f_key].keys())
    entry_nodes = set(entry.f_ts.keys())
    if len(cur_nodes - entry_nodes) > 0:
        cur_newer = True
    if cur_newer:
        if not entry_newer:
            return False
    else:
        if not entry_newer:
            return False
        return True
    return entry.f_val > g_store['data'][entry.f_key]

def commit_record(entry: Record):
    verdict = is_more_recent(entry)
    if verdict:
        if entry.f_op == 'set':
            g_store['data'][entry.f_key] = entry.f_val
        elif entry.f_op == 'del' and entry.f_key in g_store['data']:
            del g_store['data'][entry.f_key]
        if entry.f_key not in g_store['data_ts']:
            g_store['data_ts'][entry.f_key] = {}
        for node_id, t in entry.f_ts.items():
            g_store['data_ts'][entry.f_key][node_id] = t
        g_store['log'].append(entry)

def adjust_clock(t):
    for node_id, tm in t.items():
        if node_id not in g_store['cur_ts'] or g_store['cur_ts'][node_id] < tm:
            g_store['cur_ts'][node_id] = tm

app = Flask(__name__)

@app.route('/update', methods=['PATCH'])
def update_records():
    try:
        with g_store_lock:
            if 'Node' in request.headers and request.headers['Node'] in g_store['blacklist']:
                return
        payload = request.get_json()
        if not isinstance(payload, dict):
            return jsonify({'error': 'invalid body format'}), 400
        for k, v in payload.items():
            step_clock()
            op_type = 'set' if v else 'del'
            with g_store_lock:
                entry = Record(f_key=k, f_val=v, f_op=op_type, f_src=g_node_id, f_ts=copy.deepcopy(g_store['cur_ts']))
                commit_record(entry)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': 'caught exception'}), 500

@app.route('/merge', methods=['PUT'])
def merge_records():
    try:
        with g_store_lock:
            if 'Node' in request.headers and request.headers['Node'] in g_store['blacklist']:
                return
        received_ops = request.get_json()
        entries = []
        for item in received_ops:
            entries.append(Record(
                f_key=item['f_key'],
                f_val=item['f_val'],
                f_op=item['f_op'],
                f_src=item['f_src'],
                f_ts={int(x): y for x, y in item['f_ts'].items()}
            ))
        for entry in entries:
            with g_store_lock:
                commit_record(entry)
                adjust_clock(entry.f_ts)
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': 'caught exception'}), 500

@app.route('/exclude', methods=['PUT'])
def exclude_nodes():
    with g_store_lock:
        g_store['blacklist'] = request.headers['Nodes'].split(',')
        return jsonify({'status': 'blacklist was updated'}), 200

@app.route('/records', methods=['GET'])
def get_records():
    with g_store_lock:
        return jsonify(g_store['data']), 200

@app.route('/snapshot', methods=['GET'])
def snapshot_state():
    with g_store_lock:
        r = {
            'data': g_store['data'],
            'data_ts': g_store['data_ts'],
            'cur_ts': g_store['cur_ts']
        }
        return jsonify(r), 200

def server_main(node_id):
    global g_node_id
    global g_peers
    global g_shutdown_flag
    g_node_id = node_id
    g_peers = NODES_CFG
    (h, p, t) = g_peers[g_node_id]
    global g_heartbeat_duration
    g_heartbeat_duration = t
    g_store['cur_ts'] = {}
    th = threading.Thread(target=hb_loop)
    th.start()
    try:
        app.run(h, p)
    except KeyboardInterrupt:
        g_shutdown_flag = True
        th.join()

node_arg = int(sys.argv[1])
server_main(node_arg)
