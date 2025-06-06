import os
import re
import json
import paramiko
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from elasticsearch import Elasticsearch
from bson import json_util
from bson import ObjectId, Timestamp, Binary, Code
from uuid import UUID


# === 配置區 ===
SSH_USER = "dba_tw"
SSH_KEY = os.path.expanduser("~/.ssh/id_rsa")
HOST_FILE = "host.list"
STATE_DIR = ".state"
LOG_BASE_PATH = "/data/logs/mongo"
ES_INDEX = "mongodb-slowlog"
SLOW_MS_THRESHOLD = 1000
MAX_LINES = 50000  # 最大讀取行數

# === ElasticSearch 初始化 ===
hosts = [
    {'host': '10.229.1.12', 'port': 9200},
    {'host': '10.229.1.21', 'port': 9200},
    {'host': '10.229.1.32', 'port': 9200}
]
es = Elasticsearch(hosts)

# === 工具函式 ===
def load_hosts(host_file_path):
    hosts = []
    with open(host_file_path, "r") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.strip().split()
            if len(parts) >= 2:
                hosts.append({
                    "ip": parts[0],
                    "hostname": parts[1],
                    "group": parts[2] if len(parts) > 2 else "default"
                })
    return hosts

def read_state(file):
    if os.path.exists(file):
        try:
            with open(file) as f:
                return json.load(f)
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ", f"[WARN] Failed to load state from {file}: {e}. Using empty state.")
            return {}
    return {}

def write_state(file, data):
    with open(file, "w") as f:
        json.dump(data, f)

def get_remote_file_size(ssh, filepath):
    stdin, stdout, stderr = ssh.exec_command(f"stat -c %s {filepath}")
    output = stdout.read().decode().strip()
    return int(output) if output.isdigit() else 0
                                     
def ssh_read_log_lines(host, user, key_path, logfile, offset=0):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=user, key_filename=key_path, timeout=10)

    log_size = get_remote_file_size(ssh, logfile)
    if log_size < offset:
        print(f"[{datetime.now().isoformat()}] [INFO] Log file size shrinked. Resetting offset to 0")
        offset = 0
    cmd = f"sudo tail -n {MAX_LINES} -c +{offset + 1} {logfile}"
    stdin, stdout, stderr = ssh.exec_command(cmd)
    data = stdout.read().decode("utf-8", errors="ignore").splitlines()
    new_offset = offset + sum(len(line.encode("utf-8")) + 1 for line in data)
    ssh.close()
    return data, new_offset

def extract_slow_query_lines(lines, min_ms=1000):
    slow_logs = []
    pattern = re.compile(r"(\d+)ms")
    for line in lines:
        m = pattern.search(line)
        if m:
            ms = int(m.group(1))
            if ms >= min_ms:
                slow_logs.append(line)
        elif '"msg":"Slow query"' in line or '"msg": "Slow query"' in line:
            slow_logs.append(line)
    return slow_logs

def default_json(obj):
    if isinstance(obj, (datetime, UUID, ObjectId, Timestamp, Binary, Code)):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def parse_json_slow_log(line):
    try:
        doc = json_util.loads(line)
        if doc.get("msg") != "Slow query" or "attr" not in doc:
            return None
        attr = doc["attr"]
        t_field = doc.get("t")
        if isinstance(t_field, dict):
            ts = t_field.get("$date", datetime.utcnow().isoformat())
        elif isinstance(t_field, datetime):
            ts = t_field.isoformat()
        else:
            ts = datetime.utcnow().isoformat()
        command = attr.get("command", {})

        return {
            "namespace": attr.get("ns"),
            "operation": next(iter(command)) if isinstance(command, dict) else "unknown",
            "duration": attr.get("durationMillis"),
            "raw_query": json.dumps(command, default=default_json, ensure_ascii=False),
            "query_template": json.dumps(normalize_query(command), default=default_json, ensure_ascii=False),
            "timestamp": ts,
            "plan_summary": attr.get("planSummary"),
            "keys_examined": attr.get("keysExamined"),
            "docs_examined": attr.get("docsExamined"),
            "bytes_read": attr.get("storage", {}).get("data", {}).get("bytesRead"),
            "time_reading_micros": attr.get("storage", {}).get("data", {}).get("timeReadingMicros"),
        }
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ", "[JSON PARSE FAIL]", e)
        return None


def parse_traditional_log_line(line):
    timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{4})', line)
    log_ts = timestamp_match.group(1) if timestamp_match else datetime.utcnow().isoformat() + "Z"
    regex = re.compile(r'command (?P<namespace>\S+) command: (?P<op>\w+) (?P<body>\{.*?\}) .*? (?P<duration>\d+)ms')
    plan_summary_match = re.search(r'planSummary: (\S+)', line)
    keys_examined_match = re.search(r'keysExamined:(\d+)', line)
    docs_examined_match = re.search(r'docsExamined:(\d+)', line)
    bytes_read_match = re.search(r'bytesRead: (\d+)', line)
    read_time_match = re.search(r'timeReadingMicros: (\d+)', line)

    plan_summary = plan_summary_match.group(1) if plan_summary_match else None
    keys_examined = int(keys_examined_match.group(1)) if keys_examined_match else None
    docs_examined = int(docs_examined_match.group(1)) if docs_examined_match else None
    bytes_read = int(bytes_read_match.group(1)) if bytes_read_match else None
    time_reading_micros = int(read_time_match.group(1)) if read_time_match else None

    m = regex.search(line)
    if not m:
        return None
    try:
        namespace = m.group("namespace")
        op = m.group("op")
        body_str = m.group("body")
        duration = int(m.group("duration"))

        body_str = re.sub(r'([,{])\s*([a-zA-Z0-9_\$]+)\s*:', r'\1"\2":', body_str)
        body_str = body_str.replace("'", '"')
        body_str = re.sub(r'UUID\("([^\"]+)"\)', r'"UUID(\1)"', body_str)
        body_str = re.sub(r'Timestamp\((\d+),\s?(\d+)\)', r'"Timestamp(\1,\2)"', body_str)
        body_str = re.sub(r'BinData\(\d+,\s?([a-zA-Z0-9+/=]+)\)', r'"BinData(\1)"', body_str)
        body_str = re.sub(r'new Date\((\d+)\)', r'\1', body_str)
        body_str = re.sub(r'ObjectId\("([^\"]+)"\)', r'"ObjectId(\1)"', body_str)

        stack = []
        for c in body_str:
            if c in '{[':
                stack.append(c)
            elif c == '}' and stack and stack[-1] == '{':
                stack.pop()
            elif c == ']' and stack and stack[-1] == '[':
                stack.pop()

        close_map = {'{': '}', '[': ']'}
        while stack:
            body_str += close_map[stack.pop()]

        body_json = json.loads(body_str)
    except Exception:
        return None

    return {
        "namespace": namespace,
        "operation": op,
        "duration": duration,
        "raw_query": m.group("body"),
        "query_template": json.dumps(normalize_query(body_json), ensure_ascii=False),
        "timestamp": log_ts,
        "plan_summary": plan_summary,
        "keys_examined": keys_examined,
        "docs_examined": docs_examined,
        "bytes_read": bytes_read,
        "time_reading_micros": time_reading_micros,
    }

def normalize_query(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, dict) and len(v) == 1 and next(iter(v)).startswith("$"):
                obj[k] = {next(iter(v)): "?"}
    IGNORED_KEYS = {"lsid", "$clusterTime", "signature", "keyId", "hash"}
    if isinstance(obj, dict):
        return {
            k: normalize_query(v)
            for k, v in obj.items()
            if k not in IGNORED_KEYS
        }
    elif isinstance(obj, list):
        return [normalize_query(i) for i in obj]
    else:
        return "?"

def send_to_elasticsearch(index, record):
    try:
        es.index(index=index, body=record)
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ", "[ES FAIL]", e)
        with open("es_fail.log", "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

def process_host(host, state_dir, min_ms, log_suffix="", state_suffix=""):
    host_id = host["hostname"]
    logfile_path = f"{LOG_BASE_PATH}/{host_id}.log{log_suffix}"
    state_file = os.path.join(state_dir, f"{host_id}{state_suffix}.json")

    state = read_state(state_file)
    offset = state.get("offset", 0)

    try:
        lines, new_offset = ssh_read_log_lines(
            host=host["ip"],
            user=SSH_USER,
            key_path=SSH_KEY,
            logfile=logfile_path,
            offset=offset
        )
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ", f"[{host_id}] SSH failed: {e}")
        return

    slow_lines = extract_slow_query_lines(lines, min_ms=min_ms)

    for line in slow_lines:
        if ' I REPL ' in line or 'mongodb_exporter' in line or 'listDatabases' in line or 'oplog.rs' in line:
            continue

        parsed = None
        if line.strip().startswith('{') and '"msg"' in line and 'Slow query' in line:
            parsed = parse_json_slow_log(line)
        else:
            parsed = parse_traditional_log_line(line)

        if parsed:
            parsed["source_host"] = host["hostname"]
            parsed["source_ip"] = host["ip"]
            send_to_elasticsearch(ES_INDEX, parsed)

    write_state(state_file, {"offset": new_offset})
    print(f"[{datetime.now().isoformat()}] ", f"[{host_id}] Processed {len(slow_lines)} slow queries")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, help='Date string in YYYYMMDD format for historical log')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel worker threads')
    args = parser.parse_args()

    log_suffix = f"-{args.date}" if args.date else ""
    state_suffix = f"_{args.date}" if args.date else ""

    os.makedirs(STATE_DIR, exist_ok=True)
    hosts = load_hosts(HOST_FILE)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_host, host, STATE_DIR, SLOW_MS_THRESHOLD, log_suffix, state_suffix): host
            for host in hosts
        }

        for future in as_completed(futures):
            host = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] ", f"[{host['hostname']}] Thread failed: {e}")

if __name__ == "__main__":
    main()
