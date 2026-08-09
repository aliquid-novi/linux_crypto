import psutil
from pathlib import Path
import json

# Confirm PID exists
def is_process_running(pid):
    return psutil.pid_exists(pid)

pid = Path('./runtime/strategy.pid')
pid_num = int(pid.read_text().strip())
print(pid_num)
print(f"Check 1: Is strategy running?", is_process_running(pid_num))

# Read runtime/heartbeat.json 

with open('./runtime/heartbeat.json', 'r') as file:
    data = json.load(file)

print("Check 2: Heartbeat file:")

from datetime import datetime, timezone 

unix_timestamp = data['timestamp']
dt_utc = datetime.fromtimestamp(unix_timestamp, tz = timezone.utc)
data['timestamp'] = dt_utc.strftime('%Y-%m-%d %H:%M:%S')

print(data)

current_utc_time = datetime.now(timezone.utc)
print(current_utc_time - dt_utc) 