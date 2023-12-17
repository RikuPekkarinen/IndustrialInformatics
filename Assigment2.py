import paho.mqtt.client as mqtt
import json
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request

SUBSCRIPTION_TOPIC = "ii23/telemetry/rob"
cursor = None
robot_states = {}


# Calculates mean time between failures
def time_between_failures(history):

    failure_times = []
    last_down_timestamp = None
    last_event_was_down = False
    for event in history:
        if "DOWN" in event[1]:
            if last_down_timestamp is None:
                pass
            else:
                failure_times.append(calculate_time_difference(last_down_timestamp, event[2]).total_seconds())

            last_event_was_down = True
        else:
            if last_event_was_down:
                last_down_timestamp = event[2]
            last_event_was_down = False

    if len(failure_times) > 1:
        failure_times.sort()
        mean = (failure_times[0] + failure_times[-1])/2

    elif len(failure_times) == 1:
        mean = failure_times[0]

    else:
        mean = 0

    return mean


# Calculates time in state and state count
def calculate_state_times(history):

    time_in_state = {"IDLE": 0, "PROCESSING": 0, "DOWN": 0}
    state_count = {"IDLE": 0, "PROCESSING": 0, "DOWN": 0}
    current_state = ""
    last_time_stamp = ""
    for event in history:
        if current_state == "":
            if "IDLE" in event[1]:
                current_state = "IDLE"
            elif "PROCESSING" in event[1]:
                current_state = "PROCESSING"
            else:
                current_state = "DOWN"
            last_time_stamp = event[2]
        else:
            time_in_state[current_state] += calculate_time_difference(last_time_stamp, event[2]).total_seconds()
            state_count[current_state] += 1
            if "IDLE" in event[1]:
                current_state = "IDLE"
            elif "PROCESSING" in event[1]:
                current_state = "PROCESSING"
            else:
                current_state = "DOWN"
            last_time_stamp = event[2]

    if current_state != "":

        current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        current_time_string = current_time_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        state_count[current_state] += 1
        time_in_state[current_state] += calculate_time_difference(last_time_stamp, current_time_string).total_seconds()

    return time_in_state, state_count


# gets all information from specific time interval
def time_between_time(robID, min_time, max_time):
    # times need to be string
    query = f"SELECT * FROM history_table WHERE deviceID = ? AND time BETWEEN ? AND ?"
    cursor.execute(query, (robID, min_time, max_time))
    result = cursor.fetchall()
    return result


# formats time to readable form
def format_time(time):
    date_part, time_part = time.split("T")
    time_part, _ = time_part.split("Z")
    seconds, nanoseconds = time_part.split(".")
    datetime_object = datetime.strptime(f"{date_part} {seconds}", "%Y-%m-%d %H:%M:%S")
    datetime_object += timedelta(microseconds=int(nanoseconds.ljust(6, '0')))

    readable_time = datetime_object.strftime("%Y-%m-%d %H:%M:%S")
    return readable_time


# Calculates time difference between two timestamp strings
def calculate_time_difference(time_str_1, time_str_2):
    # Convert string timestamps to datetime objects
    format_str = '%Y-%m-%dT%H:%M:%S.%f'
    #          "2023-12-08T15:19:25.896436995Z"
    # cut some accuracy from time so code can handle it
    time_str_1 = time_str_1[:23]
    time_str_2 = time_str_2[:23]
    timestamp_1 = datetime.strptime(time_str_1, format_str)
    timestamp_2 = datetime.strptime(time_str_2, format_str)

    # Calculate the time difference
    time_difference = timestamp_2 - timestamp_1

    return time_difference


# Handles sql queries for finding out current device status
def get_state_for_device(device_id):
    global cursor
    cursor.execute("""
       SELECT state, time
       FROM history_table
       WHERE deviceId = ?
       ORDER BY time DESC
       LIMIT 1
    """, (device_id,))
    status = cursor.fetchall()
    if len(status) == 0:
        return "OFF", 0
    status = status[0]
    state = status[0]
    time = status[1]
    time = format_time(time)

    return state, time


# give log update and keep count of how long robot has been in Idle or Down state
def manage_timers(robId, state):

    alert = "None"

    if robId not in robot_states:       # can be done where dict is made
        # if robID is not in the dictionary
        robot_states[robId] = [0, "None"]

    if state in ("READY-IDLE-BLOCKED", "READY-IDLE-STARVED"):

        if robot_states[robId][1] != "Idle":
            robot_states[robId][0] = 0

        # here we add the interval this funtion is called (now 1/s)
        robot_states[robId][0] += 1
        robot_states[robId][1] = "Idle"

        timeinstate = robot_states[robId][0]
        idle_threshold = 20
        if timeinstate >= idle_threshold:

            alert = "Idle"
            if timeinstate == idle_threshold:
                current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                current_time_string = current_time_utc.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Robot {robId} idle for at least {idle_threshold}s at {current_time_string}")

    elif state == "DOWN":
        if robot_states[robId][1] != "Down":
            robot_states[robId][0] = 0
        robot_states[robId][0] += 1
        robot_states[robId][1] = "Down"

        timeinstate = robot_states[robId][0]
        down_threshold = 300
        if timeinstate >= 300:

            alert = "Down"
            if timeinstate == down_threshold:
                current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                current_time_string = current_time_utc.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Robot {robId} idle for at least {down_threshold}s at {current_time_string}")

    elif state in ("OFF", "SETUP", "READY-PROCESSING-EXECUTING", "READY-PROCESSING-ACTIVATED"):
        robot_states[robId][0] = 0
        robot_states[robId][1] = "None"

    else:
        print("Error in state reading")

    return alert


#
def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print("Incoming payload: ", payload)
    data = json.loads(payload)
    deviceId = data.get("deviceId")
    state = data.get("state")
    time = data.get("time")
    sequenceNumber = data.get("sequenceNumber")

    cursor.execute("INSERT INTO history_table(deviceId, state, time, "
                   "sequenceNumber) VALUES (?, ?, ?, ?)",
                   (deviceId, state, time, sequenceNumber))


def main():
    # SQLite Connection
    conn = sqlite3.connect("SQL_DB.sqlite", check_same_thread=False)
    global cursor
    cursor = conn.cursor()
    conn.commit()

    # Create a history table
    cursor.execute("DROP TABLE IF EXISTS history_table;")
    cursor.execute("""CREATE TABLE IF NOT EXISTS history_table (
                        deviceId TEXT,
                        state TEXT,
                        time TEXT,
                        sequenceNumber TEXT
                    )""")

    client = mqtt.Client()
    client.connect("broker.mqttdashboard.com")

    for i in range(1, 10):
        client.subscribe(f"{SUBSCRIPTION_TOPIC}{i}", 0)

    client.on_message = on_message
    client.loop_start()

    app = Flask(__name__)

    # This prevents Flask from posting unnecessary console logs
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    @app.route("/", methods=["GET"])
    def static_page():
        page_name = "dashboard"
        nID = request.args.get('nID')
        return render_template('%s.html' % page_name, nID=nID)

    @app.route("/state", methods=["POST"])
    def get_state_data():

        robot_state_dict = {}
        for i in range(1, 11):
            robot_id = f"rob{str(i)}"

            state, timestamp = get_state_for_device(robot_id)
            alert = manage_timers(robot_id, state)
            state_data = {
                "state": state,
                "time": timestamp,
                "alert": alert
            }
            robot_state_dict.update({i: state_data})

        state_data_json = json.dumps(robot_state_dict)
        return state_data_json

    @app.route("/history", methods=["POST"])
    def get_robot_history():

        data = request.json
        robot_id = f"rob{str(data['id'])}"

        try:
            from_datetime = datetime.strptime(data["fromDateTime"], '%Y-%m-%d %H:%M')
            from_datetime = from_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            to_datetime = datetime.strptime(data["toDateTime"], '%Y-%m-%d %H:%M')
            to_datetime = to_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            history = time_between_time(robot_id, from_datetime, to_datetime)

            state_time, state_count = calculate_state_times(history)
            mean_time_between_failures = time_between_failures(history)

            robot_data = {"HISTORY": history, "STATE_TIME": state_time, "STATE_COUNT": state_count,
                          "MEAN_TIME": mean_time_between_failures}
            robot_data_json = json.dumps(robot_data)
            return robot_data_json

        except ValueError as e:

            print(f"Error with search parameters: {e}")
            return {"HISTORY": [], "STATE_TIME": {}, "STATE_COUNT": {}, "MEAN_TIME": 0}

        except Exception as e:

            print(f"Error {e}")
            return {"HISTORY": {}, "STATE_TIME": {}, "STATE_COUNT": {}, "MEAN_TIME": 0}

    #app.run(debug=False, host='localhost', port=5000)
    app.run()

if __name__ == '__main__':
    main()