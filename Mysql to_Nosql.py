import os
import mysql.connector
import json
from datetime import datetime, date


# Custom JSON encoder for datetime objects
def datetime_encoder(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime('%Y-%m-%d %H:%M:%S')  # Adjust the format as needed
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

db_config = {
    'host': 'ozonetel-dev-testdb.c4hiinsn2ket.ap-south-1.rds.amazonaws.com',
    'user': 'rajesh_d',
    'password': 'Xa@s1Lt$ei3eix',
    'database': 'cloudagent',
}

try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    query = """
    SELECT 
    c.ucid, 
    c.report_id, 
    c.starttime, 
    c.endtime, 
    CASE WHEN c.call_data = '' THEN NULL ELSE c.call_data END AS call_data, 
    c.dest, 
    s.location_id AS location, 
    c.status,
    c.campaign_id, 
    cp.campign_name, 
    a.agent_name, 
    c.tried_number, 
    c.data_id, 
    CASE WHEN c.audio_file = '' THEN NULL ELSE c.audio_file END AS audio_file, 
    REPLACE(CASE WHEN c.disposition = '' THEN NULL ELSE c.disposition END, '$comma$', ',') AS disposition,
    c.tta, 
    CASE WHEN c.hangupby = '' THEN NULL ELSE c.hangupby END AS hangupby,
    c.monitor_ucid, 
    c.agent_id,
    CASE WHEN c.agentid = '' THEN NULL ELSE c.agentid END AS agentid,
    c.dialout_id, 
    c.skill_id, 
    s.skillname, 
    d.dialout_name, 
    REPLACE(CASE WHEN c.comment = '' THEN NULL ELSE c.comment END, '$comma$', ',') AS comment,
    REPLACE(CASE WHEN c.uui = '' THEN NULL ELSE c.uui END, '$comma$', ',') AS uui,
    CASE WHEN c.did = '' THEN NULL ELSE c.did END AS did,
    CASE WHEN c.type = '' THEN NULL ELSE c.type END AS type,
    c.transfer_agentid, 
    c.transferskill_id, 
    ta.agent_name AS transferagent, 
    CASE WHEN c.transfer_type = '' THEN NULL ELSE c.transfer_type END AS transfer_type,
    ts.skillName AS transferskillname, 
    CASE WHEN c.transfer_number = '' THEN NULL ELSE c.transfer_number END AS transfer_number,
    c.blind_transfer,
    CASE c.`is_offline` WHEN TRUE THEN 'OFFLINE' WHEN FALSE THEN 'ONLINE' END AS `campaigntype`,
    /*CASE WHEN c.campaigntype = '' THEN NULL ELSE c.campaigntype END AS campaigntype,*/
    CASE WHEN c.dial_status = '' THEN NULL ELSE c.dial_status END AS dial_status,
    c.call_completed, 
    CASE WHEN c.customer_status = '' THEN NULL ELSE c.customer_status END AS customer_status,
    CASE WHEN c.agent_status = '' THEN NULL ELSE c.agent_status END AS agent_status,
    c.pri_id, 
    c.fwp_id, 
    CASE WHEN c.reference_no = '' THEN NULL ELSE c.reference_no END AS reference_no,
    f.phone_number AS dialednumber, 
    f.phone_name, 
    c.holdduration, 
    w.wrapupstarttime, 
    w.wrapupendtime,
    c.agent_tta, 
    c.Customer_TTA, 
    CASE WHEN c.videorecordingurl = '' THEN NULL ELSE c.videorecordingurl END AS videorecordingurl,
    c.user_id, 
    -- c.user AS username, 
    CONCAT('kub_test_usr_',c.user_id) AS `username`,
   -- CONCAT('kub_test_usr_',c.user_id) AS `user`, 
    -- c.db, 
    -- c.table, 
    DATE(c.`starttime`) AS `Dt`
FROM Report_Hist c
JOIN campaign cp ON cp.campaign_id = c.campaign_id 
LEFT JOIN skill s ON s.id = c.skill_id 
LEFT JOIN agent a ON a.id = c.agent_id 
LEFT JOIN dialout_number d ON d.id = c.dialout_id 
LEFT JOIN agent ta ON ta.agent_id = c.transfer_agentid 
LEFT JOIN skill ts ON ts.id = c.transferSkill_id
LEFT JOIN fwp_numbers f ON f.id = c.fwp_id 
LEFT JOIN (
    SELECT ucid, 
    MIN(start_time) wrapupstarttime, 
    MAX(end_time) wrapupendtime 
    FROM agent_data
    WHERE event = 'ACW'
    GROUP BY ucid
) w ON c.ucid = w.ucid
WHERE c.user_id IN (7076)
and c.`starttime` BETWEEN '2022-07-26 00:00:00' and '2022-07-26 23:59:59' ;

"""

    cursor.execute(query)
    rows = cursor.fetchall()

    json_objects = []

    for row in rows:
        data = {}
        column_names = [i[0] for i in cursor.description]
        for i, col in enumerate(column_names):
            data[col] = row[i]

        json_object = {
            "EventID": data["report_id"],
            "User": data["user_id"],
            "EventTrackID": data["monitor_ucid"],
            "EventLocation": "INCCASS",
            "EventSource": "CAApp",
            "EventType": data["type"],
            "EventSubType": "Report",
            "Timestamp": data['starttime'],
            "Properties": data
        }

        json_objects.append(json_object)

    json_output = json.dumps(json_objects, indent=4, default=datetime_encoder)

    desktop_path = os.path.expanduser(r'C:\Users\Dandi Rajesh\OneDrive - OZONETEL Communications pvt ltd\Desktop')

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_filename = os.path.join(desktop_path, f'REPORT_for_{timestamp}.json')

    with open(output_filename, 'w') as json_file:
        json_file.write(json_output)

    print(f"JSON data has been saved to '{output_filename}'")

except mysql.connector.Error as err:
    print(f"Error connecting to the database: {err}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()
