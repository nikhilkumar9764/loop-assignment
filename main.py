from flask import Flask, jsonify, request
import uuid
import pandas as pd
import sqlite3
import pytz
from datetime import datetime, timedelta
from dateutil.parser import parse

app = Flask(__name__)


@app.route('/' , methods = ['GET'])
def hello_world():
       return "Hello there people!!"


@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    report_id = str(uuid.uuid4())
    print("jhdsjfsdhj kdfsfsdj")

    generate_report(report_id)

    return jsonify({'report_id': report_id})


@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    # Check if the report is still running
    if report_id in running_reports:
        return jsonify({'status': 'Running'})

    # Check if the report is complete
    if report_id in completed_reports:
        # Return the report CSV
        report_csv = completed_reports[report_id]
        return report_csv, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=report.csv'}

    # If the report ID is invalid, return an error message
    return jsonify({'error': 'Invalid report ID'})


def generate_report(report_id):
      conn = sqlite3.connect('store_activity.db')
      curr = conn.cursor()

      curr_time = datetime.now()
      print(curr_time)

      last_hr_range = ((curr_time-timedelta(hours=1)).time(), curr_time.time())
      last_day_range = ((curr_time-timedelta(days=1)).time(), curr_time.time())
      last_week_range = ((curr_time-timedelta(weeks=1)).time(), curr_time.time())

      stores_info_query = '''SELECT store.store_id , store.timezone_str , COALESCE(bhr.day,7) 
                           , bhr.start_time_local , bhr.end_time_local FROM store_timezones store LEFT JOIN 
                           store_business_hours bhr ON store.store_id = bhr.store_id '''

      curr.execute(stores_info_query)
      store_row_info = curr.fetchall()

      report_data = []
      for store_id, timezone_str, business_day , start_time_local , end_time_local in store_row_info :

              obser_query = ''' SELECT timestamp_utc , status from store_status where store_id = ? and timestamp_utc>= ?
                and timestamp_utc<= ?'''

              print((store_id , last_week_range[0], last_week_range[1]))
              curr.execute(obser_query, (store_id, str(last_week_range[0]), str(last_week_range[1])))
              observation_res = curr.fetchall()

              print(observation_res)
              final_res = calculate_uptime_downtime(start_time_local, end_time_local, timezone_str, observation_res)
              last_hour_uptime, last_hour_downtime = final_res[0], final_res[1]
              last_day_uptime, last_day_downtime = final_res[2], final_res[3]
              last_week_uptime, last_week_downtime = final_res[4], final_res[5]

              report_data.append((store_id , last_hour_uptime , last_day_uptime , last_week_uptime , last_hour_downtime,
                                  last_day_downtime, last_week_downtime))

      conn.close()
      report_csv = 'store_id,uptime_last_hour(in minutes),uptime_last_day(in hours),update_last_week(in hours),downtime_last_hour(in minutes),downtime_last_day(in hours),downtime_last_week(in hours)\n'
      for row in report_data:
          report_csv += ','.join(str(val) for val in row) + '\n'

      return report_csv


def calculate_uptime_downtime(start_time_local, end_time_local, timezone_str, records):
    timezone = pytz.timezone(timezone_str)

    start_time_utc = datetime.strptime(start_time_local, '%H:%M:%S').time()
    end_time_utc = datetime.strptime(end_time_local, '%H:%M:%S').time()
    start_time_utc = timezone.localize(datetime.combine(datetime.today(), start_time_utc)).astimezone(pytz.utc).time()
    end_time_utc = timezone.localize(datetime.combine(datetime.today(), end_time_utc)).astimezone(pytz.utc).time()

    total_business_time = timedelta()
    total_downtime = timedelta()
    total_uptime = timedelta()
    last_record_time = None
    last_status = None

    for record in records:
        print(record)
        record_time_utc = datetime.strptime(record[0], '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

        record_time_local = record_time_utc.astimezone(timezone)

        if record_time_local.time() < start_time_utc or record_time_local.time() >= end_time_utc:
            continue

        if last_record_time is not None:
            duration = record_time_utc - last_record_time
            if last_status == 'active':
                total_uptime += duration
            else:
                total_downtime += duration
            total_business_time += duration

        last_status = record[1]
        last_record_time = record_time_utc

    if last_status == 'active':
        total_uptime += datetime.utcnow().replace(tzinfo=pytz.utc) - last_record_time
    else:
        total_downtime += datetime.utcnow().replace(tzinfo=pytz.utc) - last_record_time
    total_business_time += datetime.utcnow().replace(tzinfo=pytz.utc) - last_record_time

    uptime_last_hour = total_uptime / total_business_time * timedelta(hours=1)
    uptime_last_day = total_uptime / total_business_time * timedelta(days=1)
    uptime_last_week = total_uptime / total_business_time * timedelta(days=7)
    downtime_last_hour = total_downtime / total_business_time * timedelta(hours=1)
    downtime_last_day = total_downtime / total_business_time * timedelta(days=1)
    downtime_last_week = total_downtime / total_business_time * timedelta(days=7)


    uptime_last_hour = round(uptime_last_hour.total_seconds() / 60, 2)
    uptime_last_day = round(uptime_last_day.total_seconds() / 3600, 2)
    uptime_last_week = round(uptime_last_week.total_seconds() / 3600, 2)
    downtime_last_hour = round(downtime_last_hour.total_seconds() / 60, 2)
    downtime_last_day = round(downtime_last_day.total_seconds() / 3600, 2)
    downtime_last_week = round(downtime_last_week.total_seconds() / 3600, 2)

    return [uptime_last_hour,downtime_last_hour , uptime_last_day, downtime_last_day , uptime_last_week , downtime_last_week]


if __name__ == '__main__':
    app.run()