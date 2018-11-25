from flask import Flask, request, jsonify
from flask_restful import reqparse, abort, Resource, Api
# from sqlalchemy import create_engine
# from json import dumps


import automation_for_request

DEBUG = True
app = Flask(__name__)
api = Api(app)


class RESTService(Resource):
    def post(self):
        json_data = request.get_json(force=True)
        subject = json_data['subject']
        select_year = json_data["year"]
        select_month = json_data["month"]
        select_day = json_data['day']
        start_hh = json_data['start_hh']
        start_mm = json_data['start_mm']
        end_hh = json_data['end_hh']
        end_mm = json_data['end_mm']
        time_zone = json_data['timezone']

        params = {'subject': subject, 'year': select_year, 'month': select_month, 'day': select_day, 'start_hh':start_hh,
                  'start_mm':start_mm, 'end_hh': end_hh, 'end_mm': end_mm, 'timezone': time_zone}

        # params = [subject, select_year, select_month, select_day, start_hh, start_mm, end_hh, end_mm, time_zone]
        print params

        automation_for_request.Automation().generate_result(params)
        return jsonify(s=subject, y=select_year, d=select_day, sthh=start_hh, stmm=start_mm, enhh=end_hh, enmm=end_mm, t=time_zone)


api.add_resource(RESTService, '/app')

if __name__ == '__main__':
    app.run(port='5002')