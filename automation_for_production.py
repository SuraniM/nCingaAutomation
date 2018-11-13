import json
import elasticsearch
from openpyxl import Workbook
from datetime import datetime
import pytz
import logging
import es_queries
from Tkinter import *
import tkFileDialog
import requests
import tkMessageBox
import pandas as pd
import tkFont
import os
import config


class Automation(Frame):

    def __init__(self, master=None):

        self.default_save_location = StringVar(value=os.getcwd())

        Frame.__init__(self, master)
        self.pack()

        frame = Frame(master, width=500, height=500, bd=1, bg='gray')
        frame.pack(expand=YES, fill=BOTH)

        Label(frame, text="Log Comparison", font=("Helvetica", 16), bg = "Green").pack(side=TOP)

        self.subject = StringVar(frame)
        self.subject.set("Select Subject")
        self.subject_list = OptionMenu(frame, self.subject, "ant-aal-aepz", "meg-sgt", "ncsant-aal-aepz").place(x=10, y=40)

        self.timezone_obj = StringVar(frame)
        self.timezone_obj.set("Asia/Dhaka")
        self.timezone_list = OptionMenu(frame, self.timezone_obj, "Asia/Dhaka", "Asia/Colombo").place(x=10, y=190)

        Label(frame, text="Date (YY/MM/DD): " ,font=helv10).place(x=10, y=90)
        Label(frame, text=" / ", font=helv10).place(x=180, y=90)
        Label(frame, text=" / ", font=helv10).place(x=220, y=90)
        Label(frame, text="Start Time (HH/MM): ", font=helv10).place(x=10, y=125)
        Label(frame, text="End Time (HH/MM): ", font=helv10).place(x=10, y=150)
        Label(frame, text=" : ", font=helv10).place(x=170, y=125)
        Label(frame, text=" : ", font=helv10).place(x=170, y=150)

        self.year = Entry(frame, width = 4, font=helv10)
        self.year.place(x=150, y=90)
        self.month = Entry(frame, width = 2, font=helv10)
        self.month.place(x=200, y=90)
        self.day = Entry(frame, width = 2, font=helv10)
        self.day.place(x=240, y=90)
        self.start_hour = Entry(frame, width = 2, font=helv10)
        self.start_hour.place(x=150, y=125)
        self.start_minute = Entry(frame, width = 2, font=helv10)
        self.start_minute.place(x=190, y=125)
        self.end_hour = Entry(frame, width = 2, font=helv10)
        self.end_hour.place(x=150, y=150)
        self.end_minute = Entry(frame, width = 2, font=helv10)
        self.end_minute.place(x=190, y=150)

        self.generate_csv = Button(frame, text="Generate CSV File", font=helv12, bg="black", fg="white", bd=5,
                                 command=self.generate_result).place( x=25, y=240)
        self.button = Button(frame, text="Quit", font=helv12, bg="brown", fg="white", bd=5, command=frame.quit).place( x=210, y=240)

        # Label(frame, text="Save location: ").pack(side=LEFT)
        # self.locationLabel = Label(frame, textvariable=self.default_save_location).pack(side=LEFT)
        # Button(frame, text="...", command=self.change_directory).pack(side=LEFT)

    def change_directory(self):
        self.default_save_location.set(tkFileDialog.askdirectory())

    def generate_result(self):

        subject = self.subject
        subject_name = subject.get()

        elog_date = self.year.get() + "." + self.month.get() + "." + self.day.get()
        ops_date = self.year.get() + "-" + self.month.get() + "-" + self.day.get()
        print self.timezone_obj
        tz = pytz.timezone(self.timezone_obj.get())
        starting_time = (tz.localize(datetime(int(self.year.get()), int(self.month.get()), int(self.day.get()),
                                              int(self.start_hour.get()), int(self.start_minute.get())), is_dst=None) -
                         datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

        ending_time = (tz.localize(datetime(int(self.year.get()), int(self.month.get()), int(self.day.get()),
                                            int(self.end_hour.get()), int(self.end_minute.get())), is_dst=None) -
                       datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
        print starting_time, ending_time

        if subject_name == 'ncsant-aal-aepz':
            es_qa = self.connect_elasticsearch_qa()
            logging.basicConfig(level=logging.ERROR)

            if es_qa is not None:

                core_search_object = es_queries.send_queries(starting_time, ending_time)[0]
                core_search_object = json.dumps(core_search_object)
                cate_search_object = es_queries.send_queries(starting_time, ending_time)[1]
                cate_search_object = json.dumps(cate_search_object)
                elog_search_object = es_queries.send_queries(starting_time, ending_time)[2]
                elog_search_object = json.dumps(elog_search_object)
                flink_search_object = es_queries.send_queries(starting_time, ending_time)[3]
                flink_search_object = json.dumps(flink_search_object)

                core_json_data = self.search(es_qa, subject.get() + "-ops-core-" + ops_date, core_search_object)
                cate_json_data = self.search(es_qa, subject.get() + "-ops-cate-" + ops_date, cate_search_object)
                elog_json_data = self.search(es_qa, subject.get() + "-activity-elog-" + elog_date, elog_search_object)
                flink_json_data = self.search(es_qa, subject.get() + "-flink-log-" + ops_date, flink_search_object)

            self.download_csv(core_json_data, cate_json_data, elog_json_data, flink_json_data)

        else:
            es_elog = self.connect_elasticsearch_elog()
            logging.basicConfig(level=logging.ERROR)

            if es_elog is not None:

                elog_search_object = es_queries.send_queries(starting_time, ending_time)[2]
                elog_search_object = json.dumps(elog_search_object)
                flink_search_object = es_queries.send_queries(starting_time, ending_time)[3]
                flink_search_object = json.dumps(flink_search_object)

                elog_json_data = self.search(es_elog, subject.get() + "-activity-elog-" + elog_date, elog_search_object)
                try:
                    flink_json_data = self.search(es_elog, subject.get() + "-flink-log-" + ops_date, flink_search_object)
                except:
                    flink_json_data = {}

            es_ops = self.connect_elasticsearch_ops()
            logging.basicConfig(level=logging.ERROR)

            if es_ops is not None:

                core_search_object = es_queries.send_queries(starting_time, ending_time)[0]
                core_search_object = json.dumps(core_search_object)
                cate_search_object = es_queries.send_queries(starting_time, ending_time)[1]
                cate_search_object = json.dumps(cate_search_object)

                core_json_data = self.search(es_ops, subject.get() + "-ops-core-" + ops_date, core_search_object)
                cate_json_data = self.search(es_ops, subject.get() + "-ops-cate-" + ops_date, cate_search_object)

            self.download_csv(core_json_data, cate_json_data, elog_json_data, flink_json_data)

    def get_data_flink_elog(self, json):
        main_arr = []
        print "json:" % json

        ftt, produced_mins, checkin, defect, defectives, defectives_r, rectified, reworked, bctx_change, member, reject, \
        reject_r, reject_rr, aql_pass, aql_fail, aql_defect = None, None, None, None, None, None, None, None, None, None, \
                                                              None, None, None, None, None, None
        print len(json["aggregations"]["ev"]["buckets"])
        for i in range(len(json["aggregations"]["ev"]["buckets"])):
            line_by_arr = []
            for j in range(len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"])):
                key_val = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["key"]
                if key_val == "ftt":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        ftt = \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        ftt = \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "produced_mins":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        produced_mins = \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        produced_mins = \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "checkin":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        checkin = \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        checkin = \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                            json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "defect":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        defect = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        defect = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "defectives":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        defectives = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        defectives = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "rectified":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        rectified = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        rectified = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "reworked":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        reworked = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        reworked = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "bctx_change":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        bctx_change = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        bctx_change = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "member":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        member = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        member = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "defectives_r":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        defectives_r = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        defectives_r = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "reject":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        reject = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        reject = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "reject_r":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        reject_r = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        reject_r = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == " reject_rr":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        reject_rr = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        reject_rr = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == " aql_pass":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        aql_pass = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        aql_pass = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "aql_fail":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        aql_fail = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        aql_fail = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

                if key_val == "aql_defect":
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 1:
                        aql_defect = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"]
                    if len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"]) == 2:
                        aql_defect = \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][0]["vl"]["value"] \
                        + json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["key"] * \
                        json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["cv"]["buckets"][1]["vl"]["value"]

            line_by_arr.append(json["aggregations"]["ev"]["buckets"][i]["key"])
            line_by_arr.append(ftt)
            line_by_arr.append(produced_mins)
            line_by_arr.append(checkin)
            line_by_arr.append(defect)
            line_by_arr.append(defectives)
            line_by_arr.append(defectives_r)
            line_by_arr.append(rectified)
            line_by_arr.append(reworked)
            line_by_arr.append(reject)
            line_by_arr.append(reject_r)
            line_by_arr.append(reject_rr)
            line_by_arr.append(member)
            line_by_arr.append(bctx_change)
            line_by_arr.append(aql_defect)
            line_by_arr.append(aql_pass)
            line_by_arr.append(aql_fail)

            main_arr.append(line_by_arr)
            ftt, produced_mins, checkin, defect, defectives, defectives_r, rectified, reworked, bctx_change, member, \
            reject, reject_r, reject_rr,aql_pass, aql_fail, aql_defect = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
        return main_arr

    def get_data_core_cate(self, json):
        main_arr = []

        ftt, produced_mins, checkin, defect, defectives, defectives_r, rectified, reworked, bctx_change, member, reject, reject_r, reject_rr, aql_pass, aql_fail, aql_defect = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
        for i in range(len(json["aggregations"]["ev"]["buckets"])):
            line_by_arr = []
            for j in range(len(json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"])):
                key_val = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["key"]
                if key_val == "ftt":
                    ftt = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "produced_mins":
                    produced_mins = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "checkin":
                    checkin = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "defect":
                    defect = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "defectives":
                    defectives = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "rectified":
                    rectified = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "reworked":
                    reworked = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "bctx_change":
                    bctx_change = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "member":
                    member = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "defectives_r":
                    defectives_r = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "reject":
                    reject = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "reject_r":
                    reject_r = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "reject_rr":
                    reject_rr = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "aql_pass":
                    aql_pass = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "aql_fail":
                    aql_fail = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]
                if key_val == "aql_defect":
                    aql_defect = json["aggregations"]["ev"]["buckets"][i]["ev"]["buckets"][j]["_sum"]["value"]

            line_by_arr.append(json["aggregations"]["ev"]["buckets"][i]["key"])
            line_by_arr.append(ftt)
            line_by_arr.append(produced_mins)
            line_by_arr.append(checkin)
            line_by_arr.append(defect)
            line_by_arr.append(defectives)
            line_by_arr.append(defectives_r)
            line_by_arr.append(rectified)
            line_by_arr.append(reworked)
            line_by_arr.append(reject)
            line_by_arr.append(reject_r)
            line_by_arr.append(reject_rr)
            line_by_arr.append(member)
            line_by_arr.append(bctx_change)
            line_by_arr.append(aql_defect)
            line_by_arr.append(aql_pass)
            line_by_arr.append(aql_fail)

            main_arr.append(line_by_arr)
            ftt, produced_mins, checkin, defect, defectives, defectives_r, rectified, reworked, bctx_change, member, reject, reject_r, reject_rr,aql_pass, aql_fail, aql_defect = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None

        return main_arr


    def download_csv(self,core_json_data, cate_json_data, elog_json_data, flink_json_data):

        core_result = self.get_data_core_cate(json.loads(core_json_data))
        cate_result = self.get_data_core_cate(json.loads(cate_json_data))
        elog_result = self.get_data_flink_elog(json.loads(elog_json_data))
        try:
            flink_result = self.get_data_flink_elog(json.loads(flink_json_data))
        except:
            flink_result = {}

        wb = Workbook()
        ws = wb.active
        df = pd.DataFrame(columns= ["line", "source", "Actual", "ftt", "produced_mins", "checkin", "defect", "defectives","defectives_r", "rectified",
                   "reworked", "reject", "reject_r", "reject_rr", "member", "bctx_change", "aql_defect", "aql_pass","aql_fail"])
        for i in range(len(core_result)):
            actual = sum(filter(None, [core_result[i][1],core_result[i][8]]))
            df = df.append({"line":core_result[i][0], "source":"core", "Actual":actual, "ftt":core_result[i][1], "produced_mins":core_result[i][2],
                            "checkin":core_result[i][3], "defect":core_result[i][4], "defectives":core_result[i][5],"defectives_r":core_result[i][6],
                            "rectified":core_result[i][7],"reworked":core_result[i][8], "reject":core_result[i][9], "reject_r":core_result[i][10],
                            "reject_rr":core_result[i][11], "member":core_result[i][12], "bctx_change":core_result[i][13],
                            "aql_defect":core_result[i][14], "aql_pass":core_result[i][15] ,"aql_fail":core_result[i][16]}, ignore_index=True)
        for i in range(len(cate_result)):
            actual = "NA"
            df = df.append({"line": cate_result[i][0], "source": "cate", "Actual": actual, "ftt": cate_result[i][1],
                            "produced_mins": cate_result[i][2],
                            "checkin": cate_result[i][3], "defect": cate_result[i][4], "defectives": cate_result[i][5],
                            "defectives_r": cate_result[i][6],
                            "rectified": cate_result[i][7], "reworked": cate_result[i][8], "reject": cate_result[i][9],
                            "reject_r": cate_result[i][10],
                            "reject_rr": cate_result[i][11], "member": cate_result[i][12], "bctx_change": cate_result[i][13],
                            "aql_defect": cate_result[i][14], "aql_pass": cate_result[i][15], "aql_fail": cate_result[i][16]},
                           ignore_index=True)

        for i in range(len(elog_result)):
            actual = sum(filter(None, [elog_result[i][1], elog_result[i][8]]))
            df = df.append({"line": elog_result[i][0], "source": "elog", "Actual": actual, "ftt": elog_result[i][1],
                            "produced_mins": elog_result[i][2],
                            "checkin": elog_result[i][3], "defect": elog_result[i][4], "defectives": elog_result[i][5],
                            "defectives_r": elog_result[i][6],
                            "rectified": elog_result[i][7], "reworked": elog_result[i][8], "reject": elog_result[i][9],
                            "reject_r": elog_result[i][10],
                            "reject_rr": elog_result[i][11], "member": elog_result[i][12], "bctx_change": elog_result[i][13],
                            "aql_defect": elog_result[i][14], "aql_pass": elog_result[i][15], "aql_fail": elog_result[i][16]},
                           ignore_index=True)

        for i in range(len(flink_result)):
            actual = sum(filter(None, [flink_result[i][1], flink_result[i][8]]))
            df = df.append({"line": flink_result[i][0], "source": "flink", "Actual": actual, "ftt": flink_result[i][1],
                            "produced_mins": flink_result[i][2],
                            "checkin": flink_result[i][3], "defect": flink_result[i][4], "defectives": flink_result[i][5],
                            "defectives_r": flink_result[i][6],
                            "rectified": flink_result[i][7], "reworked": flink_result[i][8], "reject": flink_result[i][9],
                            "reject_r": flink_result[i][10],
                            "reject_rr": flink_result[i][11], "member": flink_result[i][12], "bctx_change": flink_result[i][13],
                            "aql_defect": flink_result[i][14], "aql_pass": flink_result[i][15], "aql_fail": flink_result[i][16]},
                           ignore_index=True)

        df = df.sort_values(by=['line','source'])

        file_name = "log_comparison.csv"

        save_to = tkFileDialog.asksaveasfilename(**dict(
            defaultextension=".csv",
            filetypes=[('CSV', ".csv")],
            initialdir=self.default_save_location.get(),
            initialfile=file_name,
            title='Save as'))

        if save_to:
            df.to_csv(save_to, index=False)
            tkMessageBox.showinfo(message='Excel saved successfully.')
        else:
            return

    def search(self, es_object, index_name, search):
        res = es_object.search(index=index_name, doc_type = None, body=search)
        result = json.dumps(res)
        return result

    def connect_elasticsearch_elog(self):
        _es = None
        _es = elasticsearch.Elasticsearch([{'host': config.DATABASE_CONFIG_ELOG['host'], 'port': config.DATABASE_CONFIG_ELOG['port']}])
        if _es.ping():
            print('Elog Connected')
        else:
            tkMessageBox.showerror(message='Elog could not connect!')

        return _es

    def connect_elasticsearch_ops(self):
        _es = None
        _es = elasticsearch.Elasticsearch([{'host': config.DATABASE_CONFIG_OPS['host'], 'port': config.DATABASE_CONFIG_OPS['port']}])
        if _es.ping():
            print('Ops Connected')
        else:
            tkMessageBox.showerror(message='Ops could not connect!')
        return _es

    def connect_elasticsearch_qa(self):
        _es = None
        _es = elasticsearch.Elasticsearch([{'host': config.QA_CONFIG['host'], 'port': config.QA_CONFIG['port']}])
        if _es.ping():
            print('QA Connected')
        else:
            tkMessageBox.showerror(message='QA could not connect!')
        return _es

# if __name__ == '__main__':

root = Tk()
helv12 = tkFont.Font(family='Helvetica', size=12, weight='bold')
helv10 = tkFont.Font(family='Helvetica', size=10)
app = Automation(master=root)
app.master.geometry("300x300+100+50")
app.master.title("Logs Comparison")
app.mainloop()
root.destroy()