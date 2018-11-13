

def send_queries(starting_time, ending_time):
    st_time = int(round(starting_time * 1000, 0))
    end_time = int(round(ending_time * 1000, 0))

    core = {"size": 0,
            "query": {"bool": {"filter": [{"range": {"time_tags.datetime": {"gte": st_time, "lte": end_time}}}]}},
            "aggs": {"ev": {"terms": {"field": "subject", "size": 100, "order": {"_key": "asc"}},
                            "aggs": {"ev": {"terms": {"field": "attribute", "size": 100},
                                            "aggs": {"_sum": {"sum": {"field": "points.attrib_val"}}}}}}}}

    cate = {"size": 0,
            "query": {"bool": {"filter": [{"range": {"time_tags.datetime": {"gte": st_time, "lte": end_time}}}]}},
            "aggs": {"ev": {"terms": {"field": "subject", "size": 100, "order": {"_key": "asc"}},
                            "aggs": {"ev": {"terms": {"field": "attribute", "size": 100},
                                            "aggs": {"_sum": {"sum": {"field": "points.attrib_val"}}}}}}}}

    elog = {"size": 0 ,"query" :{"bool" :{"must" :[{"range" :{"w1.dt" :{"gte" : st_time ,"lte" : end_time}}}]}} ,"aggs" :
        {"ev" :{"terms" :{"field" :"a.sub.keyword" ,"order" :{"_key" :"asc"} ,"size" :100} ,"aggs" :
            {"ev" :{"terms" :{"field" :"w3.nm.keyword" ,"size" :100} ,"aggs" :
                {"cv" :{"terms" :{"field" :"w3.cv" ,"size" :10} ,"aggs" :{"vl" :{"sum" :{"field" :"w3.vl"}}}}}}}}}}

    flink = {"size":0,"query":{"bool":{"must":[{"range":{"w1_dt":{"gte": st_time,"lte": end_time}}}]}},
             "aggs":{"ev":{"terms":{"field":"a.sub.keyword","order":{"_key":"asc"},"size":100},"aggs":
                 {"ev":{"terms":{"field":"w3.nm.keyword","size":100},"aggs":{"cv":{"terms":{"field":"w3.cv","size":10},"aggs":
                     {"vl":{"sum":{"field":"w3.vl"}}}}}}}}}}

    return core, cate, elog, flink
