from analytics.settings import  UTC, db


class DbHelper:
    def data(self, start_time, end_time, _type, skip, limit, store_id, store_category_id, platform):
        query = {"type": _type, "platform": platform}
        print("store_id---->", store_id)
        if store_id in ["", "0"]:
            pass
        else:
            query["store_id"] = store_id
        if store_category_id:
            query["store_category_id"] = store_category_id
        if start_time and end_time: query["create_ts"] = {"$gte": start_time, "$lte": end_time}
        # projections = {"create_date": 0,
        #                "start_date": 0,
        #                "end_date": 0,
        #                "_id": 0
        #                }
        print("query", query)
        projections = {"_id": 0}
        # print("projections", projections)
        export_data = db["analyticsExport"].find(query, projections).sort("create_ts", -1)
        count = int(export_data.count())
        return export_data.skip(skip).limit(limit), count
