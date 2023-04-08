from bson import SON
from datetime import datetime 
from dateutil.relativedelta import relativedelta

from motor.motor_asyncio import AsyncIOMotorDatabase

async def aggregator(db: AsyncIOMotorDatabase, collection: str, dt_from: datetime, dt_upto: datetime, group_type: str):
    dt = ""
    bound_upto = ""
    hour = 0
    day = 1
    if group_type == "day":
        dt = "dayOfYear"
        bound_upto = dt_upto + relativedelta(days=1)
        day = {"$dayOfMonth": "$f"}
    elif group_type == "hour":
        dt = group_type
        bound_upto = dt_upto + relativedelta(hours=1)
        hour = {"$hour": "$f"}
        print(bound_upto)
    elif group_type == "month":
        dt = group_type
        bound_upto = dt_upto + relativedelta(months=1)
    else:
        return {}

    command = SON([('aggregate', f'{collection}'),
    ('pipeline', [
    {
        "$match": {
            "$and" : [
                {"dt": {"$gte": dt_from}},
                {"dt": {"$lte": dt_upto}}
            ]
        }
    },
    {
        "$facet": {
            "data": [
                {
                    "$group": {
                        "_id": {f"${dt}": "$dt"}, 
                        "values": {"$sum": "$value"},
                        "f": {"$min": "$dt"}
                    }
                },
                {"$project": {"date": { "$dateFromParts": {"year": {"$year": "$f"}, "month": {"$month": "$f"}, "day": day, "hour": hour}} , "values": 1}}
            ]
        }
    },
    {
        "$addFields": {
            "dates": {
                "$map": {
                    "input": {"$range": [0, {"$dateDiff": {"startDate": dt_from, "endDate": bound_upto, "unit": group_type}}]},
                    "in": {
                        "date": {
                            "$dateAdd": {
                                "startDate": dt_from,
                                "unit": group_type,
                                "amount": "$$this"
                            }
                        },
                        "values": []
                    }
                }
            }
        }
    },
    {"$project": {"data": {"$concatArrays": ["$data", "$dates"]}}},
    {"$unwind": "$data"},
    {"$group": {"_id": "$data.date", "data": {"$sum": "$data.values"}}},
    {
        "$project": {
        "_id": 0,
        "label": {"$dateToString":{"format": "%Y-%m-%dT%H:%M:%S","date": "$_id"}},
        "data": 1
        }
    },
    {"$sort": {"label": 1}},
    {
        "$group": {
        "_id": None,
        "dataset": {"$push": "$data"},
        "labels": {"$push": "$label"}
        }
    },
    {
        "$project": {"_id": 0}
    }
    ]),
    ('explain', False)
    ])

    data = await db.command(command)
    
    return data["cursor"]["firstBatch"][0]