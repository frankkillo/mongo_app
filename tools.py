from dateutil.parser import parse
from dateutil.parser import ParserError
import json
from json.decoder import JSONDecodeError

def query_validator(data):
    try:
        query = json.loads(data)
    except JSONDecodeError:
        return False
    dt_from = query.get("dt_from")
    dt_upto = query.get("dt_upto")
    group_type = query.get("group_type")
    if not dt_from or not dt_upto or not group_type:
        return False
    try:
        dt_from = parse(dt_from)
        dt_upto = parse(dt_upto)
    except ParserError:
        return False
    if dt_from > dt_upto:
        return False
    if group_type not in ["hour", "day", "month"]:
        return False
    
    return {"dt_from": dt_from, "dt_upto": dt_upto, "group_type": group_type}