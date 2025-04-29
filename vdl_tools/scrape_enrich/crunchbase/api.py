from typing import Callable


def __predicate(field, operator, values=list()):
    return {
        "type": "predicate",
        "field_id": field,
        "operator_id": operator,
        "values": values,
    }


def blank(field):
    return __predicate(field, "blank")


def eq(field, value):
    return __predicate(field, "eq", [value])


def not_eq(field, value):
    return __predicate(field, "not_eq", [value])


def gt(field, value):
    return __predicate(field, "gt", [value])


def gte(field, value):
    return __predicate(field, "gte", [value])


def lt(field, value):
    return __predicate(field, "lt", [value])


def lte(field, value):
    return __predicate(field, "lte", [value])


def starts(field, value):
    return __predicate(field, "starts", [value])


def contains(field, values):
    return __predicate(field, "contains", values)


def between(field, value1, value2):
    return __predicate(field, "between", [value1, value2])


def includes(field, values):
    return __predicate(field, "includes", values)


def not_includes(field, values):
    return __predicate(field, "not_includes", values)


def domain_eq(field, values):
    return __predicate(field, "domain_eq", values)


def domain_includes(field, values):
    return __predicate(field, "domain_includes", values)


def print_progress_bar(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()


def api_query_factory(url, default_fields, limit=None) -> Callable:
    import requests
    import pandas as pd

    def api_query(fields=default_fields, filters=list(), limit=limit):
        next_uuid = None
        results = []

        while True:
            next = {} if next_uuid is None else {"after_id": next_uuid}
            data = requests.post(
                url,
                json={"field_ids": fields, "query": filters, "limit": 1000, **next},
            )

            res_json = data.json()

            if 'entities' not in res_json:
                print(res_json)
                break

            org_data = [
                {**item["properties"], "uuid": item["uuid"]}
                for item in res_json["entities"]
            ]

            results.extend(org_data)

            if len(res_json["entities"]) == 0:
                break

            print_progress_bar(len(results), res_json["count"], suffix=f"({len(results)} / {res_json['count']})")
            next_uuid = results[-1]["uuid"]
            if limit and len(results) >= limit:
                break

        return pd.DataFrame.from_dict(results)

    return api_query
