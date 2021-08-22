# (C) 2021 GoodData Corporation
import re


def _split_camel_case(val: str):
    return re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", val)


def _db_identifier_to_lc_words(val: str):
    """
    Splits database identifier into lowercase words; the identifier may be snake case or camel case.

    Similar to the string split method, if this function cannot find any words, it will return list with one element:
    the input value to this function.

    :param val: db identifier to get words from
    :return: list of lower cases words; if no words safely identified then the list has one element: the `val`
    """
    if "_" in val:
        words = val.split("_")
    else:
        words = _split_camel_case(val)

        if len(words) == 0:
            words = [val]

    return [w.lower() for w in words]
