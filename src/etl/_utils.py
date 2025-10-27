import sys
import logging
from uuid import uuid4


class StatxoLogger(logging.Logger):
    def __init__(self, name, level = 0):
        super().__init__(name, level)
        self.handlers.append(sys.stdin)



logger = StatxoLogger(name="statxo-logger")

def generate_random_id(length: int=6):
    if length < 0 or length > 16:
        length = 16
    
    return uuid4().hex[:length]


def get_root_metaclass(obj_or_class):
    """Get the root metaclass by walking up the type hierarchy"""
    cls = obj_or_class if isinstance(obj_or_class, type) else type(obj_or_class)
    
    # Walk up the metaclass chain until we hit 'type'
    current = type(cls)
    while type(current) is not type:
        current = type(current)
    
    return current

def have_same_root_metaclass(obj1, obj2):
    """Check if two objects' classes share the same root metaclass"""
    return get_root_metaclass(obj1) is get_root_metaclass(obj2)


def are_metaclasses_compatible(cls1, cls2):
    """Check if two classes can be inherited together (no metaclass conflict)"""
    meta1 = type(cls1)
    meta2 = type(cls2)
    
    # They're compatible if one is a subclass of the other
    return issubclass(meta1, meta2) or issubclass(meta2, meta1)



import re
import json


def flatten_dictionary(unflattened_dict: dict, basekey=None) -> dict:
    '''Flattens a dictionary whose values may be list or dictionaries themselves'''

    if basekey: 
        unflattened_dict = {f"{basekey}_{key}": value for key, value in unflattened_dict.items()}

    curr_dict = unflattened_dict.copy()
        
    for key, value in unflattened_dict.items():
        if type(value) is dict:
            # it is a dictionary itself
            # flatten it
            value = curr_dict.pop(key)
            curr_dict.update(flatten_dictionary(value, basekey=key))

        if type(value) is list:
            value = curr_dict.pop(key)
            for idx, v in enumerate(value):
                curr_dict.update(flatten_dictionary(v, basekey=f"{key}_{idx}"))

    return curr_dict


def convert_single_quote_to_double_quote(string: str) -> str:
    # string = string.replace("'", '"')
    # Replace single quotes around keys and string values with double quotes
    # 1. Replace key names: {'key': -> {"key":
    string = re.sub(r"(?<=\{|,)\s*'([^']+?)'\s*:", r'"\1":', string)
    
    # 2. Replace string values: : 'value' -> : "value"
    string = re.sub(r":\s*'([^']*?)'", r': "\1"', string)
    return string



STRING_PATTERN_DATETIME_MATCHING = r"datetime.datetime\(((\d{1,4}(, )?){3,})\)" # r"datetime.datetime\((\d+, \d+, \d+, \d+, \d+, \d+)\)"
'''Matches string like `datetime.datetime(2024, 04, 11, 12, 23, 45)`'''


def convert_dict_string_jsonable(dict_str: str) -> str:
    '''Converts the dictionary string to the jsonable string'''
    
    # first ' -> "
    dict_str = convert_single_quote_to_double_quote(dict_str)
    
    # search for the pattern in the dictionary
    string_matches = re.findall(
        pattern=re.compile(STRING_PATTERN_DATETIME_MATCHING), 
        string=dict_str
    )
    
    # substituting the different instances one by one
    for datetime_string in string_matches:
        # for each string containing datetime pattern
        # get the substitute
        sub = _datetime_to_jsonable_string_(datetime_string[0])

        # substitute
        dict_str = dict_str.replace(
            f"datetime.datetime({datetime_string[0]})", 
            f'"{sub}"', 
        )

    # return after all the substitutions
    return dict_str


DATETIME_STRING_FORMAT_JSON = "%Y-%m-%d %H:%M:%S"
'''To convert `datetime.datetime(2024, 04, 11, 12, 23, 45)` -> `2024-04-11 12:23:45`'''


def _datetime_to_jsonable_string_(datetime_string: str) -> str:
    import datetime
    date_time = datetime.datetime(
        *[int(num) for num in datetime_string.split(", ")]
    )
    return date_time.strftime(DATETIME_STRING_FORMAT_JSON)


def basic_dict_string_flattener(dict_str: str) -> dict[str, str | int | float]:
    # first convert single quotes -> double quotes
    x = convert_dict_string_jsonable(dict_str)
    # handle bool values
    x = x.replace("False", "false").replace("True", "true")
    # load into json
    x_dict = json.loads(x)
    # flatten it
    x_flat = flatten_dictionary(x_dict)

    return x_flat