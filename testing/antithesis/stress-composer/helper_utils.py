import string

from antithesis.random import get_random, random_choice


def generate_random_identifier(type: str, num: int):
    return "".join(type, "_", get_random() % num)


def generate_random_value(type_str):
    if type_str == "INTEGER":
        return str(get_random() % 100)
    elif type_str == "REAL":
        return "{:.2f}".format(get_random() % 100 / 100.0)
    elif type_str == "TEXT":
        return f"'{''.join(random_choice(string.ascii_lowercase) for _ in range(5))}'"
    elif type_str == "BLOB":
        return f"x'{''.join(random_choice(string.ascii_lowercase) for _ in range(5)).encode().hex()}'"
    elif type_str == "NUMERIC":
        return str(get_random() % 100)
    else:
        return "NULL"
