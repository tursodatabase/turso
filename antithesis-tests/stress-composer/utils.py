import string
from antithesis.random import get_random, random_choice

def generate_random_identifier(type: str, num: int):
    return ''.join(type, '_', get_random() % num)

def generate_random_value(type: str):
    match type:
        case 'INTEGER':
            return str(get_random() % 100)
        case 'REAL':
            return '{:.2f}'.format(get_random() % 100 / 100.0)
        case 'TEXT':
            return f"'{''.join(random_choice(string.ascii_lowercase) for _ in range(5))}'"
        case 'BLOB':
            return f"x'{''.join(random_choice(string.ascii_lowercase) for _ in range(5)).encode().hex()}'"
        case 'NUMERIC':
            return str(get_random() % 100)
        case _:
            return NULL