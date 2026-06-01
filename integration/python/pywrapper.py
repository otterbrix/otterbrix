try:
    from main_python import add
except ImportError:
    pass

def pure_add(a, b):
    return add(a, b)


