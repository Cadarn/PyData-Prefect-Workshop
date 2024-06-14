# Example of processing some data


def add(a, b):
    return a + b 


def square_num(num):
    return num ** 2


def add_and_square(a:int = 2, b:int = 3):
    add_result = add(a, b)
    square_result = square_num(add_result)
    print(f"({a} + {b}) squared = {square_result}")

if __name__ == "__main__":
    add_and_square(4, 8)
