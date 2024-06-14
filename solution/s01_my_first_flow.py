from prefect import task, flow

@task(name="Addition operator")
def add(a, b):
    return a + b 

@task(name="Squaring function")
def square_num(num):
    return num ** 2

@flow(log_prints=True, name="Demo 1")
def add_and_square(a:int = 2, b:int = 3):
    add_result = add(a, b)
    square_result = square_num(add_result)
    print(f"({a} + {b}) squared = {square_result}")

if __name__ == "__main__":
    add_and_square(4, 8)
