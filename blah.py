import os,sys, time

def foo(x, y):
    result = x + y
    if x > 10:
     print("x is greater than 10")
    else:
      print("x is less than or equal to 10")
    return result

def bar():
    print "This is a syntax error"
    for i in range(10)
        print(i)

def unused_function():
    pass

def long_function_name_with_too_many_parameters(a, b, c, d, e, f, g, h, i, j, k, l, m):
    return a + b + c + d + e + f + g + h + i + j + k + l + m

class ExampleClass:
    def __init__(self, value):
        self.value = value
    def get_value(self):
        return self.value
    def set_value(self, value):
        self.value = value

foo(5, 7)
bar()
example = ExampleClass(10)
print(example.get_value())
example.set_value(20)
print(example.get_value())
