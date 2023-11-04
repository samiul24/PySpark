#Python Build in Function
my_list=[1,2,3,4,5,6,7,8,9,10,11]
print('If odd then return same number, otherwise zero')
odd_list=list(map(lambda x:x if x%2!=0 else 0, my_list))
print(odd_list)

print('\nOnly odd number')
odd_list=list(filter(lambda x:x if x%2!=0 else 0, my_list))
print(odd_list)

#Write a function
def square(n):
    return n*n

print('\nSquare of list')
square_list=list(map(lambda x:square(x), my_list))
print(square_list)

#Find max number
print('\nMax number')
from functools import reduce
max_number=reduce(lambda x,y: x if x>y else y, my_list)
print(max_number)