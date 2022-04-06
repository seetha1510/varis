# -*- coding: utf-8 -*-
"""
Created on Tue Apr  5 17:07:42 2022

@author: Owner
"""

import time
import multiprocessing 
from itertools import repeat
#import sys
#import math

def basic_func(x): # A simple function to see if x is even or odd.
    #print("Test")
    if x == 0:
        return 'zero'
    elif x%2 == 0:
        return 'even'
    else:
        return 'odd'

def multiprocessing_func(x, a, b): # squares x and then sees if the square is even or odd
    y = x*a + b
    #c = a + 1
    time.sleep(1)
    return y
    #print('{} squared results in a/an {} number'.format(x, basic_func(y)))

def print_cube(num):
    """
    function to print cube of given num
    """
    print("Cube: {}".format(num * num * num))
    #sys.stdout.flush()
  
def print_square(num):
    """
    function to print square of given num
    """
    print("Square: {}".format(num * num))
    #sys.stdout.flush()
    
def example():
    # creating processes
    p1 = multiprocessing.Process(target=print_square, args=(10, ))
    p2 = multiprocessing.Process(target=print_cube, args=(10, ))
  
    # starting process 1
    p1.start()
    # starting process 2
    p2.start()
  
    # wait until process 1 is finished
    p1.join()
    # wait until process 2 is finished
    p2.join()
  
    # both processes finished
    print("Done!")

def proc():
    processes = []
    for i in range(0,10):
        """
        if i%10 == 0:
            #print('{}%'.format(i/1000))
            sys.stdout.write("Completion: %f \r" % (i))
            sys.stdout.flush()
        """
        
        #multiprocessing_func(i)
        p = multiprocessing.Process(target = multiprocessing_func(i))
        #print(i)
        processes.append(p)
        p.start()
    
    j = 0.0
    for process in processes:
        #sys.stdout.write("\rCompletion: %d%%" %(math.floor(j))) # very succesful
        #print("Completion: %d%%"%j, end='\r', flush=True) # not as succesful
        #sys.stdout.write("\r%d"% j) # very succesful
        #sys.stdout.flush() #seems to break it
        process.join()
        j = j+1

 
def pol():
    pool = multiprocessing.Pool()
    #result = pool.map(multiprocessing_func, range(0,100))
    result = pool.starmap(multiprocessing_func, zip(range(0,10), repeat(10), repeat(5)))
    pool.close()
    
    for i in result:
        print(i)
 
if __name__ == '__main__':
    pstart = time.time()
    pol()
    pend = time.time()
    
    """
    sstart = time.time()
    for i in range(0,100):
        multiprocessing_func(i)
    send = time.time()
    """
    
    print("Multi took: %f seconds"%(pend-pstart))
    #print("Serial took: %f seconds"%(send-sstart))
    #proc()
    #pol()
    #multiprocessing_func(10)
    #example()
    
    #print('\nThat took {} seconds'.format(time.time() - starttime))