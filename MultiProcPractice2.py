# -*- coding: utf-8 -*-
"""
Created on Tue Apr  5 21:06:21 2022

@author: Owner
"""

import multiprocessing

def worker(num):
    """Returns the string of interest"""
    return "worker %d" % num

def main():
    pool = multiprocessing.Pool(4)
    results = pool.map(worker, range(10))

    pool.close()
    pool.join()

    for result in results:
        # prints the result string in the main process
        print(result)

if __name__ == '__main__':
    # Better protect your main function when you use multiprocessing
    main()