# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 15:46:38 2020
Key-value data store which supports basic CRD Operations
@author: AJAYRAGAVAN M V
"""

#This is a input file on client side to access backend code to perform CRD Operations

import fwbackendmodule as fw
#fwbackendmodule which performs basic CRD Operations has been imported into this code as fw

print("Welcome to Key-Value Datastore")
print("Press 1 to Inititate single access datastore\nPress 2 to Inititate Multi access datastore\nPress any key to quit program")
ip=int(input()) #to get user choice
# If any key pressed other than 1 or 2, The program will end here
if(ip == 1):
    fw.initiatefn()  #to call the single process fn in the other code
elif(ip == 2):
    fw.threadfn()    #to call the threads fn in the other code to intiate mutli threads
