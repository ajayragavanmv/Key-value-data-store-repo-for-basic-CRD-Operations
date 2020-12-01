# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 14:47:59 2020
Key-value data store which supports basic CRD Operations
@author: AJAYRAGAVAN M V
"""
"""
Objective given:-
To implement a key-value data store object to support basic CRD Operations
which has certain Functional and Non-Functional requirements
"""
import threading
import json

import time
import sys


dstore={} #dictionary (dstore) to store the data

"""
Dictionary is chosen to conveniently store and access data as key-value pairs
"""

#Print operation
def printfn():
    print(dstore)
    print('\n')

"""
Print operation is an extra function to check and print the values present in datastore

Note: As expired keys are not accessible by read and delete function,It is not permanently deleted
so while we create an
extra print function, even the expired keys will be printed.

So, to avoid this, we can provide user an option inside delete function also to delete the expired key.
"""
#Lock is being used to avoid two threads accessing data at the same time to avoid clashes between them
"""
Multiple process client requirments:-
1. More than one client process cannot be allowed to use the same file as a data store at any
given time.
2. A client process is allowed to access the data store using multiple threads, if it desires to.
The data store must therefore be thread-safe.
"""
def thread_task(lock):
    lock.acquire()     #If lock acquired, till it is released other thread cant access the data
    print("\nEntering New Thread Process")
    initiatefn()
    print("\nCurrent Thread Process exited")
    lock.release()    #Lock released allowing other thread to access data
    
"""
To ensure thread safety, and to prevent parallel thread from accesing datastore,
thread lock is being used
"""

def threadfn(): 
	#Lock creation process - Lock creation is done to ensure thread safety, to avoid two threads accessing a single dataspace at same time
	lock = threading.Lock() 
	thread1 = threading.Thread(target=thread_task, args=(lock,))
	thread2 = threading.Thread(target=thread_task, args=(lock,)) 
    #Till thread(n), we can create any number of threads required

	# To start the threads 
	thread1.start() 
	thread2.start() 

	# To join our newly created threads with our Main thread,else incosistency in output may arise
	thread1.join() 
	thread2.join() 

"""
Python is genrally single threaded, using threading module we can improve
performance and time taken for seperate processing

Here only two threads are taken for testing purpose.
"""

def initiatefn():  #Starting point of access of datastore
    oplist=['1.Create value','2.Read value','3.Delete the value','4.Print the datastore','5.Quit']
    i=1
    while(i):  #With respect to user choice,execute appropriate functions
        print("\nEnter the operation needed to be performed")
        print(*oplist, sep = "\n")  #Print the list menu, each menu operations printed in seperate line, to increase clarity in output console window
        j=int(input())
        if(j==1): #createfunction
            print("Enter key,value and timeout(Optional)")
            print("Note:- Enter the values seperated by space")
            ipcomb=input()
            ip=ipcomb.split(" ")
            lenip=len(ip)
            if(lenip == 3):
                c=int(ip[2])
                createfn(ip[0],ip[1],c)
            elif(lenip == 2):
                createfn(ip[0],ip[1])
            else:
                print("Entered data incorrect format")
        elif(j==2): #readfunction
            print("Enter key to read")
            ip=input()
            readfn(ip)
        elif(j==3): #deletefunction
            print("Enter key to delete")
            ip=input()
            deletefn(ip)
        elif(j==4): #printfunction
            printfn()
        elif(j==5): #Quit current execution
            i=0
        else:
            print("Enter the correct option from 1 - 5")
    
    print("Execution ended")

#Basic CRD Operations - Create,Read,Delete operations


#create operation
"""
Create operation requirements:-
1. A new key-value pair can be added to data store using create operation
2. The key is always a "String" with max 32 chars
3. The value is JSON(JavaScript Object Notation) Object with max 16Kb size
4. Duplicate value for existing key is not entertained, Return appropriate error statement
5. Every key should support Time - to - Live property during creation. ITs should be optional
"""

"""
tout-Timeout value. It is optional only.
"""
def createfn(key,val,tout=0):
    if key in dstore:
        print("Error: The entered key exists already") #Duplicate value creation's should be avoided
    else:
        if(key.isalpha()): #isalpha returns true only when whole strings consists of alphabets. alphanumerals are not allowed.
            dstoresize=sys.getsizeof(dstore)
            valsize=sys.getsizeof(val)
            keylen=len(key)
            if((dstoresize < (1024*1024*1024)) and (valsize <= (16*1024*1024))):
                """
                According to requirements, File size should not exceed 1GB and
                size of Value should be less than 16Kb
                
                1Gb can be represented as 1 e-9 bytes, which is pow(1024,3).
                The reason we express in bytes is sys.getsizeof(obj) returns size in bytes
                
                """
                if(keylen<=32):  #To limit key length less than 32 as mentioned in requirement
                    if(tout==0):
                        dstore[key]=[val,tout]
                        print("Value store operation Successful")
                    else:
                        dstore[key]=[val,time.time()+tout]
                        print("Value store operation Successful")
                else:
                    print("Error: The entered key contains more than 32 chars")
            else:
                print("Error: Memory Limit has excedded. Try Again")
        else:
            print("Error: Keyname Invalid. Alphanumeral values not entertained in keys. Try again")
            

#Read operation
"""
Read operation requirements:-
1. Read operation can be performed by providing the key, to receive value as response.
"""

def readfn(key):
    if key not in dstore:
        print("Error: Key doesn't Exist. Use create function to create a new key or Enter a valid key")
    else:
        tcheck=dstore[key]
        if tcheck[1] != 0:
            if time.time() < tcheck[1]:
                jsonop=json.dumps(dstore[key])     #python to json conversion acheived by importing json library and using it functions
                print(jsonop)                      #To read and print value as a json object
            else:
                print("Error: Time to Live has expired for the given key")
        else:
            jsonop=json.dumps(dstore[key])
            print(jsonop)                          #To read and print value as a json object
            
                
#Delete operation
"""
Delete operation requirements:-
1. Delete operation can be performed if key is provided
"""

def deletefn(key):
    if key not in dstore:
        print("Error: Key doesn't Exist. Enter a valid Key")
    else:
        tcheck=dstore[key]
        if tcheck[1] != 0:
            if time.time() < tcheck[1]:
                del dstore[key]
                print("Delete operation Successful")
            else:
                print("Error: Time to Live has expired for the given key")
        else:
            del dstore[key]
            print("Delete operation Successful")
