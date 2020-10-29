import psycopg2, sys
import logging
import threading
import time

def connectWithDB():
    with open('password.txt') as f:
        lines = [line.rstrip() for line in f]

    username = lines[0]
    pg_password = lines[1]

    conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
    cursor = conn.cursor()

    server_var = [cursor, conn]

    return server_var

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

def callThread():
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    threads = list()
    for index in range(3):
        logging.info("Main    : create and start thread %d.", index)
        x = threading.Thread(target=thread_function, args=( index,))
        threads.append(x)
        x.start()

    for index, thread in enumerate(threads):
        logging.info("Main    : before joining thread %d.", index)
        thread.join()
        logging.info("Main    : thread %d done", index)

def yesTrans(fileName, threadNum):
    file = open(fileName, 'r')
    for i in file:
        string = i.split(',')

        #if the string before the first ',' is a digit
        if string[0].isdigit(): 
            print(string[0] + " " + string[1])
            #check for availability
            #i.e. check if the value in flight_id column matches string[1]
            # and check if 
            sql = "select * from bookings.flights;"

def updateDB(server_var, inputList):
    sql = "select * from bookings.flights;"
    server_var[0].execute(sql)
    fetch = server_var[0].fetchall()
    server_var[1].commit()

    if inputList[1] == 'y':
        yesTrans(inputList[0], inputList[2]) #(fileName, # of threads)
        """
            1. grab the passenger id
            2. grab the flights id
            3. check if there is a seat available in the flights that corresponds to flights_id
                3.1 open [flights table] 
                3.2 check if a seat is available
                3.3 
        """
        

def main(argv):
    inputs = []

    indexOfEq = argv.find('=')
    indexOfSemiFirst = argv.find(';')
    
    #file name
    inputFileName = argv[indexOfEq+1:indexOfSemiFirst]
    restOfLines = argv[indexOfSemiFirst+1: len(argv)]
    inputs.append(inputFileName)

    #input yes or no (for transaction)
    inputYesNo = restOfLines[restOfLines.find('=')+1: restOfLines.find(';')]
    restOfLines = restOfLines[restOfLines.find(';')+1: len(restOfLines)]
    inputs.append(inputYesNo)
    
    #input # of thread
    inputThreadNum = restOfLines[restOfLines.find('=')+1: len(restOfLines)]
    print(inputFileName + " " + inputYesNo + " " + inputThreadNum)
    inputs.append(inputThreadNum)

    returned_server_var = connectWithDB()
    updateDB(returned_server_var, inputs)
    

if __name__ == "__main__":
    main(sys.argv[1])
    # callThread()