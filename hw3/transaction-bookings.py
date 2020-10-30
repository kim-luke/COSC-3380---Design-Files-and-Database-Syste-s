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

def yesTrans(server_var, fileName, threadNum):
    file = open(fileName, 'r')
    for i in file:
        string = i.split(',')

        #if the string before the first ',' is a digit
        if string[0].isdigit(): 
            # print(string[0] + " " + string[1])
            #check for availability
            #i.e. check if the value in flight_id column matches string[1]
            # and check if 

            sql = "drop table if exists temp;"
            sql += " create table temp( "
            sql += " flight_id int,"
            sql += " seats_booked int,"
            sql += " seats_available int );"
            server_var[0].execute(sql)
            server_var[1].commit()

            print("before execution")
            sql = " select * from flights where flight_id = " + string[1] + ";"
            server_var[0].execute(sql)
            fetch = server_var[0].fetchall()
            for index in fetch:
                print(str(index[0]) + " " + str(index[8]) + " " + str(index[9]))


            sql = "start transaction;"
            sql += " update flights"
            sql += " set seats_available = case"
            sql += " when seats_available > 0 and seats_available is not null and flight_id = " + string[1]
            sql += " then seats_available-1"
            sql += " end,"
            sql += " seats_booked = case "
            sql += " when flight_id = " + string[1] + " then seats_booked+1"
            sql += " end;"
            sql += " commit;"

            server_var[0].execute(sql)

            print("after execution")
            sql = " select * from flights where flight_id = " + string[1] + ";"
            server_var[0].execute(sql)
            fetch = server_var[0].fetchall()
            for index in fetch:
                print(str(index[0]) + " " + str(index[8]) + " " + str(index[9]))

def noTrans(server_var, fileName, threadNum):
    file = open(fileName, 'r')
    book_ref = 60000
    for i in file:
        string = i.split(',')
        if string[0].isdigit():
            
            # sql = "SELECT * FROM flights WHERE seats_available = 50 AND flight_id = " + string[1] + ";"
            sql = "SELECT flights.scheduled_departure FROM flights WHERE seats_available > 0 AND flight_id = " + string[1] + ";"

            server_var[0].execute(sql)
            fetch = server_var[0].fetchall()
            print(fetch)

            print(book_ref)
        book_ref += 1


        # 1. generate book_ref number, update bookings
        # 2. generate ticket_num, insert new record in ticket and ticket_flights IF available seats in specified flight AND date
        # 3. update seats_booked and seats_available in flights (simple)
        # 4. IF no available seats, ONLY book_ref generated and bookings updated

def updateDB(server_var, inputList):
    if inputList[1] == 'n':
        noTrans(server_var, inputList[0], inputList[2])
    #if inputList[1] == 'y':
        #yesTrans(server_var, inputList[0], inputList[2]) #(fileName, # of threads)
        """
            1. grab the passenger id
            2. grab the flights id
            3. check if there is a seat available in the flights that corresponds to flights_id
                3.1 open [flights table] 
                3.2 check if a seat is available
                3.3 
        """
    #if inputList[1] == 'n':
        #noTrans(server_var, inputList[0], inputList[2])
        

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
    # print(inputFileName + " " + inputYesNo + " " + inputThreadNum)
    inputs.append(inputThreadNum)

    returned_server_var = connectWithDB()
    updateDB(returned_server_var, inputs)
    

if __name__ == "__main__":
    main(sys.argv[1])
    # callThread()