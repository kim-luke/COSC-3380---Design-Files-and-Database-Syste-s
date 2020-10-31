import psycopg2, sys
import logging
import threading
import datetime

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

def addZero(singleDigit):
    toReturn = '0' + str(singleDigit)
    return toReturn

def yesTrans(server_var, fileName, threadNum):
    file = open(fileName, 'r')
    index = 0
    y_m_d = [2000, 0, 0]
    m_d_str = ["", ""]
    for i in file:

        """
            y_m_d is a array with [year, month, day]
            m_d_str is a array with [month, day] both data is in the data type string
            
            if month and day is a single digit => then add a '0' in from of it
        """
        if y_m_d[0] > 2020:
                 y_m_d[0]= 2000
        if y_m_d[1] > 12:
            y_m_d[1]= 0
        if y_m_d[2] > 31:
            y_m_d[2] = 0
        
        if y_m_d[1] < 10:
            m_d_str[0] = addZero(y_m_d[1])
        if y_m_d[2] < 10:
            m_d_str[1] = addZero(y_m_d[2])
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
            for index_ in fetch:
                print(str(index_[0]) + " " + str(index_[8]) + " " + str(index_[9])) 

            #decrements seats_available by 1 and increments seats_booked by 1 if the flight_id matches
            sql = "start transaction;"
            sql += " update flights"
            sql += " set seats_available = case"
            sql += " when seats_available > 0 and flight_id = " + string[1]
            sql += " then seats_available-1 else seats_available"
            sql += " end,"
            sql += " seats_booked = case "
            sql += " when flight_id = " + string[1] +  " then seats_booked+1 else seats_booked"
            sql += " end;"
                       
            
            sql = " rollback;"
            server_var[0].execute(sql)

            #generate a book_ref and update bookings table (book_ref, book_date, total_amount)
            
            

            sql = "start transaction;"
            sql += " insert into bookings"
            sql += " values(" + str(index) + ", TIMESTAMP '" + str(year_var) + "-" + str(month_var) + "-" + str(day_var) + " 00:00:00-05'" + ", 127000);"
            
            server_var[0].execute(sql)

            sql = " select * from bookings;"
       
            print("after execution")
            server_var[0].execute(sql)
            fetch = server_var[0].fetchall()
            for index_ in fetch:
                print(str(index_[0]) + " " + str(index_[1]) + " " + str(index_[2])) 
            
            sql = " rollback;"
            server_var[0].execute(sql)
            index = index+1
            
            year_var = year_var+1
            month_var = month_var+1
            day_var = day_var+1
            

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
        noTrans(server_var, inputList[0], inputList[2]) #(fileName, # of threads)
    if inputList[1] == 'y':
        yesTrans(server_var, inputList[0], inputList[2]) #(fileName, # of threads)

    # sql = "delete from bookings; commit;"    
    # sql = "select * from bookings;"
    # server_var[0].execute(sql)
    # print(server_var[0].fetchall())

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
    # print(inputFileName + " " + inputYesNo + " " + inputThreadNum)
    inputs.append(inputThreadNum)

    returned_server_var = connectWithDB()
    updateDB(returned_server_var, inputs)
    

if __name__ == "__main__":
    main(sys.argv[1])
    # callThread()