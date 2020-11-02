import psycopg2, sys
import logging
import threading
import datetime
import time

"""
    THINGS TO KEEP IN MIND
    1. book_date column in table "bookings" should be on or before the sheduled_departure date in the "flights" table
    2. count the number of succesful and unsuccessful transactions (confused)
        =>this should be simple for nonTrans()
            ->can run "select seats_available from flights where flight_id = same" => fetch => see if value is 0 or not [0=fail, >0=success]
        =>don't understand for yesTrans() since all the sql has to run once (????) 
            -> thought 
                --> before anything 
                --> run a sql to check if seats_available is 0 
                --> if it is then terminate the sql query
                --> does terminating return anything to python? psycopg2?
                --> links:
                    -- https://stackoverflow.com/questions/10377781/return-boolean-value-on-sql-select-statement 
                    -- https://stackoverflow.com/questions/2862080/sql-return-true-if-list-of-records-exists/2862117
    3. TRANSACTIONS SHOULD BE SPREAD BETWEEN THREADS AS EVENLY AS POSSIBLE (i think? whats are we outputting?)
        => think output is:
            Successful transaction: #
            Unsuccessful transaction: #
    4. no need to measure the time completion for the program
    TEST CASES
    1. normal cases 10-20 threads
    2. hard cases >= 100 threads and >= 10000 input lines   
"""

queryList = ""

def connectWithDB():
    with open('password.txt') as f:
        lines = [line.rstrip() for line in f]

    username = lines[0]
    pg_password = lines[1]

    conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
    cursor = conn.cursor()

    server_var = [cursor, conn]

    return server_var

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

#adds '0' infront of a single digit and returns it
def addZero(singleDigit):
    toReturn = '0' + str(singleDigit)
    return toReturn

def yesTransThread(server_var, book_ref, ticket_no, y_m_d, m_d_str, lines, queryList, outputFile):
    print(lines[0] + " " + lines[1] + " executed")
    """
        y_m_d is a array with [year, month, day]
        m_d_str is a array with [month, day] both data is in the data type string
        
        if month and day is a single digit => then add a '0' in front of it
    """
    if y_m_d[0] > 2020:
                y_m_d[0]= 2000
    if y_m_d[1] > 12:
        y_m_d[1]= 1
    if y_m_d[2] > 31:
        y_m_d[2] = 1
    
    if y_m_d[1] < 10:
        m_d_str[0] = addZero(y_m_d[1])
    if y_m_d[2] < 10:
        m_d_str[1] = addZero(y_m_d[2])

    sql = "declare"
    sql += " \ntemp int;"
    sql += " \nbegin"
    sql += " \nupdate flights"
    sql += " \nset seats_available = case"
    sql += " \nwhen seats_available > 0 and flight_id = " + lines[1]
    sql += " \nthen seats_available-1 else seats_available"
    sql += " \nend,"
    sql += " \nseats_booked = case "
    sql += " \nwhen flight_id = " + lines[1] +  " then seats_booked+1 else seats_booked"
    sql += " \nend;"
    sql += " \ninsert into bookings"
    sql += " \nvalues(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 12700);"  
    sql += " \ninsert into ticket(ticket_no, book_ref, passenger_id)"
    sql += " \nvalues(" + str(ticket_no) + ", " + str(book_ref) + ", " + lines[1] + ");"
    sql += " \nexception"
    sql += " \nwhen seats_available = 0 then rollback, select case when exists(select * from flights where seats_available = 0) then cast (1 as bit)"
    sql += " \nwhen seats_available > 0 then commit, select case when exists(select * from flights where seats_available > 0) then cast (0 as bit);"
    sql += " \nend;\n"
    queryList += sql
    server_var[0].execute(sql)
    fetch = server_var[0].fetchall()
    print(fetch)
    if fetch[0] == 1:
        print("success")
    elif fetch[0] == 0:
        print("fail")
    # time.sleep(1)
    outputFile.write(queryList)

def yesTrans(server_var, fileName, threadNum, queryList, outputFile):
    file = open(fileName, 'r')
    book_ref = 0
    ticket_number = 6000000000
    y_m_d = [2003, 1, 2]
    m_d_str = ["", ""]
    
    lines = []

    for line in file:
        string = line.split(',')
        if line[0].isdigit(): 
            lines.append(string) 

    line = 0
    while line < len(lines):
        print(line)
        threads = []
        for i in range(int(threadNum)):
            y_m_d[0] = y_m_d[0]+1
            y_m_d[1] = y_m_d[1]+1
            y_m_d[2] = y_m_d[2]+1
            book_ref = book_ref+1
            ticket_number = ticket_number+1
            print("creating thread #", i)
            x = threading.Thread(target=yesTransThread, args=(server_var, book_ref, ticket_number, y_m_d, m_d_str, lines[line], queryList, outputFile))
            if line < len(lines)-1:
                print(str(line) + " updated to " + str(line+1))
                line+=1
                print("line:", line, ", range:", len(lines))
            x.start()
            threads.append(x)
            print("x got appended")
        
        for thread in range(len(threads)):
            print("thread #", thread, "joined")
            threads[thread].join()
        line+=1

def noTrans(server_var, fileName, threadNum, queryList, outputFile):
    file = open(fileName, 'r')
    book_ref = 60000
    ticket_no = 6000000000
    y_m_d = [2003, 1, 2]
    m_d_str = ["", ""]
    flight_id = ""
    for i in file:
        string = i.split(',')
        if string[0].isdigit():   

            flight_id = string[1].rstrip()
            
            # sql = "SELECT * FROM flights WHERE seats_available = 50 AND flight_id = " + string[1] + ";"
            # sql = "SELECT flights.scheduled_departure FROM flights WHERE seats_available > 0 AND flight_id = " + string[1] + ";"
            if y_m_d[0] > 2020:
                y_m_d[0] = 2000

            if y_m_d[1] > 12:
                y_m_d[1] = 1

            if y_m_d[2] > 31:
                y_m_d[2] = 1
    
            if y_m_d[1] < 10:
                m_d_str[0] = addZero(y_m_d[1])

            if y_m_d[2] < 10:
                m_d_str[1] = addZero(y_m_d[2])

            sql = "\nSELECT seats_available FROM flights WHERE flight_id = " + flight_id + ";\n"
            queryList += sql
            server_var[0].execute(sql)
            fetch = server_var[0].fetchall()
            temp = fetch
            store = temp[0][0]  
            
            sql = " \nINSERT INTO bookings"
            sql += " \nVALUES(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 12700);\n" 
            server_var[0].execute(sql)
            queryList += sql

            if (store > 0):

                sql = " \nINSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name)"
                sql += " \nVALUES(" + str(ticket_no) + ", " + str(book_ref) + ", " + string[0] + ", " + "' '" + ");\n"
                server_var[0].execute(sql)
                queryList += sql

                """
                sql = " SELECT * FROM ticket"
                server_var[0].execute(sql)
                fetch = server_var[0].fetchall()
                print(fetch)
                """
            
                sql = " \nINSERT INTO ticket_flights(ticket_no, flight_id, fare_conditions, amount)"
                sql += " \nVALUES(" + str(ticket_no) + ", " + flight_id + ", " + "'Economy', " + "'12700');\n"
                server_var[0].execute(sql)
                queryList += sql

                sql = " \nUPDATE flights"
                sql += " \nSET seats_available = seats_available - 1, seats_booked = seats_booked + 1 WHERE flight_id = " + flight_id + ";\n"
                server_var[0].execute(sql)
                queryList += sql

                sql = " \nSELECT seats_available FROM flights WHERE flight_id = " + flight_id + ";\n"
                server_var[0].execute(sql)
                fetch = server_var[0].fetchall()
                temp = fetch
                store = temp[0][0]    
                queryList += sql

                # print(string[1] + " " + str(store)) 
                # print(book_ref)
                #print(flight_id)

            else:
                print("no seats available!")  
                # print(book_ref)
                
        book_ref = book_ref + 1
        y_m_d[0] = y_m_d[0] + 1
        y_m_d[1] = y_m_d[1] + 1
        y_m_d[2] = y_m_d[2] + 1
        ticket_no = ticket_no + 1   

    outputFile.write(queryList)

    #print(store)

        # 1. generate book_ref number, update bookings
        # 2. generate ticket_num, insert new record in ticket and ticket_flights IF available seats in specified flight AND date
        # 3. update seats_booked and seats_available in flights (simple)
        # 4. IF no available seats, ONLY book_ref generated and bookings updated
 
def updateDB(server_var, inputList, queryList, outputFile):

    #delete all the rows from all the tables
    sql = "TRUNCATE ticket CASCADE; "
    sql += "\ndelete from ticket_flights;"
    sql += "\ndelete from bookings;"
    sql += " \nupdate flights "
    sql += " \nset seats_available = 50, seats_booked = 0; \ncommit;\n"
    # sql = "select case when exists ("
    # sql += " select * from flights where seats_available = 0 and flight_id = 1001)"
    # sql += " then cast(1 as bit)"
    # sql += " else cast(0 as bit) end;"
    server_var[0].execute(sql)
    queryList += sql
    # fetch = server_var[0].fetchall()
    # print(fetch)

    if inputList[1] == 'n':
        noTrans(server_var, inputList[0], inputList[2],  queryList, outputFile) #(fileName, # of threads)
    if inputList[1] == 'y':
        yesTrans(server_var, inputList[0], inputList[2], queryList, outputFile) #(fileName, # of threads)

    outputFile.write(queryList)

def main(argv):
    inputs = []
    outputFile = open("checkdb.sql", "w")

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
    updateDB(returned_server_var, inputs, queryList, outputFile)
    outputFile.close()
    

if __name__ == "__main__":
    main(sys.argv[1])
    # callThread()
