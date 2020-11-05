import psycopg2, sys
import logging
import threading
import datetime
import time

book_ref = 0
ticket_number = 0
y_m_d = [2003, 1, 2]
m_d_str = ["", ""]

success_num = 0
unsuccess_num = 0
fail_num = 0

update_bookings = 0
update_flights = 0
update_ticket = 0
update_ticket_flights = 0

count = 0
queryList = ""
outputFile = open("checkdb.sql", "w")

def connectWithDB():
    with open('password.txt') as f:
        lines = [line.rstrip() for line in f]

    username = lines[0]
    pg_password = lines[1]

    conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
    cursor = conn.cursor()

    server_var = [cursor, conn]

    return server_var 

#adds '0' infront of a single digit and returns it
def addZero(singleDigit):
    toReturn = '0' + str(singleDigit)
    return toReturn

def countShit(connection_, lines):
    local_sql = ""
    sql = "\nselect flight_id, seats_available, seats_booked from flights where seats_available = 0 and flight_id = " + lines[1] + ";"
    local_sql += sql
    connection_[0].execute(sql)
    # d_count = 1
    flight_val_n_seats_not_free = connection_[0].fetchall()
    
    sql = "\nselect * from flights where flight_id = " + lines[1] + ";"
    local_sql += sql
    connection_[0].execute(sql)
    flight_val = connection_[0].fetchall()

    global queryList
    queryList += local_sql
    
    global fail_num
    global unsuccess_num
    global success_num

    if not flight_val or lines[0] == "" or lines[0] == "NULL" or lines[1] == "" or lines[1] == "NULL":
        fail_num += 1
        return -2
    elif not flight_val_n_seats_not_free:
        success_num+=1
        return 0
    elif len(flight_val_n_seats_not_free) > 0:
        unsuccess_num+=1
        return -1

def yesTransThread(lines, lock):
    global y_m_d
    global m_d_str

    if y_m_d[1] < 10:
        m_d_str[0] = addZero(y_m_d[1])
    if y_m_d[2] < 10:
        m_d_str[1] = addZero(y_m_d[2])
    
    if y_m_d[1] > 12:
        y_m_d[0] += 1
        y_m_d[1] = 1
    elif y_m_d[2] > 30:
        y_m_d[1] += 1 
        y_m_d[2] = 1
    else:
        y_m_d[1] += 1 

    with lock:
        global book_ref
        book_ref += 1
        global ticket_number
        ticket_number += 1

        global queryList

        global update_bookings
        global update_flights
        global update_ticket
        global update_ticket_flights

        connection = connectWithDB()
        continue_ = countShit(connection, lines)
        if continue_ == -1:
            sql = "\nbegin transaction;"
            sql += " \ninsert into bookings"
            sql += " \nvalues(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 127000);"  
            sql += " \ncommit transaction;"
            queryList += sql
            connection[0].execute(sql)
            update_bookings += 1
            return
        if continue_ == -2:
            return        

        sql = "\nbegin transaction;"

        sql += " \ninsert into bookings"
        sql += " \nvalues(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 127000);"  
        update_bookings += 1
        sql += " \nupdate flights"
        sql += " \nset seats_available = case"
        sql += " \nwhen seats_available > 0 and flight_id = " + lines[1]
        sql += " \nthen seats_available-1 else seats_available"
        sql += " \nend,"
        sql += " \nseats_booked = case "
        sql += " \nwhen flight_id = " + lines[1] +  " then seats_booked+1 else seats_booked"
        sql += " \nend;"
        update_flights += 1
        sql += " \nINSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name)"
        sql += " \nVALUES(" + str(ticket_number) + ", " + str(book_ref) + ", " + lines[0] + ", " + "' '" + ");\n"
        update_ticket += 1
        sql += " \nINSERT INTO ticket_flights(ticket_no, flight_id, fare_conditions, amount)"
        sql += " \nVALUES(" + str(ticket_number) + ", " + lines[1] + ", " + "'Economy', " + "'12700');\n"
        update_ticket_flights += 1
        sql += " \ncommit transaction;"
        queryList += sql
        connection[0].execute(sql)

def yesTrans(fileName, threadNum):
    file = open(fileName, 'r')
    
    lines = []

    for line in file:
        string = line.split(',')
        if line[0].isdigit(): 
            lines.append(string) 
    
    lock_var = threading.Lock()

    line = 0
    while line < len(lines):
        threads = []
        for i in range(int(threadNum)):
            if line < len(lines):
                x = threading.Thread(target=yesTransThread, args=(lines[line], lock_var,))
                x.start()
                threads.append(x)
            line+=1
        
        for thread in range(len(threads)):
            threads[thread].join()

def noTransThread(lines, lock):
    global y_m_d
    global m_d_str
    if y_m_d[1] < 10:
        m_d_str[0] = addZero(y_m_d[1])
    if y_m_d[2] < 10:
        m_d_str[1] = addZero(y_m_d[2])
    
    if y_m_d[1] > 12:
        y_m_d[0] += 1
        y_m_d[1] = 1
    elif y_m_d[2] > 30:
        y_m_d[1] += 1 
        y_m_d[2] = 1
    else:
        y_m_d[1] += 1     
    
    with lock:
        connection = connectWithDB()
        global book_ref
        global ticket_number
        global queryList

        global update_bookings
        global update_flights
        global update_ticket
        global update_ticket_flights

        continue_ = countShit(connection, lines)
        if continue_ == -1:
            sql = " insert into bookings"
            sql += " values(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 127000);"  
            queryList += sql
            connection[0].execute(sql)
            update_bookings += 1
            return
        if continue_ == -2:
            return

        sql = " insert into bookings"
        sql += " values(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 127000);" 
        update_bookings += 1
        queryList += sql
        connection[0].execute(sql)

        sql = " \nINSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name)"
        sql += " \nVALUES(" + str(ticket_number) + ", " + str(book_ref) + ", " + lines[0] + ", " + "' '" + ");\n"
        update_ticket += 1
        queryList += sql
        connection[0].execute(sql)

        sql = " \nUPDATE flights"
        sql += " \nSET seats_available = seats_available - 1, seats_booked = seats_booked + 1 WHERE flight_id = " + lines[1] + ";\n"
        update_flights += 1
        connection[0].execute(sql)
        queryList += sql

        sql = " \nINSERT INTO ticket_flights(ticket_no, flight_id, fare_conditions, amount)"
        sql += " \nVALUES(" + str(ticket_number) + ", " + lines[1] + ", " + "'Economy', " + "'12700');\n"
        update_ticket_flights += 1
        queryList += sql
        connection[0].execute(sql)
    
    book_ref += 1
    ticket_number += 1

def noTrans(fileName, threadNum):
    file = open(fileName, 'r')
    
    lines = []

    for line in file:
        string = line.split(',')
        if line[0].isdigit(): 
            lines.append(string) 

    line = 0
    lock_var = threading.Lock()

    while line < len(lines):
        threads = []
        for i in range(int(threadNum)):
            if line < len(lines):
                x = threading.Thread(target=noTransThread, args=(lines[line], lock_var,))
                x.start()
                threads.append(x)
            line+=1
        
        for thread in range(len(threads)):
            threads[thread].join()

def updateDB(inputList):
    sql = "truncate ticket CASCADE; "
    sql += "delete from ticket;"
    sql += " delete from bookings;"
    sql += " update flights "
    sql += " set seats_available = 50, seats_booked = 0; commit;"
    connectWithDB()[0].execute(sql)
    start = time.time()
    if inputList[1] == 'n':
        noTrans(inputList[0], inputList[2]) #(fileName, # of threads)
    if inputList[1] == 'y':
        yesTrans(inputList[0], inputList[2]) #(fileName, # of threads)
    end = time.time()
    print("success: ", success_num)
    print("unsuccess: ", unsuccess_num)
    # print("fail: ", fail_num) 
    print("# of records update for table bookings:", update_bookings)
    print("# of records update for table flights:", update_flights)
    print("# of records update for table ticket:", update_ticket)
    print("# of records update for table ticket_flights:", update_ticket_flights)
    print(f"Runtime of the program is {end - start} seconds")
    outputFile.write(queryList)

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
    inputs.append(inputThreadNum)

    updateDB(inputs)

if __name__ == "__main__":
    main(sys.argv[1])