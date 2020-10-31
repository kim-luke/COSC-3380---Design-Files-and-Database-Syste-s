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

#adds '0' infront of a single digit and returns it
def addZero(singleDigit):
    toReturn = '0' + str(singleDigit)
    return toReturn

def yesTrans(server_var, fileName, threadNum):
    file = open(fileName, 'r')
    book_ref = 0
    ticket_number = 6000000000
    y_m_d = [2003, 1, 2]
    m_d_str = ["", ""]
    for line in file:
        """
            y_m_d is a array with [year, month, day]
            m_d_str is a array with [month, day] both data is in the data type string
            
            if month and day is a single digit => then add a '0' in from of it
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
        
        #split the line with ','
        string = line.split(',')

        #if the string before the first ',' is a digit
        if string[0].isdigit(): 
            sql = "start transaction;"
            sql += " update flights"
            sql += " set seats_available = case"
            sql += " when seats_available > 0 and flight_id = " + string[1]
            sql += " then seats_available-1 else seats_available"
            sql += " end,"
            sql += " seats_booked = case "
            sql += " when flight_id = " + string[1] +  " then seats_booked+1 else seats_booked"
            sql += " end;"
            sql += " insert into bookings"
            sql += " values(" + str(book_ref) + ", TIMESTAMP '" + str(y_m_d[0]) + "-" + m_d_str[0] + "-" + m_d_str[1] + " 00:00:00-05'" + ", 127000);"  
            sql += " insert into ticket"
            sql += " values(" + str(ticket_number) + ", " + str(book_ref) + ", " + string[1] + ", null, null, null);"
            sql += " commit;"
            server_var[0].execute(sql)

            y_m_d[0] = y_m_d[0]+1
            y_m_d[1] = y_m_d[1]+1
            y_m_d[2] = y_m_d[2]+1
            book_ref = book_ref+1
            ticket_number = ticket_number+1            

            

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

    #delete all the rows from all the tables
    sql = "delete from bookings; "
    sql += "delete from ticket; commit;"
    server_var[0].execute(sql)

    if inputList[1] == 'n':
        noTrans(server_var, inputList[0], inputList[2]) #(fileName, # of threads)
    if inputList[1] == 'y':
        yesTrans(server_var, inputList[0], inputList[2]) #(fileName, # of threads)

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