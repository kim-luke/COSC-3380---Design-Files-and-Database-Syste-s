#!/usr/bin/python

import psycopg2,sys

with open('password.txt') as f:
    lines = [line.rstrip() for line in f]

username = lines[0]
pg_password = lines[1]

conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
cursor = conn.cursor()

lines = []
storeRefInt = []
storeNorm = []
storeTables = []
storeDBRI = []
storeDBNorm = []
queryList = ""
checkRef = False
checkNorm = False
# compare1 = 0
# compare2 = 0
# storeTable1 = ""
# storeTable2 = ""

def checkRefInt(toRead, queryList, outputFile, checkRef):
    count = 0

    # checkRef = True
    for line in toRead:
        lines.append(line.split(','))

    for i in lines:
        if (i[0][6:8] != "pk"):
            checkRef = False
            count = count + 1

        reference_table = i[0][0:2]  # original table
        storeTables.append(str(reference_table))
        fk_index = str(i[1]).find("fk")
        if fk_index == -1:
            checkRef = True

        else:
            fk_table = i[1][6:8] # foreign key's table
            fk_column = i[1][9:11] #  foreign key's column
            reference_column = i[1][0:2] # original table column which contains the foreign key
            g = "\nSELECT " + reference_column + " \nFROM " + reference_table + " WHERE " + reference_column + " IS NULL;\n"
            cursor.execute(g)
            queryList += g
            nullCheck = cursor.fetchall()
            # print(type(nullCheck))

            if nullCheck:
                checkRef = False
                count = count + 1
                print(type(nullCheck))

            else:
                s = "\nSELECT " + fk_column + " \nFROM " + fk_table + " \nINTERSECT SELECT " + reference_column + " FROM " + reference_table + " \nORDER BY " + fk_column + " ASC;\n"
                # print(s)
                queryList += s
                # print(queryList)
                cursor.execute(s)
                store = cursor.fetchall()
                # print(store)
                r = "\nSELECT DISTINCT " + reference_column + " \nFROM "  + reference_table + " \nORDER BY " + reference_column + " ASC;\n"
                queryList += r
                # print(queryList)
                # print(r)
                cursor.execute(r)
                record = cursor.fetchall()
                # print(record)

                if (store == record):
                    checkRef = True

                else:
                    checkRef = False
                    count = count + 1
        # print(reference_table)
        if checkRef == False:
            # resultFile.write(reference_table + "                " + "N\n")
            storeRefInt.append("N")
        else:
            # resultFile.write(reference_table + "                " + "Y\n")
            storeRefInt.append("Y")

    # print(queryList)
    if checkRef == False or count >= 1:
        # resultFile.write("DB referential integrity: N\n")
        storeDBRI.append("N")
        # storeRefInt.append("N")
        # print(storeRefInt)
    else:
        # resultFile.write("DB referential integrity: Y\n")
        storeDBRI.append("Y")
        # storeRefInt.append("Y")
        # print(storeRefInt)
    outputFile.write(queryList)

def checkNorma(toRead, queryList, outputFile, checkNorm):

    word = ""
    char = ''
    bad_chars = [')']
    count = 0
    compare1 = 0
    compare2 = 0
    storeTable1 = ""
    storeTable2 = ""
    check = 0

    for line in toRead:
        lines.append(line.split(','))

    for i in lines:
        if (i[0][6:8] != "pk"):
            # resultFile.write("Normalization of " + i[0][:2] + ": No\n")
            checkNorm = False
            check = check + 1
        # print(i)
        reference_table = i[0][0:2]
        # print(reference_table)

        for word in i:
            # print(word)
            # count = 0
            # compare1 = 0
            # compare2 = 0
            # storeTable1 = ""
            # storeTable2 = ""
            
            if (word[0:1] == 'T'):
                reference_column = word[3:5]
                #print(reference_column)

            else:
                reference_column = word[0:2]

            reference_column = ''.join(i for i in reference_column if not i in bad_chars)
            # print(reference_column)
            s = "\nSELECT " + reference_column + ", COUNT(*) \nFROM " + reference_table + " \nGROUP BY " + reference_column + " \nHAVING COUNT(*) > 1\n"
            queryList += s
            # print(queryList)
            cursor.execute(s)
            store = cursor.fetchall()
            # print(s)
            # print(reference_table)
            # print(store)

            checkNorm = True
            # compare1 = 0
            # compare2 = 0
            # storeTable1 = ""
            # storeTable2 = ""

            if store:
                # print(store)

                for a in store:

                    # storeTable1 = ""
                    # storeTable2 = ""
                    # print(a[0])
                    # compare1 = a[0]
                    # print(compare1)
                    # print(a[-1])
                    # print(i[-1])

                    # compare1 = i
                    # storeTable1 = reference_table
                    # print(compare1)
                    # print(storeTable1)

                    if a[-1] > 2:
                        # print(i[-1])
                        # print("Normalization of " + reference_table + ": No")
                        checkNorm = False
                        check = check + 1

                    elif (a[-1] == 2):

                        # print(a)

                        if (count == 0):
                            compare1 = a[0]
                            # print(compare1)
                            storeTable1 = reference_table
                            # count = count + 1
                            # print(count)
                            # print(str(compare1) + "compare 1")
                            # print(count)
                            # print("Compare1 " + reference_table)
                            count = count + 1
                            if(storeTable1 == storeTable2):
                                c = "\nSELECT "  + reference_column + " \nFROM " + reference_table + " \nGROUP BY " + reference_column + "\nHAVING " + reference_column + " = " + str(compare1) + " AND " + reference_column + " = " + str(compare2) + ";"
                                # print(c)
                                queryList += c
                                cursor.execute(c)
                                recprd = cursor.fetchall()
                                # print(record)

                        if (count >= 1 and compare1 != a[0]):
                            compare2 = a[0]
                            # print(str(compare2) + "compare 2")
                            storeTable2 = reference_table
                            if(storeTable1 == storeTable2):
                                c = "\nSELECT "  + reference_column + " \nFROM " + reference_table + " \nGROUP BY " + reference_column + "\nHAVING " + reference_column + " = " + str(compare1) + " AND " + reference_column + " = " + str(compare2) + ";"
                                # print(c)
                                queryList += c
                                cursor.execute(c)
                                record = cursor.fetchall()
                                # print(record)
                                if record:
                                    print("eh")
                                else:
                                    # print("ehhh")
                                    checkNorm = True
                                # print(compare1)
                                # print(compare2)
                            # print(count)
                            # print("Compare2 " + reference_table)
                            count = 0
                    else:
                        checkNorm = True

                    # count = count + 1

        if checkNorm == False:
            # resultFile.write(reference_table + "                " + "N\n")
            storeNorm.append("N")
            # print(storeNorm)
        else:
            # resultFile.write(reference_table + "                " + "Y\n")
            storeNorm.append("Y")
            # print(storeNorm)

    # print(queryList)
    if checkNorm == False or check >= 1:
        storeDBNorm.append("N")
        # resultFile.write("DB normalized: N")
    else:
        storeDBNorm.append("Y")
        # resultFile.write("DB normalized: Y")

    outputFile.write(queryList)

def main(argv):
    z = 0
    index = argv.find('=')
    inputFileName = argv[index + 1:len(argv)]
    file = open(inputFileName)
    outputFile = open("checkdb.sql", "w")
    resultFile = open("refintnorm.txt", "w")

    resultFile.write("refintnorm.txt\n")
    resultFile.write("-----------------------------------------\n")
    resultFile.write("    referential integrity normalized\n")

    checkRefInt(file, queryList, outputFile, checkRef)
    checkNorma(file, queryList, outputFile, checkNorm)
    for z in range(len(storeTables)):
        resultFile.write(str(storeTables[z]) + "                " + str(storeRefInt[z]) + "           " + str(storeNorm[z]) + "\n")
        z = z + 1

    resultFile.write("DB referential integrity: " + str(storeDBRI[0]))
    resultFile.write("\nDB normalized: " + str(storeDBNorm[0]))
    # outputFile.write(queryList)
    file.close()
    outputFile.close()
    resultFile.close()


    # inputFileName = argv[index + 1:len(argv)]
    # file = open(inputFileName)

    # checkNorm(file, queryList)
    # file.close()

if __name__ == "__main__":
    main(sys.argv[1])
