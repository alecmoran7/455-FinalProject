import re
inputFile = open("internetStats.csv", "r")
outputFile = open("internetTrimmed.csv", "w")
nextLine = inputFile.readline()
while nextLine:
    newString = nextLine[:5] + "," + nextLine[5:8] + "," + nextLine[8:]
    outputFile.write(newString)
    nextLine = inputFile.readline()
inputFile.close()
outputFile.close()
