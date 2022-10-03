from logging import exception
from turtle import delay
#from types import NoneType
from xmlrpc.client import boolean
import requests
import time
from bs4 import BeautifulSoup
from time import sleep
import mysql.connector
from pyModbusTCP.client import ModbusClient
import sys
import json
from pathlib import Path
import WPConfig
import os
from configparser import ConfigParser

def DatenAbrufen(urlin, Liste, SuchZeichenEnde):
    # Url of website
    rawdata=requests.get(urlin)
    html=rawdata.content

    # Parsing html content with beautifulsoup
    soup = BeautifulSoup(html, 'html.parser')

    paragraphs = soup.find_all('td')
    Liste.clear()
    
    for i in paragraphs:
        Liste.append(str(i).replace('</td>', ''))
    
    
    for i in range(0, len(Liste)):
        Ende = Liste[i].find(SuchZeichenEnde, 17)
        if (Ende < 0):
            Liste[i] = Liste[i][Liste[i].find(">")+1:]
        else:
            Liste[i] = Liste[i][Liste[i].find(">")+1:Ende]

# Abrufen als Zahl speichern
def DatenAbrufenZahl(urlin, Liste, SuchZeichenEnde):
    # Url of website
    rawdata=requests.get(urlin)
    html=rawdata.content

    # Parsing html content with beautifulsoup
    soup = BeautifulSoup(html, 'html.parser')

    paragraphs = soup.find_all('td')
    Liste.clear()
    TempList = []
    for i in paragraphs:
        TempList.append(str(i).replace('</td>', ''))
    
 #   print(TempList)
    for i in TempList:
        Ende = i.find(SuchZeichenEnde, 17)
        Temp = 0
        if (Ende < 0):
            Temp = i[i.find(">")+1:].replace(",", ".")
        else:
            Temp = i[i.find(">")+1:Ende].replace(",", ".")
        try:
            Liste.append(float(Temp))
        except ValueError:
            pass



def EinstellungenAbrufen(urlin, Liste, Flag_Abfrageart): #Abfrageart 1.bit 2⁰ = Textfelder, 2. bit 2¹ Auswahlfelder
    # Url of website
    rawdata=requests.get(urlin)
    html=rawdata.content
    # Parsing html content with beautifulsoup
    soup = BeautifulSoup(html, 'html.parser')
    # Parsing-Code zum Auslesen der Auswahlfelder
    if(Flag_Abfrageart & 0x02):
        paragraphs = soup.find_all('input')
        ValPos = 0
        StartPos = 0
        EndPos = 0
        TempVal = 0
        TempVal2 = 0
        for i in paragraphs:
            if(str(i).find("checked")!=-1):
                Temp = str(i)
                ValPos = Temp.find("name=")
                StartPos = ValPos + 9
                EndPos = StartPos + Temp[StartPos:StartPos+10].find("\"")
                TempVal = Temp[StartPos:EndPos]
                ValPos = Temp.find("value=")
                StartPos = ValPos + 7
                EndPos = StartPos + Temp[StartPos:StartPos+10].find("\"")
                TempVal2 = Temp[StartPos:EndPos]
                Liste.append([TempVal, TempVal2])
    # Parsing-Code zum Auslesen der normalen Textfelder
    if(Flag_Abfrageart & 0x01):
        paragraphs = soup.find_all('script')
        ValPos = 0
        StartPos = 0
        EndPos = 0
        for i in paragraphs:
            if(str(i).find("jsvalues[")!=-1):
                Temp = str(i)
                Temp = Temp.replace("datecheck", "")
                ValPos = Temp.find("['val']")
                StartPos = ValPos -9 + Temp[ValPos-10:ValPos+10].find("'")
                EndPos = StartPos + Temp[StartPos:StartPos+10].find("'")
                TempVal = Temp[StartPos:EndPos]
                StartPos = ValPos + 9
                EndPos = StartPos + Temp[StartPos:StartPos+10].find("'")
                TempVal2 = Temp[StartPos:EndPos].replace(',', '.')
                Liste.append([TempVal, TempVal2])


def existDBHeader(Liste, Start, lstDB, DBCursor):
    NeuerEintrag = 0
    for i in range(Start, len(Liste), 2):
        try:
            lstDB.index(Liste[i])
        except ValueError:
            DBCursor.execute ("INSERT INTO tabStatusHeader (strHeader) VALUES ('" + Liste[i] + "');")
            NeuerEintrag = 1
    return NeuerEintrag

def makeSQLcommandStatus(Liste, lstDB):
    strSQLColumns = "INSERT INTO tabStatus (Zeit_id"
    strSQLValues = ") VALUES (" + time.strftime("\n%Y%m%d%H%M00", time.localtime())
    if (len(Liste) > 1):
        for i in range(0, len(Liste), 2):
            strSQLColumns = strSQLColumns + ", Data_" + str(lstDB.index(Liste[i]))
            strSQLValues = strSQLValues + ", 1"
    else:
        return str(len(Liste))
    
    return strSQLColumns + strSQLValues + ");"

def printStatus(Liste):
    strTemp = ""
    if (len(Liste) > 1):
        for i in range(0, len(Liste), 2):
            strTemp = strTemp + Liste[i] + " | "
    else:
        return "Kein Eintrag" + time.strftime(str(Fehler+1)+"%Y-%m-%d, %H:%M", time.localtime())
    
    return strTemp + time.strftime(str(Fehler+1)+"%Y-%m-%d, %H:%M", time.localtime())


def StatuslisteAbrufen(curs, List):
    try:
        curs.execute ("SELECT strHeader FROM tabStatusHeader;")
        rows = curs.fetchall()
        List.clear()
        for row in rows:
            List.append(row[0])
    except:
        print("SQL-DB-Error. Rolling back.")
        print("Fehler beim Datensatz Auslesen")
        db.rollback()
        db.close()
        quit()

def SQLEinstellungenAbrufen(curs, Config, TabName, ListOut):
    ListOut.clear()
    for i in Config:
        if(i[2] < 3):
            curs.execute ("SELECT " + i[1] + " FROM " + TabName[i[2]] + " ORDER BY Zeit_id DESC LIMIT 1;")
            rows = curs.fetchall()
            ListOut.append(rows[0][0])
        else:
            ListOut.append(0)



#Funktion für Solarwerte in reelle Zahlen zu konvertieren
def ConvertRegisterValue(Data, CountRegister, factor, signed):
    try:
        if(Data == None):
            return -999
        Temp = Data[0]
        #Register zusammensetzen
        for j in range(1, CountRegister):
            Temp = (Temp<<16)+ Data[j]
        #Wenn Signed, umwandlung in Minuswert
        if((Temp&(1<<((CountRegister * 16)-1)))and(signed)):
            Temp = Temp - 2**(CountRegister * 16)
        #Wenn Faktor > 0 muss die Kommastelle korrigiert werden
        if(factor):
            Temp = Temp / (10**factor)
    except Exception as e:
        print("Fehler beim Convertieren (PV-Daten)")
        strExcept = " 110 Fehler beim Convertieren (PV-Daten)\n"
        strExcept += str(e)
        strExcept += "\nData: " + str(Data)
        strExcept += "\nCountRegister: " + str(CountRegister) + "\n"
        WPConfig.logData(strExcept, FehlerLogFile)

    return Temp
#Datensatz bei 2dimensionaler Liste suchen Value-Spalte (Suchwertspalte) muss sortiert sein (Column ist die Spalte des Suchwerts)
def DatasetSearch(Data, Value, Column):
    Start = 0
    End = len(Data)-1
    while ((End - Start) > 1):
        if(Data[Start + int((End - Start)/2)][Column] == Value):
            return Start + int((End - Start)/2)
        if(Value < Data[Start + int((End - Start)/2)][Column]):
            End = Start + int((End - Start)/2)
        else:
            Start = Start + int((End - Start)/2)
    if(Data[Start][Column] == Value):
        return Start
    else: 
        if(Data[End][Column] == Value):
            return End
        else:
            return -1


if __name__ == "__main__":
    curs= None
    db = None
    Counter = 500
    myobj = {'URI' : 'POST /reboot.php HTTP/1.1'}
    Fehler = 0
    FehlerSumme = 0
    Sekunde = 0
    MinSperre = 1
    Sek10Sperre = 1
    TageswerteLesen = 0
    TageswertWPSchreiben = 0
    EinstellungWPPruefen = 0
    EinstellungWPCounter = 0
    aktTag = int(time.strftime("%d", time.localtime()))
    SolAktiv = 1
    SolPause = 0
    CountDaten = 0
    WPPause = 0
    strListStatus = []
    WPDaten_1 = []
    WPDaten_2 = []
    SolDaten = []
    SolRohDaten = []
    RegisterData = []
    WPColumnData = []
    WPTableData = []
    WPConfigSites = []
    WPConfigColumn = []
    DateiJSON = []
    DataLogFile = ''
    FehlerLogFile = ''
    StringSQLTabName = ["tabSolGenDev", "tabSolDayVal", "tabSolDCData", "tabSolACData"]
    StringSQLTabName2 = ["tabISGHeizenConfig", "tabISGWWLuftConfig", "tabISGRestConfig"]
    StringSQL1 = [] #Wechselrichter SQL-Anweisung
    StringSQL2 = [] #WP-Daten SQL-Anweisung
    StringVisualSpalten = [] #Variable zum Aufnehmen der Srrings für die Visuelle ausgabe am Raspi
    strSQLConfig = [] #Variable zum aufnehmen der aktuell letzten Daten in der SQL-DB
    Anfangswert = 0
    AktuelleLText = ""

    #Get the configparser object
    config_object = ConfigParser()
    config_object.read(sys.argv[0][:-8]+'config.ini')
    for sect in config_object.sections():
        print('Section:', sect)
        for k,v in config_object.items(sect):
            print(' {} = {}'.format(k,v))
        print()

    SQL_Config = config_object["SQL_CONFIG"]
    General_Config = config_object["GENERAL_CONFIG"]
    Modbus_Config = config_object["MODBUS_CONFIG"]
    sleep(3)
    if (len(sys.argv)>1):
        if(sys.argv[1] == "r"):
            x = requests.post(General_Config["isgaddress"] + "reboot.php", data = myobj)
            print(x)
            print("Warte 180 s bis ISG neu gestartet wurde")
            WPPause = 3
            Counter = 0
        else:
            try:
                TempZahl=0
                TempZahl = int(sys.argv[1]) 
                if((TempZahl >=0) and (TempZahl < 681)):
                    Counter = TempZahl
                print("Counter auf " + str(TempZahl) + " gesetzt!")
            except:
                print("Eingabewert nicht bekannt!")    
    
    try:
        DataLogFile = General_Config["pydirectory"] + 'Data.log'
        FehlerLogFile = General_Config["pydirectory"] + 'Fehler.log'
        DateiJSON.append(General_Config["pydirectory"] + "data_file.json")
        DateiJSON.append(General_Config["pydirectory"] + "WP_Todo.json")
        DateiJSON.append(General_Config["pydirectory"] + "data_file_WP_column.json")
        DateiJSON.append(General_Config["pydirectory"] + "data_file_WP_table.json")
        DateiJSON.append(General_Config["pydirectory"] + "data_file_WP_config_sites.json")
        DateiJSON.append(General_Config["pydirectory"] + "data_file_WP_config_column.json")
        db = mysql.connector.connect(host=SQL_Config["host"],
                             database=SQL_Config["database"],
                             user=SQL_Config["user"],
                             password=SQL_Config["password"],
                             port=int(SQL_Config["port"]))
    
        db.autocommit = True
        curs=db.cursor()

    except Exception as e:
        print (f"Error connecting to MySQL-DB: {e}")
        quit()


    #Konfiguration aus JSON-Datei einlesen (WP-Einstellungen HTML-Seiten)
    with open(DateiJSON[4], "r") as read_file:
        WPConfigSites = json.load(read_file)
        read_file.close

    #Konfiguration aus JSON-Datei einlesen (WP-Einstellungen Spalten)
    with open(DateiJSON[5], "r") as read_file:
        WPConfigColumn = json.load(read_file)
        read_file.close

    #Letzten Daten von SQL-DB abrufen
    SQLEinstellungenAbrufen(curs, WPConfigColumn, StringSQLTabName2, strSQLConfig)


    StatuslisteAbrufen(curs, strListStatus)


    #Registerkonfiguration aus JSON-Datei einlesen
    with open(DateiJSON[0], "r") as read_file:
        RegisterData = json.load(read_file)
        read_file.close

    #WP-Spaltenkonfiguration aus JSON-Datei einlesen
    with open(DateiJSON[2], "r") as read_file:
        WPColumnData = json.load(read_file)
        read_file.close

    #WP-Tabellenkonfiguration aus JSON-Datei einlesen
    with open(DateiJSON[3], "r") as read_file:
        WPTableData = json.load(read_file)
        read_file.close

    
    #Modbus-Verbindung aufbauen
    c = ModbusClient(host=Modbus_Config["host"], port=int(Modbus_Config["port"]), unit_id=int(Modbus_Config["unit_id"]), auto_open=(Modbus_Config["auto_open"]=="True"))
    #Modbus-SQL-Anweisungen vorbereiten Anfang mit Tabellen- und Spaltennamen
    
    x = 0
    tempString = "\n     ZeitID    | "
    for list1, j in zip(StringSQLTabName, range(0, 4)):
        StringSQL1.append("INSERT INTO " + list1 + " (Zeit_id")
        for i in RegisterData:
            #SQL-Daten vorbereiten
            if(i[4]==j+1):
                StringSQL1[j] += ", " + i[5]
            #Anzeige vorbereiten was später auf dem Terminal steht
            if((i[4]!=0)and(i[4]!=2)and(i[7])and(j == 0)):
                if((len(i[5])+len(tempString))>95):
                    StringVisualSpalten.append(tempString)
                    tempString = i[5] + " | "
                    x +=1
                else:
                    tempString += i[5] + " | "
            
                            
        StringSQL1[j] += ") VALUES ("
    StringVisualSpalten.append(tempString)
    #WP-SQL-Anweisungen vorbereiten Anfang mit Tabellen- und Spaltennamen
    tempString = ""
    for j in range(0, 8):
        StringSQL2.append("INSERT INTO " + WPTableData[j+1][1] + " (Zeit_id")
    for i in WPColumnData:
        #SQL-Daten vorbereiten
        if(i[1]):
            StringSQL2[i[1]-1] += ", " + i[0]
        #Anzeige vorbereiten was später auf dem Terminal steht
        if((len(i[0])+len(tempString))>95):
            StringVisualSpalten.append(tempString)
            tempString = i[0] + " | "
            x +=1
        else:
            tempString += i[0] + " | "
    StringVisualSpalten.append(tempString)
    #Mittelteil der WP-SQL-Anweisung nach Spaltenname anhängen       
    for j in range(0, 8):                            
        StringSQL2[j] += ") VALUES ("
    while(1):
        Sekunde = int(time.strftime("%S", time.localtime()))
        
        if(((Sekunde%10) > 5)and(Sek10Sperre)): # Sperre aufheben damit nur einmal alle 10 Sekunden die Datenabfrage für Sol durchläuft
            Sek10Sperre = 0
        if((Sekunde > 30)and(MinSperre)): # Sperre aufheben damit nur einmal in der Minute die Datenabfrage für WP durchläuft
            MinSperre = 0
        #Aufgaben die alle 10 Sekunden ausgeführt werden
        if(((Sekunde%10) < 4)and(Sek10Sperre == 0)):
            Sek10Sperre = 1
            try:
                SolRohDaten = c.read_holding_registers(RegisterData[45][0], RegisterData[45][1])#innentemperatur Gerät abfragen zum verifizieren, dass Wechselrichter arbeitet
                if(ConvertRegisterValue(SolRohDaten, RegisterData[45][1], RegisterData[45][2], RegisterData[45][3]) >= 0):
                    for i in range(0, len(RegisterData)):
                        if(((RegisterData[i][4]!=0)and(RegisterData[i][4]!=2))or((RegisterData[i][4]==2) and TageswerteLesen)):#nicht in Gruppe 0 und nicht in Gruppe 2 auser wenn in Gruppe 2 dann mit Tageswert lesen
                            SolRohDaten = c.read_holding_registers(RegisterData[i][0], RegisterData[i][1])
                            if(SolRohDaten == None): #Wenn keine Daten Empfangen, abbrechen
                                SolDaten.clear()
                                SolAktiv = 0
                                CountDaten = 0
                                break
                            else:
                                SolAktiv = 1

                        if(CountDaten > 0):
                            if(((RegisterData[i][4]!=0)and(RegisterData[i][4]!=2)and(RegisterData[i][6]))):#nicht in Gruppe 0 und nicht in Gruppe 2 und wenn Durchschnitt berechtnet wird
                                SolDaten[i] += ConvertRegisterValue(SolRohDaten, RegisterData[i][1], RegisterData[i][2], RegisterData[i][3])
                                if(i == 23):
                                    AktuelleLText = "\tLeistung Ges Aktuell in W: " + str(ConvertRegisterValue(SolRohDaten, RegisterData[i][1], RegisterData[i][2], RegisterData[i][3]))
                                    print(AktuelleLText, end = "\r")
                        else:    
                            if(((RegisterData[i][4]!=0)and(RegisterData[i][4]!=2))or((RegisterData[i][4]==2) and TageswerteLesen)):
                                SolDaten.append(ConvertRegisterValue(SolRohDaten, RegisterData[i][1], RegisterData[i][2], RegisterData[i][3]))
                                if((RegisterData[i][4]==2) and TageswerteLesen and SolRohDaten[1]):
                                    TageswerteLesen += 1
                            else:
                                SolDaten.append(0)
                    CountDaten += 1
                    
                else:
                    SolAktiv = 0
                    SolDaten.clear()
                    if(int(time.strftime("%d", time.localtime())) != aktTag):
                        aktTag = int(time.strftime("%d", time.localtime())) 
                        TageswerteLesen = 1
                        TageswertWPSchreiben = 1
                        EinstellungWPPruefen = 1
                        CountDaten = 0
            except Exception as e:
                print("Fehler beim Datensatz Erstellen für (PV-Daten)")
                print(SolDaten)
                strExcept = " 100 Fehler beim Datensatz Erstellen (Solardaten)\n" + "Fehlerindex = "
                strExcept += "Index i = " + str(i) + "SolDaten = " + str(SolDaten) + "\nRohdaten = " + str(SolRohDaten)
                strExcept += str(e)
                WPConfig.logData(strExcept, FehlerLogFile)
        #Aufgaben die Minütlich ausgeführt werden
        if((Sekunde < 20)and(MinSperre == 0)):
            MinSperre = 1
            #Solardaten SQL-Befehle erstellen
            if(SolAktiv):
                TempSQLCommand = []
                x = 0
                tempString = time.strftime("%Y%m%d%H%M00 | ", time.localtime())
                for i in range(0, 4):
                    TempSQLCommand.append(StringSQL1[i] + time.strftime("%Y%m%d%H%M00", time.localtime()))
                #für Tageswerte 000000 Uhrzeit als Zeitid eintragen
                TempSQLCommand[1] = StringSQL1[1] + time.strftime("%Y%m%d000000", time.localtime())
                if(len(RegisterData) <= len(SolDaten)):    
                    for Reg, Sol in zip(RegisterData, SolDaten): # range(0, len(RegisterData)):
                        varTeiler = 1
                        if(Reg[4]==0): #Bei Gruppe 0 soll die Schleife übersprungen werden
                            continue
                        if(Reg[6]):
                            varTeiler *= CountDaten
                        TempSQLCommand[(Reg[4])-1]+= ", " + str(Sol/varTeiler)
                        #Terminal Anzeige erstellen
                        if(Reg[7]):
                            if((len(Reg[5])+len(tempString))>95):
                                print(StringVisualSpalten[x])
                                print(tempString)
                                tempString = ""
                                x +=1
                            var = "{:^"+ str(len(Reg[5])) +"} | "
                            tempString += var.format(round((Sol/varTeiler), 2))
                print(StringVisualSpalten[x])
                print(tempString)
                SolDaten.clear()
                CountDaten = 0
                #Solardaten in DB schreiben
                try:
                    SolRohDaten = c.read_holding_registers(RegisterData[2][0], RegisterData[2][1])#Schütz auslesen
                    if((SolRohDaten[1] == 51) or (SolRohDaten[1] == 311)):
                        for i in range(0, 4):
                            TempSQLCommand[i]+=");"
                            if (i != 1):
                                curs.execute(TempSQLCommand[i])
                            else:
                                if(TageswerteLesen > 9):
                                    curs.execute(TempSQLCommand[i])
                                    TageswerteLesen = 0
                except Exception as e:
                    print("SQL-DB-Error. Rolling back.")
                    print("Fehler beim Datensatz Einfügen (PV-Daten)")
                    strExcept = " 101 Fehler beim Datensatz Einfügen (Solardaten)\n"
                    strExcept += str(e)
                    WPConfig.logData(strExcept, FehlerLogFile)

            else:
                print("\n------------------------------------------------\n")

            #Ab hier Datenabruf von Wärmepumpe
            if(WPPause == 0):
                try:
                    DatenAbrufenZahl(General_Config["isgaddress"] + "?s=1,0", WPDaten_1, ' ')
                    DatenAbrufenZahl(General_Config["isgaddress"] + "?s=1,1", WPDaten_2, ' ')
                    Counter = Counter +2
                except Exception as e:
                    print ("Verbindungsproblem mit ISG: ", e)
                    strExcept = " 102 Verbindungsproblem mit ISG\n"
                    strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                    strExcept += str(e)
                    WPConfig.logData(strExcept, FehlerLogFile)
                    Fehler+=1
                    if(Fehler < 5):
                        WPPause = 2
                        continue
                    else:
                        WPPause = 300
                        continue
                    db.rollback()
                    db.close()
                    quit()

                TempSQLCommand = []
                x = 3
                tempString = ""
                try:
                    for list1, i in zip(StringSQL2, range(0, 8)):
                        if(i != 7):
                            TempSQLCommand.append(list1 + time.strftime("%Y%m%d%H%M00", time.localtime()))
                        else:
                            #für Tageswerte 000000 Uhrzeit als Zeitid eintragen
                            TempSQLCommand.append(list1 + time.strftime("%Y%m%d000000", time.localtime()))
                    for i in WPColumnData:
                        if(i[1]==0):
                            continue
                        #Terminal Anzeige erstellen
                        if((len(i[0])+len(tempString))>95):
                            print(StringVisualSpalten[x])
                            print(tempString)
                            tempString = ""
                            x +=1
                        var = "{:^"+ str(len(i[0])) +"} | "
                        if (i[1] < 5):
                            TempSQLCommand[i[1]-1]+= ", " + str(WPDaten_1[i[2]])
                            tempString += var.format(round((WPDaten_1[i[2]]), 2))
                        else:
                            TempSQLCommand[i[1]-1]+= ", " + str(WPDaten_2[i[2]])
                            tempString += var.format(round((WPDaten_2[i[2]]), 2))
                except Exception as e:
                    print("SQL-DB-Error. Rolling back.")
                    print("Fehler beim vorbereiten des SQL-Befehls")
                    strExcept = " 103 Fehler beim vorbereiten des SQL-Befehls\n"
                    strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                    strExcept += str(e)
                    WPConfig.logData(strExcept, FehlerLogFile)
                    Fehler+=1
                    if(Fehler < 5):
                        WPPause = 2
                        continue
                    else:
                        WPPause = 300
                        continue
                    db.rollback()
                    db.close()
                    quit()
                print(StringVisualSpalten[x])
                print(tempString)
                try:
                    if((WPDaten_1[WPColumnData[1][2]]==0)or(WPDaten_1[WPColumnData[4][2]]==0)or(WPDaten_1[WPColumnData[8][2]]==0)or(WPDaten_2[WPColumnData[22][2]]==0)or(WPDaten_2[WPColumnData[22][2]]==0)):
                        raise
                    
                    for list1, i in zip(TempSQLCommand, range(0, 8)):
                        if(i < 5): 
                            #print(TempSQLCommand[i])
                            curs.execute(list1 + ");")
                        if((i ==7)and(TageswertWPSchreiben)): # Die Stunden werden ab dem 26.12.2021 nur noch einmal Täglich geschrieben                            
                            curs.execute(list1 + ");")
                            TageswertWPSchreiben = 0
                        if(((i == 6)or(i==5))and(int(time.strftime("%M", time.localtime()))%10 == 0 )):
                            curs.execute(list1 + ");")
                except Exception as e:
                    print("SQL-DB-Error. Rolling back.")
                    print("Fehler beim Datensatz Einfügen oder Daten entsprechen nicht der Vorgabe")
                    #Folgende 5 Zeilen werden aktuell nicht mitgelogt
                    #datei.write(WPColumnData[1][0] + " = " + str(WPDaten_1[WPColumnData[1][2]]))
                    #datei.write(WPColumnData[4][0] + " = " + str(WPDaten_1[WPColumnData[4][2]]))
                    #datei.write(WPColumnData[8][0] + " = " + str(WPDaten_1[WPColumnData[8][2]]))
                    #datei.write(WPColumnData[22][0] + " = " + str(WPDaten_2[WPColumnData[22][2]]))
                    #datei.write(WPColumnData[22][0] + " = " + str(WPDaten_2[WPColumnData[22][2]]))
                    strExcept = " 104 Fehler beim Datensatz Einfügen (Anlage)\n"
                    strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                    strExcept += str(e)
                    WPConfig.logData(strExcept, FehlerLogFile)
                    Fehler+=1
                    if(Fehler < 5):
                        WPPause = 2
                        continue
                    else:
                        WPPause = 300
                        continue
                    db.rollback()
                    db.close()
                    quit()

                #Abrufen der Status-Seite
                try:
                    DatenAbrufen(General_Config["isgaddress"] + "?s=2,0", WPDaten_1, '<')
                    Counter = Counter +1
                except Exception as e:
                    print ("Verbindungsproblem mit ISG: ", e)
                    strExcept = " 105 Verbindungsproblem mit ISG\n"
                    strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                    strExcept += str(e)
                    WPConfig.logData(strExcept, FehlerLogFile)
                    Fehler+=1
                    if(Fehler < 5):
                        WPPause = 2
                        continue
                    else:
                        WPPause = 300
                        continue
                    db.close()
                    print (str(Counter))
                    quit()
                # Prüfen ob alle Einträge der Statusseite in der DB vorhanden sind, wenn nicht erstelle diesen        
                if existDBHeader(WPDaten_1, 0, strListStatus, curs):
                    StatuslisteAbrufen(curs, strListStatus)

                try:
                    if (len(WPDaten_1)>1):
                        curs.execute (makeSQLcommandStatus(WPDaten_1, strListStatus))
                        print(printStatus(WPDaten_1))
                        print(strListStatus)
                    else:
                        print("Keine Statusdaten vorhanden")
                        print(printStatus(WPDaten_1))
                        print(strListStatus)
                except Exception as e:
                    print("SQL-DB-Error. Rolling back.")
                    print("Fehler beim Datensatz Einfügen (Status)")
                    strExcept = " 106 Fehler beim Datensatz Einfügen (Status)\n"
                    strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                    strExcept += str(e)
                    WPConfig.logData(strExcept, FehlerLogFile)
                    Fehler+=1
                    if(Fehler < 5):
                        WPPause = 2
                        continue
                    else:
                        WPPause = 300
                        continue
                    db.rollback()
                    db.close()
                    quit() 
                #Einstellungen vom ISG abrufen
                # SQL-Anweisung für die ISG-Einstellungen vorbereiten
                # strSQLConfig = [] #Variable zum aufnehmen der aktuell letzten Daten in der SQL-DB

                if(EinstellungWPPruefen and (int(time.strftime("%H%M", time.localtime())) >= 130) and (Counter < 660)):
                    EinstellungWPPruefen = 0
                    WPConfigDaten = []
                    strCountConfig = 0
                    StringSQL3 = [] #WP-Einstellungen SQL-Anweisung vorderer Teil
                    TempSQLCommand = [] #WP-Einstellungen SQL-Anweisung hinterer Teil mit den Werten
                    Unterschiede = 0
                    try:
                        for i in StringSQLTabName2:
                            StringSQL3.append("INSERT INTO " + i + " (Zeit_id")
                            TempSQLCommand.append(time.strftime("%Y%m%d000000", time.localtime()))
                        Statusbalken = "#"
                        for i in WPConfigSites:
                            EinstellungenAbrufen(General_Config["isgaddress"] + i[1], WPConfigDaten, i[2])
                            print(Statusbalken)
                            Statusbalken += "#" 
                            Counter += 1
                    except Exception as e:
                        print ("Verbindungsproblem mit ISG: ", e)
                        strExcept = " 107 Verbindungsproblem mit ISG\n"
                        strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                        strExcept += str(e)
                        WPConfig.logData(strExcept, FehlerLogFile)
                        Fehler+=1
                        if(Fehler < 5):
                            WPPause = 2
                            continue
                        else:
                            WPPause = 300
                            continue
                        db.close()
                        print (str(Counter))
                        quit()
                    try:
                        for j in WPConfigDaten:
                            TempTime = 0
                            if(int(j[0]) == 52):
                                TempTime = int(time.strftime("%H", time.localtime())) 
                                if(TempTime != int(j[1])):
                                    WPConfig.SetWPConfig(j[0], TempTime)
                                    Counter += 1
                                    print(TempTime)
                                    print(j[1])
                                    WPConfig.logData("akt Zeit (h): " + str(TempTime) + " - WP Zeit (h)" + j[1], DataLogFile)
                                else:
                                    print(TempTime)
                                    print(j[1])
                            elif(int(j[0]) == 53):
                                TempTime = int(time.strftime("%M", time.localtime())) 
                                if(TempTime != int(j[1])):
                                    WPConfig.SetWPConfig(j[0], TempTime+1)
                                    Counter += 1
                                    print(TempTime)
                                    print(j[1])
                                    WPConfig.logData("akt Zeit (m): " + str(TempTime) + " - WP Zeit (m)" + j[1], DataLogFile)
                                else:
                                    print(TempTime)
                                    print(j[1])
                            else:
                                TempIndex = DatasetSearch(WPConfigColumn, int(j[0]), 0)
                                if(TempIndex == -1):
                                    raise ValueError('Gesuchter Wert konnte nicht gefunden werden', j[0], "TempIndex = DatasetSearch(WPConfigColumn, int(j[0]), 0)")
                                if(WPConfigColumn[TempIndex][2] < 3):
                                    StringSQL3[WPConfigColumn[TempIndex][2]] += ", " + WPConfigColumn[TempIndex][1]
                                    strCountConfig+=1
                                    if(WPConfigColumn[TempIndex][3]):
                                        TempSQLCommand[WPConfigColumn[TempIndex][2]] += ", " + str(float(j[1]))
                                        if(float(j[1])!= float(strSQLConfig[TempIndex])):
                                            Unterschiede+=1
                                    else:
                                        TempSQLCommand[WPConfigColumn[TempIndex][2]] += ", " + j[1]
                                        if(int(j[1])!= int(strSQLConfig[TempIndex])):
                                            Unterschiede+=1
                    except ValueError as err:
                        print(err.args)
                        strExcept = " 108 WPConfig-Daten\n"
                        strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                        strExcept += str(err.args)
                        WPConfig.logData(strExcept, FehlerLogFile)
                    try:
                        if(Unterschiede):
                            for i, j in zip(StringSQL3, TempSQLCommand):
                                i += ") VALUES ("
                                print(i + j + ");")
                                curs.execute(i + j + ");")
#                            for i in range(0,  3):
#                                StringSQL3[i] += ") VALUES ("
#                                print(StringSQL3[i] + TempSQLCommand[i] + ");")
#                                curs.execute(StringSQL3[i] + TempSQLCommand[i] + ");")
                            #Letzten Daten von SQL-DB abrufen
                            SQLEinstellungenAbrufen(curs, WPConfigColumn, StringSQLTabName2, strSQLConfig)
                        EinstellungWPCounter += 1
                    except Exception as e:
                        print("SQL-DB-Error. Rolling back.")
                        print("Fehler beim Abruf der Einstellungen")
                        strExcept = " 109 Fehler beim Abruf der Einstellungen\n"
                        strExcept += "Zählerstand Seitenaufrufe: " + str(Counter) + "\n"
                        strExcept += str(e)
                        WPConfig.logData(strExcept, FehlerLogFile)
                        Fehler+=1
                        if(Fehler < 5):
                            WPPause = 2
                            continue
                        else:
                            WPPause = 300
                            continue
                        db.rollback()
                        db.close()
                        quit() 


        
                if (Counter >= 680):
                    x = requests.post(General_Config["isgaddress"] + "reboot.php", data = myobj)
                    print(x)
                    Counter = 0
                    WPPause=3
                if (Fehler >0):
                    FehlerSumme += Fehler
                    Fehler = 0

                print ("Seitenaufrufe beim ISG: " + str(Counter)+ "  Fehler Summe:" + str(FehlerSumme) + "  Anfrangswert: " + str(Anfangswert)) 
                print(AktuelleLText, end = "\r")
            else:
                print("Wartezeit auf ISG noch " + str(WPPause) + "Minuten")  
                print ("Seitenaufrufe beim ISG: " + str(Counter)+ "  Fehler Summe:" + str(FehlerSumme) + "  Anfangswert: " + str(Anfangswert)) 
                WPPause-=1 
                print(AktuelleLText, end = "\r")
                print("-----------------------------------------")
        file_Test = Path(DateiJSON[1])
        if(file_Test.is_file()):
            TempData=[]
            sleep(0.5)
            with open(DateiJSON[1], "r") as read_file:
                TempData = json.load(read_file)
                read_file.close
            for i in range(0, len(TempData)-1):
                WPConfig.SetWPConfig(TempData[i][0], TempData[i][1])
                Counter+=1
        
            os.remove(DateiJSON[1])

 
    
