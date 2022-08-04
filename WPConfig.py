import requests
import time
from enum import Enum

 
class WP_Variablen(Enum):
    LStufeTag = 82      #Lüftungsstufen 0 - 3
    LStufeNacht = 83
    LStufeBereit = 84
    LStufeParty = 85
    LStufeHand = 188
    LVolZu1 = 91  #Volumenstrom Zuluft
    LVolZu2 = 92
    LVolZu3 = 93
    LVolAb1 = 94  #Volumenstrom Abluft
    LVolAb2 = 95
    LVolAb3 = 96
    LZeitAuss0 = 86     #Lüftung Aussetzen Zeit in Minuten
    LZeitAuss1 = 87
    LZeitAuss2 = 88
    LZeitAuss3 = 89
    HK1RaumTTag = 5     #Raumtemp. HK1
    HK1RaumTNacht = 7
    HK1RaumTBereit = 58
    HK1SollHand = 54    #Heizkreistemp. HK1
    HK2RaumTTag = 6     #Raumtemp. HK2
    HK2RaumTNacht = 8
    HK2RaumTBereit = 59
    HK2SollHand = 55    #Heizkreistemp. HK2
    HK1Steigung = 35    #Heizkurve HK1
    HK1Fusspunkt = 16
    HK1Raumeinfluss = 37
    HK1AntVorlauf = 127
    HK1TempSollMax = 22
    HK1TempSollMin = 56
    HK2Steigung = 36    #Heizkurve HK2
    HK2Fusspunkt = 129
    HK2Raumeinfluss = 38
    HK2TempSollMax = 23
    HK2TempSollMin = 57
    HIntAnt = 61      #Heizen Grundeinstellungen
    HMaxStufen = 130
    HMaxVorlaufTemp = 21
    HSommerbetrieb = 40
    HHystSoWi = 133
    HDaempfAussenTemp = 34
    HBivalenz = 64
    HZeitsperreNE = 131
    HKorrekturAT = 134
    HMaskZeitTempMessung = 187
    WWSollTag = 17        #Warmwasser Temperaturen
    WWSollNacht = 161
    WWSollBereit = 102
    WWSollHand = 101
    WWHysterese = 60          #Warmwasser Grundeinstellungen
    WWZeitsperreNE = 111
    WWTempFreigabeNE = 112
    WWIntervAntilegionellen = 109
    WWMaxDauerWWErzeug = 62
    WWAntilegTemp = 110
    WWNEStufe = 113
    WWPufferbetrieb = 114 #Werte (1 = Ein, 0 = Aus)
    WWMaxVorlaufTemp = 115
    WWECO = 116 #Werte (1 = Ein, 0 = Aus)
    SolFreigabe = 75      #Solar Einstellungen Werte (1 = Ein, 0 = Aus)
    SolTempDifferenz = 65
    SolVerzVerdichterWW = 117
    SolWWTemp = 160
    SolHysterese = 118
    SolKolGrenzTemp = 181
    SolKolSchutzTemp = 182
    SolKolSperrTemp = 183
    SolKolSchutz = 180 #Werte (1 = Ein, 0 = Aus)
    FMVerdTempAbtEnde = 149        #Fachmann Verdampfereinstellungen
    FMVerdMaxAbtDauer = 208
    FMVerdEinfrierschutzNE = 150
    FMVerdEinfrierschutzAbtauAbr = 151
    FMKAVerdTaktung = 152          #Fachmann Kälteaggregat
    FMKADrehzFortluft = 153
    FMPZMinZyklen = 155            #Fachmann Pumpenzyklen Einstellungen
    FMPZMaxZyklen = 154
    FMPZATMinZyklen = 157
    FMPZATMaxZyklen = 156
    ProgPartyStartSt = 119      #Partyprogramm [{"name":"119","value":"01"},{"name":"119","value":"15"},{"name":"119","value":"03"},{"name":"119","value":"30"}]
    ProgPartyStartMin = 119                   #[{"name":"119","value":"hEin"},{"name":"119","value":"MinEin"},{"name":"119","value":"hAus"},{"name":"119","value":"MinAus"}]
    ProgPartyEndeSt = 119
    ProgPartyEndeMin = 119
    

def logData(Daten, Datei):
    with open(Datei, "a") as file:
        file.write(time.strftime("\n%Y-%m-%d %H:%M:%S - ", time.localtime()) + Daten + "\n")
        file.close


def SetWPConfig(Register, Value):
    myobj2 = {'data' : '[{"name":"val'+ str(Register) +'","value":"' + str(Value) + '"}]'}
    x = requests.post("http://192.168.178.5/save.php", data = myobj2)
    logData("WP-Config change - Register: " + str(Register) + " - Wert: " + str(Value), '/home/pi/share/Data.log')
    print(x)
