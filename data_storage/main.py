# -------------------------- DATA STORAGE --------------------------#

import json
import mysql.connector

from confluent_kafka import Consumer

#MYSQL CONNECTION
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="datastorage"
)
#IMPOSTAZIONE CONSUMER E INSCRIZIONE AL TOPIC
c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    #'bootstrap.servers': 'localhost:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
})
c.subscribe(['promethuesdata'])

mycursor = mydb.cursor()

key_vector = []

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    #VERIFICO IL PRIMO FILE E POI INSERISCO NEL DB
    if str(msg.key()) == "b'etl_data_pipeline#1'":
        file_json_1 = json.loads(msg.value())
        key_vector.append(msg.key())

        #print(file_json_1)
        print("\n---> FILE 1 LOADED\n")

        sql = """INSERT INTO metadata (metric_name, autocorrelation, stationarity, seasonality) VALUES (%s,%s,%s,%s);"""

        for key in file_json_1:
            param1 = key
            param2 = str(file_json_1[key]["autocorrelazione"])
            param3 = str(file_json_1[key]["stazionarietà"])
            param4 = str(file_json_1[key]["stagionalità"])
            val = (param1, param2, param3, param4)

            mycursor.execute(sql, val)
            mydb.commit()

            #LE SEQUENZE COMMENTATE DI QUESTO TIPO MI SONO SERVITE PER VERIFICARE CHE I PARAMETRI PASSATI FOSSERO CORRETTI
            '''
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM metadata;")

            for x in mycursor:
                print(x)'''

    # VERIFICO IL SECONDO FILE E POI INSERISCO NEL DB
    elif str(msg.key()) == "b'etl_data_pipeline#2'":
        key_vector.append(msg.key())

        file_json_2 = json.loads(msg.value())
        #print(file_json_2)
        print("---> FILE 2 LOADED\n")

        sql = """INSERT INTO metrics (metric_info, max, min, average, std) VALUES (%s,%s,%s,%s,%s);"""

        for key in file_json_2:
            param1 = key
            param2 = file_json_2[key]["max"]
            param3 = file_json_2[key]["min"]
            param4 = file_json_2[key]["avg"]
            param5 = file_json_2[key]["std"]
            val = (param1, param2, param3, param4, param5)

            mycursor.execute(sql, val)
            mydb.commit()

            ''' 
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM metrics;")

        for x in mycursor:
                print(x)'''

    # VERIFICO IL TERZO FILE E POI INSERISCO NEL DB
    elif str(msg.key()) == "b'etl_data_pipeline#3'":
        file_json_3 = json.loads(msg.value())
        key_vector.append(msg.key())

        #print(file_json_3)
        print("---> FILE 3 LOADED\n")

        sql = """INSERT INTO prediction (metric_name, max, min, average) VALUES (%s,%s,%s,%s);"""

        for key in file_json_3:
            param1 = key
            param2 = file_json_3[key]["max"]
            param3 = file_json_3[key]["min"]
            param4 = file_json_3[key]["avg"]
            val = (param1, param2, param3, param4)

            mycursor.execute(sql, val)
            mydb.commit()

        '''
            print(mycursor.rowcount, "was inserted.")
            mycursor.execute("SELECT * FROM prediction;")

            for x in mycursor:
                print(x)'''

c.close()