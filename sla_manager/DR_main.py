# -------------------------- DATA RETRIEVAL --------------------------#

import mysql.connector

# MYSQL CONNECTION
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    database="DataStorage"
)

mycursor = mydb.cursor()

def get_all_metrics():
    sql = """SELECT * from metadata;"""
    # print(sql)
    mycursor.execute(sql)
    all_metrics = {}
    metric_list = []
    y = 0
    for x in mycursor:
        # print(x)
        metric_list.append(x)
        all_metrics[y] = metric_list[y][0]
        y = y + 1

    return all_metrics
