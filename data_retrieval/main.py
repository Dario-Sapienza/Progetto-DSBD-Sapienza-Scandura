# -------------------------- DATA RETRIEVAL --------------------------#

from flask import Flask
import mysql.connector

# MYSQL CONNECTION
mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    database="DataStorage"
)

mycursor = mydb.cursor()

app = Flask(__name__)

#REST API
@app.get('/values')
def get_values_metrics():
    sql = """SELECT * from metrics;"""

    mycursor.execute(sql)
    values_list = {}
    metrics_list = []
    y = 0
    for x in mycursor:
        # print(x)
        metrics_list.append(x)
        max_metric = metrics_list[y][1]
        min_metric = metrics_list[y][2]
        avg_metric = metrics_list[y][3]
        std_metric = metrics_list[y][4]
        values_list[metrics_list[y][0]] = {"max": max_metric, "min": min_metric, "avg": avg_metric,
                                                "std": std_metric}
        y = y + 1

    return values_list


@app.get('/metadata')
def get_values_metadata():
    sql = """SELECT * from metadata;"""

    mycursor.execute(sql)
    metadata_list = {}
    metric_list = []
    y = 0
    for x in mycursor:

        metric_list.append(x)
        autocorrelazione = metric_list[y][1]
        stazionarietà = metric_list[y][2]
        stagionalità = metric_list[y][3]

        metadata_list[metric_list[y][0]] = {"AUTOCORRELAZIONE": autocorrelazione,
                                                "STAZIONARIETA'": stazionarietà,
                                                "STAGIONALITA'": stagionalità}
        y = y + 1

    return metadata_list

@app.get('/predicts')
def get_values_predict():
    sql = """SELECT * from prediction;"""

    mycursor.execute(sql)
    predicts_list = {}
    metric_list = []
    y = 0
    for x in mycursor:

        metric_list.append(x)
        max_metric = metric_list[y][1]
        min_metric = metric_list[y][2]
        avg_metric = metric_list[y][3]
        predicts_list[metric_list[y][0]] = {"max": max_metric, "min": min_metric, "avg": avg_metric}
        y = y + 1

    return predicts_list

@app.get('/')
def hello():
    return 'HELLO FROM DATA RETRIEVAL'


@app.get('/all_metrics')
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

if __name__ == "__main__":

    app.run(port=5002, debug=False)
