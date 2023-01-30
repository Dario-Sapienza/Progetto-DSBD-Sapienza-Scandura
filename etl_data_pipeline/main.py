# -------------------------- ETL DATA PIPELINE --------------------------#

from prometheus_api_client import PrometheusConnect, MetricsList, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime
from flask import Flask
from confluent_kafka import Producer
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.seasonal import seasonal_decompose

import json
import sys
import time
import statsmodels.api as sm
#import xlsxwriter

app = Flask(__name__)

#MI CONNETTO ALL'EXPORTER PROMETHEUS
prom = PrometheusConnect(url="http://15.160.61.227:29090/", disable_ssl=True)

#FUNZIONE IMPOSTAZIONE DEI PARAMETRI
def setting_parameters(metric_name, label_config, start_time_metadata, end_time, chunk_size):
    start = time.time()
    metric_data = prom.get_metric_range_data(
        metric_name=metric_name,
        label_config=label_config,
        start_time=start_time_metadata,
        end_time=end_time,
        chunk_size=chunk_size,
    )
    end = time.time()
    return metric_data, (end - start)

#QUESTA FUNZIONE E' PENSATA PER LA CREAZIONE DI UN EVENTUALE FILE CSV PER L'ANALISI DEI GRAFICI TRAMITE JUPYTER
'''def creating_file_csv(metric_name, metric_object_list):
    # scriviamo il timestamp ed il value in un file csv
    xlxsname = metric_name + str('.xlsx')
    csvname = metric_name + str('.csv')

    workbook = xlsxwriter.Workbook(xlxsname)
    worksheet = workbook.add_worksheet()
    row = 0
    col = 0
    format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss.ms'})

    for item in metric_object_list.metric_values.ds:
        worksheet.write(row, col, item, format)
        row += 1

    row = 0

    for item in metric_object_list.metric_values.y:
        worksheet.write(row, col + 1, item)
        row += 1

    workbook.close()

    read_file = pd.read_excel(xlxsname)
    read_file.to_csv(csvname, index=None)

    return'''

#FUNZIONE PER LA COMUNICAZIONE KAFKA COL DATASTORAGE
# FILE SONO PASSATI COME JSON
def kafka_producer(file, key):
    #broker = "localhost:29092"
    broker = "kafka:9092"
    topic = "promethuesdata"
    conf = {'bootstrap.servers': broker}

    #---- Crea l'istanza Produttore ----#
    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    #---- Legge le linee dalla stdin, produce ogni linea a Kafka ----#
    try:
        record_key = key
        record_value = json.dumps(file)
        #print("Producing record: {}\t{}".format(record_key, record_value))
        print("\n---> RECORD PRODUCED\n")
        p.produce(topic, key=record_key, value=record_value, callback=delivery_callback)

    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(p))
    p.poll(0)

    # Aspetta finche tutti i messaggi sono stati consegnati
    sys.stderr.write('%% Waiting for %d deliveries\n,' % len(p))
    p.flush()

#FUNZIONE CHE RICHIAMA I METODI PER IL CALCOLO DI AUTOCORRELAZIONE, STAZIONARIETA' E STAGIONALITA' E CHE GENERA UNA LISTA DI TALI VALORI
def calculate_values_1(metric_df):
    list = {}
    list = {"autocorrelazione": autocorrelation(metric_df['value']),
            "stazionarietà": stationarity(metric_df['value']),
            "stagionalità": seasonal(metric_df['value'])}

    return list

#FUNZIONE PER IL CALCOLO DI MASSIMO, MINIMO, MEDIA E DEVIAZIONE STANDARD
def calculate_values_2(metric_name, metric_df, start_time):
    # Calcoliamo il valore di massimo, minimo e media
    max = metric_df['value'].max()
    min = metric_df['value'].min()
    avg = metric_df['value'].mean()
    std = metric_df['value'].std()

    result_2[str(metric_name + "," + str(start_time))] = {"max": max, "min": min,
                                                          "avg": avg, "std": std}

    return result_2

#FUNZIONE PER IL CALCOLO DELLA STAZIONARIETA'
def stationarity(values):

    stationarity_test = adfuller(values, autolag='AIC')

    if stationarity_test[1] <= 0.05:
        print("[P-VALUE]:", stationarity_test[1])
        stationarity_result = 'serie stazionaria'
    else:
        print("[P-VALUE]:", stationarity_test[1])
        stationarity_result = 'serie non stazionaria'

    return stationarity_result

#FUNZIONE PER IL CALCOLO DELLA STAGIONALITA'
def seasonal(values):
    result_seasonal = seasonal_decompose(values, model='additive', period=5)
    seasonal_result = {str(k): v for k, v in result_seasonal.seasonal.to_dict().items()}

    return seasonal_result

#FUNZIONE PER IL CALCOLO DELL' AUTOCORRELAZIONE'
def autocorrelation(values):
    lags = len(values)
    result_autocorr = sm.tsa.acf(values, nlags=lags - 1).tolist()

    print("[LAGS]:", lags)

    return result_autocorr


#FUNZIONE PER IL CALCOLO DELLA PREDIZIONE
def predict(metric_name, metric_df):

    dictPred = {}

    data = metric_df.resample(rule='10s').mean(numeric_only='True')
    tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
    prediction = tsmodel.forecast(6) # numero di valori per 10min

    dictPred = {"max": prediction.max(), "min": prediction.min(), "avg": prediction.mean()}

    return dictPred

#FUNZIONE PER LA SCELTA DELLE METRICHE
def get_metrics():
    metric_list = []
    not_zero_metrics = []

    metric_data = prom.get_metric_range_data(
        metric_name='',
        label_config={'job': 'ceph-metrics'}
    )
    print("\nTOTAL METRICS: ", len(metric_data))
    for metric in metric_data:
        for p1,p2 in metric["values"]:
            if p2 != '0' and metric["metric"]["__name__"] not in not_zero_metrics:
                not_zero_metrics.append(metric["metric"]["__name__"])

    print("METRICS WITH VALUE !=0 : ",len(not_zero_metrics))
    # algoritmo scelta metriche
    for k in range(0,20):
        metric_list.append(not_zero_metrics[k])

    return metric_list

#REST API
@app.get('/')
def Hello():
    return 'HELLO FROM ETL DATA PIPELINE'

@app.get('/t_time')
def get_TT():
    return 'TOTAL TIME:' + str(total_time)

@app.get('/p_time')
def get_PT():
    return 'PARTIAL TIME:' + str(partial_time)


if __name__ == "__main__":

    partial_time = {}
    total_time = {}
    result_1 = {}
    result_2 = {}
    result_predict = {}
    #metric_list_pred = ['available', 'drive_capacity_total', 'drive_read_bytes', 'drive_read_latency', 'drive_temperature']
    label_config = {'job': 'ceph-metrics'}
    #start_time = ["10m", "20m", "30m"]
    #start_time_metadata = parse_datetime("1h")
    start_time = ["1h", "3h", "12h"]
    start_time_metadata = parse_datetime("48h")
    end_time = parse_datetime("now")
    chunk_size = timedelta(minutes=1)

    metric_list = get_metrics()
    print("\n\t\t\t\t\t\t\t---> METRICS ANALYZED <---\n")
    print(metric_list)
    print("\n\n --> PARAMETERS SET <--")

    for name in range(0, len(metric_list)):

        #----- Settiamo la metrica con i dati configurati -----
        metric_data, exec_time = setting_parameters(metric_list[name], label_config, start_time_metadata, end_time, chunk_size)
        total_time[metric_list[name]] = {"time ": str(exec_time) + " s"}

        #print(metric_data)
        metric_object_list = MetricsList(metric_data)

        #----- VEDO L'ANDAMENTO DELLE METRICHE -----
        metric_df = MetricRangeDataFrame(metric_data)


        # ----- MI CREO I FILE CSV CON TIMESTAMP e VAlUE -----#
        #creating_file_csv(metric_list[name], metric_object_list[0])


        print("\n")
        print("[" + metric_list[name] + "]")

        #----- INVOCO LA FUNZIONE PER IL CALCOLO DI AUTOC., STAG. E STAZ. -----
        result_1[metric_list[name]] = calculate_values_1(metric_df)

        #print(result)

        # ------- CALCOLO IL VALORE DI MAX, MIN, AVG E DEV_STD DELLE METRICHE PER 1H, 3H, 12H -------#
        for h in range(0, len(start_time)):
            metric_data, part_exec_time = setting_parameters(metric_list[name], label_config, parse_datetime(start_time[h]), end_time, chunk_size)

            partial_time[metric_list[name] + "," + start_time[h]] = {str(part_exec_time) + " s"}

            metric_object_list = MetricsList(metric_data)
            my_metric_object = metric_object_list[0]
            
            metric_df = MetricRangeDataFrame(metric_data)

            result_2 = calculate_values_2(metric_list[name], metric_df, start_time[h])

    print("\n[TOTAL TIME]:" + str(total_time) + "\n")
    print("[PARTIAL TIME]:" + str(partial_time) + " \n")

    for name in range(0, len(metric_list)):
        metric_data, exec_time = setting_parameters(metric_list[name], label_config, start_time_metadata, end_time, chunk_size)

        metric_df = MetricRangeDataFrame(metric_data)

        result_predict[metric_list[name]] = predict(metric_list[name], metric_df)

    print("\nRESULT PREDICT ---> ", result_predict)

#INVOCO LA FUNZIONE PER LA COMUNICAZIONE CON KAFKA
    kafka_producer(result_1, 'etl_data_pipeline#1')
    kafka_producer(result_2, 'etl_data_pipeline#2')
    kafka_producer(result_predict, 'etl_data_pipeline#3')

    app.run(port=5003, debug=False)
