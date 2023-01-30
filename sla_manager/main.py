# -------------------------- SLA MANAGER --------------------------#

from prometheus_api_client import MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from flask import Flask

import ETL_D_P_main
import DR_main

app = Flask(__name__)

#FUNZIOME PER IL CALCOLO DELLE VIOLAZIONI
def past_violations():
    for key in range(0, len(metrics_sla)):
        num_violations = 0
        for h in range(0, len(start_time)):
            metric_data, t = ETL_D_P_main.setting_parameters(DR_main.get_all_metrics().get(key),
                                                                       label_config, parse_datetime(start_time[h]),
                                                                       end_time, chunk_size)

            metric_df = MetricRangeDataFrame(metric_data)
            for k in metric_df['value']:
                if not int(SLA_result[DR_main.get_all_metrics().get(key)]['min']) < k < int(
                        SLA_result[DR_main.get_all_metrics().get(key)]['max']):
                    num_violations = num_violations + 1

            print("\n\nVIOLATIONS BY METRIC: " + DR_main.get_all_metrics().get(key) + " per: " + str(
                start_time[h]) + " --> " + str(num_violations))

            if num_violations > 0:
                state[DR_main.get_all_metrics().get(key)] = "VIOLATED"
            else:
                state[DR_main.get_all_metrics().get(key)] = "NOT VIOLATED"

            violations[DR_main.get_all_metrics().get(key)+","+ start_time[h]] = num_violations

    return violations

#FUNZIONE PER IL CALCOLO DI POSSIBILI VIOLAZIONI NEL FUTURO
def fut_violations():
    for key in range(0, len(metrics_sla)):
        num_violations = 0

        metric_data, t = ETL_D_P_main.setting_parameters(DR_main.get_all_metrics().get(key),
                                                                   label_config, parse_datetime('1h'),
                                                                   end_time, chunk_size)
        metric_df = MetricRangeDataFrame(metric_data)
        num_samples = 5
        data = metric_df.resample(rule='10s').mean(numeric_only='True')
        tsmodel = ExponentialSmoothing(data.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
        prediction = tsmodel.forecast(steps=round((10 * 60) / num_samples))
        for k in prediction.values:
            if not SLA_result[DR_main.get_all_metrics().get(key)]['min'] < k \
                   < SLA_result[DR_main.get_all_metrics().get(key)]['max']: num_violations = num_violations + 1

        print("\n\nPOSSIBLE VIOLATIONS BY METRIC: " + DR_main.get_all_metrics().get(key) + " --> " + str(num_violations))

        future_violations[DR_main.get_all_metrics().get(key)] = {num_violations}

    return future_violations

#REST API
@app.get('/')
def Hello():
    return 'HELLO FROM SLA MANAGER'

@app.get('/state')
def get_state():
    return 'STATE: '+ str(state)

@app.get('/future_violations')
def get_future_violations():
    return  'POSSIBLE VIOLATIONS: '+ str(future_violations)

@app.get('/violations')
def get_violations():
    return 'VIOLATIONS: ' + str(violations)


if __name__ == "__main__":

    label_config = {'job': 'ceph-metrics'}
    start_time = ["1h", "3h", "12h"]
    start_time_range = parse_datetime("48h")
    #start_time = ["10m", "20m", "30m"]
    #start_time_range = parse_datetime("1h")
    end_time = parse_datetime("now")
    chunk_size = timedelta(minutes=1)
    metrics_sla = []
    SLA_result = {}
    result_predict = {}
    future_violations={}
    violations={}
    state={}

    print("\n --- METRICS LIST ---\n")

    #ASSEGNAZIONE DI UN CODICE PER OGNI METRICA
    for key in DR_main.get_all_metrics():
        print(str(key) + " - " + DR_main.get_all_metrics().get(key) + "\n")


    #CONTROLLO SULL'ESISTENZA DEL CODICE DELLA METRICA
    for key in range(0, 4):
        print("CHOOSE 5 METRICS TO ANALYZE\n")
        metric = input('\nSELECT METRIC --> ')
        print("\n")
        if metric not in metrics_sla:
            metrics_sla.append(metric)

    print(metrics_sla)

    #IMPOSTAZIONE RANGE DI MASSIMO E MINIMO
    for key in range(0, len(metrics_sla)):
        print(DR_main.get_all_metrics().get(key))
        print("\n")
        x = input('DO YOU WANT TO ENTER MAXIMUM AND MINIMUM ? s/n \n')
        if x == 's':
            min = input("INSERT MAX: ")
            max = input("INSERT MIN: ")
            SLA_result[DR_main.get_all_metrics().get(key)] = {'min': min, 'max': max}
        else:
            metric_data, t = ETL_D_P_main.setting_parameters(DR_main.get_all_metrics().get(key),
                                                                       label_config, start_time_range,
                                                                       end_time, chunk_size)
            metric_df = MetricRangeDataFrame(metric_data)
            SLA_result[DR_main.get_all_metrics().get(key)] = {'min': metric_df['value'].min(),
                                                                           'max': metric_df['value'].max()}

        print(SLA_result)

        #INVOCAZIONE FUNZIONE VIOLAZIONI
        violations = past_violations()

        #INVOCAZIONE FUNZIONE POSSIBILI VIOLAZIONI FUTURE
        future_violations = fut_violations()

    app.run(port=5004, debug=False)