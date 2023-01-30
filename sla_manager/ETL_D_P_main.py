# -------------------------- ETL DATA PIPELINE --------------------------#

from prometheus_api_client import PrometheusConnect, MetricsList, MetricRangeDataFrame
import time

# MI CONNETTO ALL'EXPORTER PROMETHEUS
prom = PrometheusConnect(url="http://15.160.61.227:29090/", disable_ssl=True)

# FUNZIONE IMPOSTAZIONE DEI PARAMETRI
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



