# OTUS-DE-Graduation-project

Система для обработки и хранения данных по поездкам байкшеринга 

airflow/
test_gobike_tripdata.py - DAG
test_raw_gobike_tripdata.py - task загрузки CSV в RAW
test_ods_gobike_tripdata.py - task загрузки в ODS
test_dm_station_gobike_tripdata.py - task загрузки измерения в DM
test_dm_trips_gobike_tripdata.py - task расчета агрегата в DM
test_dm_teradata_gobike_tripdata.py - task копирования витрины в Teradata
