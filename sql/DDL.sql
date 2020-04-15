-- таблица логов сеансов ETL
create table test_sys_etl_session (
  etl_session_id string comment 'ID сеанса загрузки',
  start_time timestamp comment 'Отметка времени начала сеанса',
  table_name string comment 'Имя загружаемой таблицы',
  file_name string comment 'Имя файла',
  num_records int comment 'Кол-во загруженных записей'
)
comment 'Лог сеансов загрузки'
stored as orc; 

-- таблица поездок слоя ODS
create table test_ods_gobike_tripdata (
  start_station_id int comment 'ID станции начала поездки',
  start_station_name string comment 'Наименование станции начала поездки',
  start_station_latitude decimal(12, 7) comment 'Широта станции начала поездки',
  start_station_longitude decimal(12, 7) comment 'Долгота станции начала поездки',
  end_station_id int comment 'ID станции конца поездки',
  end_station_name string comment 'Наименование станции конца поездки',
  end_station_latitude decimal(12, 7) comment 'Широта станции конца поездки',
  end_station_longitude  decimal(12, 7) comment 'Долгота станции конца поездки',
  bike_id int comment 'ID велосипеда',
  start_time timestamp comment 'Отметка времени начала поездки',
  end_time timestamp comment 'Отметка времени конца поездки',
  user_type string comment 'Код типа пользователя',
  duration_sec int comment 'Продолжительность поездки',
  hash int comment 'Hash ключевых полей',
  etl_session_id int comment 'ID сеанса загрузки',
  load_ts timestamp comment 'Отметка времени загрузки'
)
comment 'Таблица слоя ODS c данными поездок'
partitioned by (business_dt date)
stored as orc;

-- таблица измерения станций слоя DM
create table test_dm_gobike_station_dim (
  station_id int comment 'ID станции',
  station_name string comment 'Наименование станции',
  station_latitude decimal(12, 7) comment 'Широта станции',
  station_longitude decimal(12, 7) comment 'Долгота станции',
  etl_session_id int comment 'ID сеанса загрузки',
  load_ts timestamp comment 'Отметка времени загрузки'
)
comment 'Измерение станций gobike'
stored as orc;

-- таблица агрегата поездок слоя DM 
create table test_dm_gobike_trips_daily_agg (
  start_station_id int comment 'ID станции начала поездок',
  end_station_id int comment 'ID станции конца поездок',
  user_type string comment 'Код типа пользователя',
  trips_date as date comment 'Дата поездок',
  trips_count as int comment 'Количество поездок',
  trips_duration as int comment 'Продолжительность поездок',
  etl_session_id int comment 'ID сеанса загрузки',
  load_ts timestamp comment 'Отметка времени загрузки'
)
comment 'Витрина - агрегат поездок по дням'
stored as orc;

