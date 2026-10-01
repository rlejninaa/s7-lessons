"""geo_marts.py
Проектная работа Data Lake для соцсети - геоаналитика.
Строит три витрины на основе событий с геоданными:
- user_mart     — витрина пользователей,
- zone_mart     — витрина зон,
- friend_mart   — витрина рекомендаций друзей.
Скрипт параметризован: один и тот же код запускается в sandbox-контуре
(geo_data_d3_test) и в analytics-контуре (analytics/geo_data_d{depth}).
"""

import sys
import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


def main():
    if len(sys.argv) < 6:
        print(
            "Usage: geo_marts.py "
            "<date> <depth> <events_base_path> <geo_base_path> <output_base_path>",
            file=sys.stderr,
        )
        sys.exit(1)
   
    date = sys.argv[1] # '2022-01-30'
    depth = int(sys.argv[2]) # '3'
    events_base_path = sys.argv[3] #'/user/s31894037/data/geo/events' 
    geo_base_path = sys.argv[4] # '/user/s31894037/data/geo' (Добавить /geo.csv)
    output_base_path = sys.argv[5] # '/user/s31894037/data/geo_data_d3_test' ( И '/user/s31894037/data/analytics/geo_data_d30')

    spark = SparkSession.builder.appName(f"GeoMartsJob-{date}-{depth}").getOrCreate()
    # Справочник городов
    geo = prepare_geo(spark, geo_base_path)
    #geo.show(5)
    
    # Читаем события
    events = spark.read.parquet(
        *input_event_paths(events_base_path, date, depth)
    )
    #events.show(9)

    ### Расчет витрины пользователей

    # Определяем город для каждого сообщения
    messages_with_city = add_city_to_messages(events, geo)
    print(f"[main] messages_with_city: {messages_with_city.count()}")

    # Витрина пользователей
    user_mart = build_user_mart(spark, messages_with_city, depth)
    print(f"[main] user_mart: {user_mart.count()}")
    #user_mart.printSchema()
    #user_mart.show(10, truncate=False)
    
    # Запись в HDFS
    user_mart_path = f"{output_base_path}/user_mart"
    user_mart.write.mode("overwrite").parquet(user_mart_path)
    print(f"[main] Записал {user_mart_path}")


    ### Расчет витрины зон

    events_with_coords = add_coords_to_all_events(events)
    print(f"[main] events_with_coords: {events_with_coords.count()}")
    
    events_with_zone = add_zone_id_to_events(events_with_coords, geo).cache()
    _ = events_with_zone.count()
    print(f"[main] events_with_zone: {_}")
    events_with_zone.show(10, truncate=False)
    
    # Витрина зон
    zone_mart = build_zone_mart(events_with_zone)
    print(f"[main] zone_mart: {zone_mart.count()}")
    #zone_mart.printSchema()
    #zone_mart.show(10, truncate=False)

    # Запись в HDFS
    zone_mart_path = f"{output_base_path}/zone_mart"
    zone_mart.write.mode("overwrite").parquet(zone_mart_path)
    print(f"[main] Записал {zone_mart_path}")

    
    ### Витрина для рекомендации друзей 

    friend_mart = build_friend_mart(events, messages_with_city, geo, depth)
    friend_mart.show(10, truncate=False)

    # Запись в HDFS
    friend_mart_path = f"{output_base_path}/friend_mart"
    friend_mart.write.mode("overwrite").parquet(friend_mart_path)
    print(f"[main] Записал {friend_mart_path}")
    return


def input_event_paths(base_path, date, depth):
    """Возвращает список путей к партициям событий за окно [date-depth+1; date].
    Аргументы:
        base_path — корень с партициями date=YYYY-MM-DD;
        date      — дата отсчёта (конец диапазона), строка YYYY-MM-DD;
        depth     — сколько дней вглубь истории (int).
    Возвращает:
        list[str] — пути к партициям, по одному на каждый день.
    """
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [
        f"{base_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(depth)
    ]


def prepare_geo(spark, geo_base_path):
    """Читает geo.csv, приводит lat/lng к double, добавляет timezone.

    Если geo.parquet уже существует — возвращает готовый DataFrame без пересчёта.
    Аргументы:
        spark         — SparkSession;
        geo_base_path — путь к папке со справочником в HDFS.
    Возвращает:
        DataFrame с колонками id, city, lat, lng, timezone.
    """
    geo_csv_path = f"{geo_base_path}/geo.csv"
    geo_parquet_path = f"{geo_base_path}/geo.parquet"

    # Пытаемся прочитать уже готовый parquet
    try:
        geo = spark.read.parquet(geo_parquet_path)
        print(f"[prepare_geo] Использую существующий {geo_parquet_path}")
        return geo
    except Exception:
        print(f"[prepare_geo] Паркет не найден, строю из {geo_csv_path}")

    # Читаем csv (разделитель ";", десятичная запятая)
    geo = spark.read.csv(
        geo_csv_path,
        header=True,
        inferSchema=True,
        sep=";"
    )

    # Приводим lat/lng к double (заменяем "," на ".")
    geo = geo.withColumn(
        "lat",
        F.regexp_replace(F.col("lat"), ",", ".").cast("double")
    ).withColumn(
        "lng",
        F.regexp_replace(F.col("lng"), ",", ".").cast("double")
    )

    # Добавляем timezone через маппинг по городам
    geo = geo.withColumn(
        "timezone",
        F.when(F.trim(F.col("city")).isin("Perth", "Bunbury"), "Australia/Perth")
         .when(F.trim(F.col("city")) == "Darwin", "Australia/Darwin")
         .when(F.trim(F.col("city")) == "Adelaide", "Australia/Adelaide")
         .otherwise("Australia/Sydney")
    )

    # Пишем паркет одним файлом, без партиций
    geo.write.mode("overwrite").parquet(geo_parquet_path)
    print(f"[prepare_geo] Записал {geo_parquet_path}")

    return geo

def haversine(lat1, lon1, lat2, lon2):
    """Расстояние между точками (lat1, lon1) и (lat2, lon2) по формуле гаверсинуса.    
    Возвращает расстояние в км.
    """
    r = 6371  # радиус Земли в км
    
    lat1_rad = F.radians(lat1)
    lat2_rad = F.radians(lat2)
    lon1_rad = F.radians(lon1)
    lon2_rad = F.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = F.pow(F.sin(dlat / 2), 2) + \
        F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlon / 2), 2)
    
    return F.lit(2 * r) * F.asin(F.sqrt(a))

def add_city_to_messages(events, geo):
    """Определяет ближайший город для каждого сообщения.    
    Аргументы:
        events — DataFrame событий (используются только event_type = 'message');
        geo    — DataFrame справочника городов (id, city, lat, lng, timezone).    
    Возвращает:
        DataFrame с колонками: user_id, event_time, city, timezone, msg_lat, msg_lon.
    """
    
    # Оставляем только сообщения, вычисляем event_time и переименовываем координаты
    messages = events.where("event_type = 'message'").select(
        F.col("event.message_from").alias("user_id"),
        F.col("lat").alias("msg_lat"),
        F.col("lon").alias("msg_lon"),
        F.coalesce(
            F.to_timestamp("event.datetime", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("event.message_ts") + F.expr("INTERVAL 1 YEAR")
        ).alias("event_time"),
    )
    
    # Переименовываем колонки справочника
    geo_renamed = geo.select(
        F.col("id").alias("city_id"),
        F.col("city").alias("city"),
        F.col("timezone").alias("timezone"),
        F.col("lat").alias("city_lat"),
        F.col("lng").alias("city_lng"),
    )
    
    # Каждое сообщение × каждый город
    joined = messages.crossJoin(geo_renamed)
    
    # Считаем расстояние по формуле гаверсинуса
    joined = joined.withColumn(
        "distance_km",
        haversine(
            F.col("msg_lat"), F.col("msg_lon"),
            F.col("city_lat"), F.col("city_lng")
        )
    )
    
    # Для каждого сообщения оставляем ближайший город
    # partitionBy по (user_id, event_time, msg_lat, msg_lon), чтобы не потерять сообщения с разными координатами в один момент
    window = Window.partitionBy(
        "user_id", "event_time", "msg_lat", "msg_lon"
    ).orderBy("distance_km")
    
    nearest = (
        joined
        .withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .select(
            "user_id", "event_time", "city_id", "city", 
            "timezone", "msg_lat", "msg_lon"
        )
    )
    
    return nearest

def add_act_city(messages_with_city):
    """Определяет act_city — город последнего сообщения пользователя.    
    Аргументы:
        messages_with_city — DataFrame из add_city_to_messages.    
    Возвращает:
        DataFrame с колонками: user_id, act_city.
    """
    
    window = Window.partitionBy("user_id").orderBy(F.desc("event_time"))
    
    act_city = (
        messages_with_city
        .withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .select(
            "user_id",
            F.col("city").alias("act_city"),
        )
    )
    
    return act_city

def add_travel(messages_with_city):
    """Определяет travel_array и travel_count для каждого пользователя.    
    Идущие подряд повторы городов схлопываются в одно посещение.
    Возврат в город после другого считается новым посещением.    
    Аргументы:
        messages_with_city — DataFrame из add_city_to_messages.    
    Возвращает:
        DataFrame с колонками: user_id, travel_array, travel_count.
    """
    
    # Сортируем по (user_id, event_time, msg_lat, msg_lon)
    order_cols = ["event_time", "msg_lat", "msg_lon"]
    window_order = Window.partitionBy("user_id").orderBy(*order_cols)
    
    # Смотрим предыдущий город
    with_prev = messages_with_city.withColumn(
        "prev_city",
        F.lag("city").over(window_order)
    )
    
    # Помечаем новое посещение
    with_flag = with_prev.withColumn(
        "is_new_visit",
        F.when(
            (F.col("prev_city").isNull()) | (F.col("city") != F.col("prev_city")),
            1
        ).otherwise(0)
    )
    
    # Кумулятивная сумма = номер посещения
    # присваиваем номер посещения каждому сообщению
    window_cum = Window.partitionBy("user_id").orderBy(*order_cols) \
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    with_visit_id = with_flag.withColumn(
        "visit_id",
        F.sum("is_new_visit").over(window_cum)
    )
    
    # Для каждого (user_id, visit_id) берём первый город
    visits = (
        with_visit_id
        .groupBy("user_id", "visit_id")
        .agg(F.first("city").alias("city"))
    )
    
    # Собираем города в порядке visit_id через sort_array
    travel = (
        visits
        .groupBy("user_id")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("visit_id", "city"))
            ).alias("visits_sorted")
        )
        .withColumn("travel_array", F.col("visits_sorted.city"))
        .withColumn("travel_count", F.size("travel_array"))
        .drop("visits_sorted")
    )
    
    return travel

def add_home_city(spark, messages_with_city, depth):
    """Определяет home_city — последний город, где пользователь был ≥27 дней подряд.    
    Условие: каждый день из 27+ подряд пользователь писал ≥1 сообщение 
    из этого города.    
    Аргументы:
        spark              — SparkSession;
        messages_with_city — DataFrame из add_city_to_messages;
        depth              — глубина расчёта. Если <27, home_city не считается.    
    Возвращает:
        DataFrame с колонками: user_id, home_city.
        Если depth < 27 — пустой DataFrame с теми же колонками.
    """
    from pyspark.sql.types import StructType, StructField, LongType, StringType
    
    # Если данных меньше 27 дней — home_city не рассчитывается
    if depth < 27:
        print(f"[add_home_city] depth={depth} < 27 — home_city не рассчитывается")
        empty_schema = StructType([
            StructField("user_id", LongType()),
            StructField("home_city", StringType()),
        ])
        return spark.createDataFrame([], schema=empty_schema)
    
    # Уникальные (user_id, city, date)
    daily = (
        messages_with_city
        .withColumn("date", F.to_date("event_time"))
        .select("user_id", "city", "date")
        .distinct()
    )
    
    # Сортируем и считаем разницу с предыдущей датой
    window = Window.partitionBy("user_id", "city").orderBy("date")
    
    with_gap = (
        daily
        .withColumn("prev_date", F.lag("date").over(window))
        .withColumn("gap_days", F.datediff("date", "prev_date"))
        .withColumn(
            "is_new_chain",
            F.when(
                (F.col("prev_date").isNull()) | (F.col("gap_days") > 1),
                1
            ).otherwise(0)
        )
    )
    
    # Присваиваем chain_id
    window_cum = Window.partitionBy("user_id", "city").orderBy("date") \
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    with_chain = with_gap.withColumn(
        "chain_id",
        F.sum("is_new_chain").over(window_cum)
    )
    
    # Длина каждой цепочки
    chains = (
        with_chain
        .groupBy("user_id", "city", "chain_id")
        .agg(
            F.min("date").alias("chain_start"),
            F.max("date").alias("chain_end"),
        )
        .withColumn(
            "chain_length_days",
            F.datediff("chain_end", "chain_start") + 1
        )
        .where("chain_length_days >= 27")
    )
    
    # Последняя подходящая цепочка для каждого (user_id, city)
    window_pick = Window.partitionBy("user_id", "city").orderBy(F.desc("chain_end"))
    
    last_chain_per_city = (
        chains
        .withColumn("rn", F.row_number().over(window_pick))
        .where("rn = 1")
        .select("user_id", "city", "chain_end")
    )
    
    # Последний город по chain_end
    window_home = Window.partitionBy("user_id").orderBy(F.desc("chain_end"))
    
    home_city = (
        last_chain_per_city
        .withColumn("rn", F.row_number().over(window_home))
        .where("rn = 1")
        .select(
            "user_id",
            F.col("city").alias("home_city"),
        )
    )
    
    return home_city

def add_local_time(messages_with_city):
    """Определяет local_time — время последнего события пользователя 
    в таймзоне города этого события.    
    Аргументы:
        messages_with_city — DataFrame из add_city_to_messages.    
    Возвращает:
        DataFrame с колонками: user_id, local_time.
    """
    
    # Для каждого пользователя — последнее сообщение
    window = Window.partitionBy("user_id").orderBy(F.desc("event_time"))
    
    last_event = (
        messages_with_city
        .withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .select(
            "user_id",
            "event_time",
            "timezone",
        )
    )
    
    # Преобразуем UTC → локальное время
    local_time = last_event.select(
        "user_id",
        F.from_utc_timestamp(F.col("event_time"), F.col("timezone")).alias("local_time"),
    )
    
    return local_time

def build_user_mart(spark, messages_with_city, depth):
    """Строит витрину пользователей.    
    Колонки результата:
        user_id, act_city, home_city, travel_count, travel_array, local_time.    
    Аргументы:
        spark              — SparkSession;
        messages_with_city — DataFrame из add_city_to_messages;
        depth              — глубина расчёта (для home_city).    
    Возвращает:
        DataFrame — витрина пользователей.
    """
    # Каждая часть
    act_city = add_act_city(messages_with_city)
    travel = add_travel(messages_with_city)
    home_city = add_home_city(spark, messages_with_city, depth)
    local_time = add_local_time(messages_with_city)
    
    # Полный outer join по user_id
    user_mart = (
        act_city
        .join(travel, "user_id", "full_outer")
        .join(home_city, "user_id", "full_outer")
        .join(local_time, "user_id", "full_outer")
    )
    
    # Порядок колонок
    user_mart = user_mart.select(
        "user_id",
        "act_city",
        "home_city",
        "travel_count",
        "travel_array",
        "local_time",
    )
    
    return user_mart

def add_coords_to_all_events(events):
    """Присваивает координаты всем событиям.    
    Для message — свои lat/lon.
    Для reaction/subscription — координаты последнего сообщения пользователя.    
    Возвращает: user_id, event_time, event_type, lat, lon.
    """
    
    # Собираем все типы событий в один формат.
    all_events = events.select(
        F.coalesce(
            F.col("event.message_from"),
            F.col("event.reaction_from"),
            F.col("event.user"),
        ).alias("user_id"),
        F.coalesce(
            F.to_timestamp("event.datetime", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("event.message_ts") + F.expr("INTERVAL 1 YEAR")
        ).alias("event_time"),
        F.col("event_type"),
        F.col("lat"),
        F.col("lon"),
    ).where("user_id is not null")
    
    # Последнее сообщение каждого пользователя
    window = Window.partitionBy("user_id").orderBy(F.desc("event_time"))
    
    last_message = (
        all_events
        .where("event_type = 'message'")
        .withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .select(
            "user_id",
            F.col("lat").alias("last_msg_lat"),
            F.col("lon").alias("last_msg_lon"),
        )
    )
    
    # Подставляем координаты
    events_with_coords = (
        all_events
        .join(last_message, "user_id", "left")
        .withColumn(
            "lat_final",
            F.when(F.col("event_type") == "message", F.col("lat"))
             .otherwise(F.col("last_msg_lat"))
        )
        .withColumn(
            "lon_final",
            F.when(F.col("event_type") == "message", F.col("lon"))
             .otherwise(F.col("last_msg_lon"))
        )
        .select(
            "user_id", "event_time", "event_type",
            F.col("lat_final").alias("lat"),
            F.col("lon_final").alias("lon"),
        )
        .where("lat is not null and lon is not null")
    )
    
    return events_with_coords

def add_zone_id_to_events(events_with_coords, geo):
    """Определяет zone_id (город) для каждого события.    
    Аргументы:
        events_with_coords — DataFrame из add_coords_to_all_events 
                             (с колонками user_id, event_time, event_type, lat, lon);
        geo                — справочник городов (id, city, lat, lng, timezone).    
    Возвращает:
        DataFrame с колонками: user_id, event_time, event_type, lat, lon, zone_id.
    """
    
    # Переименовываем geo
    geo_renamed = geo.select(
        F.col("id").alias("zone_id"),
        F.col("lat").alias("city_lat"),
        F.col("lng").alias("city_lng"),
    )
    
    # crossJoin + haversine
    joined = events_with_coords.crossJoin(geo_renamed)
    
    joined = joined.withColumn(
        "distance_km",
        haversine(
            F.col("lat"), F.col("lon"),
            F.col("city_lat"), F.col("city_lng")
        )
    )
    
    # Ближайший город для каждого события
    window = Window.partitionBy(
        "user_id", "event_time", "event_type", "lat", "lon"
    ).orderBy("distance_km")
    
    events_with_zone = (
        joined
        .withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .select(
            "user_id", "event_time", "event_type",
            "lat", "lon", "zone_id",
        )
    )
    
    return events_with_zone

def build_zone_mart(events_with_zone):
    """Строит витрину зон — агрегаты по городам за неделю и месяц.    
    Колонки результата:
        month, week, zone_id,
        week_message, week_reaction, week_subscription, week_user,
        month_message, month_reaction, month_subscription, month_user.    
    Структура: одна таблица, month_* дублируется для каждой недели 
    в этом месяце.    
    Аргументы:
        events_with_zone — DataFrame из add_zone_id_to_events.    
    Возвращает:
        DataFrame — витрина зон.
    """
    
    # week_agg — агрегаты по (month, week, zone_id)
    # Важно: month берём из даты события, а не из даты начала недели.
    week_agg = (
        events_with_zone
        .withColumn("week", F.date_trunc("week", "event_time"))
        .withColumn("month", F.date_trunc("month", "event_time"))
        .groupBy("month", "week", "zone_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0))
                .alias("week_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0))
                .alias("week_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0))
                .alias("week_subscription"),
        )
    )
    
    # month_agg — агрегаты по (month, zone_id)
    month_agg = (
        events_with_zone
        .withColumn("month", F.date_trunc("month", "event_time"))
        .groupBy("month", "zone_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0))
                .alias("month_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0))
                .alias("month_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0))
                .alias("month_subscription"),
        )
    )
    
    # week_user — по первым сообщениям (регистрации)
    first_message = (
        events_with_zone
        .where("event_type = 'message'")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("user_id").orderBy("event_time")
        ))
        .where("rn = 1")
        .select(
            "user_id",
            "zone_id",
            F.date_trunc("week", "event_time").alias("week"),
            F.date_trunc("month", "event_time").alias("month"),
        )
    ).cache()
    
    week_user = (
        first_message
        .groupBy("month", "week", "zone_id")
        .agg(F.countDistinct("user_id").alias("week_user"))
    )
    
    month_user = (
        first_message
        .groupBy("month", "zone_id")
        .agg(F.countDistinct("user_id").alias("month_user"))
    )
    
    # Собираем week-часть (с week_user)
    week_part = week_agg.join(week_user, ["month", "week", "zone_id"], "left")
    
    # Собираем month-часть (с month_user)
    month_part = month_agg.join(month_user, ["month", "zone_id"], "left")
    
    # Финальный join: week_part + month_part по (month, zone_id)
    zone_mart = (
        week_part
        .join(month_part, ["month", "zone_id"], "left")
        .select(
            "month", "week", "zone_id",
            "week_message", "week_reaction", "week_subscription", "week_user",
            "month_message", "month_reaction", "month_subscription", "month_user",
        )
    )
    
    return zone_mart

def extract_subscriptions(events):
    """Возвращает уникальные пары (user_id, channel_id) из подписок.    
    Аргументы:
        events — DataFrame всех событий.    
    Возвращает:
        DataFrame с колонками: user_id, channel_id.
    """
    subscriptions = (
        events
        .where("event_type = 'subscription'")
        .select(
            F.col("event.user").alias("user_id").cast("long"),
            F.col("event.subscription_channel").alias("channel_id"),
        )
        .where("user_id is not null and channel_id is not null")
        .distinct()
    )
    
    return subscriptions

def build_friend_mart(events, messages_with_city, geo, depth):
    """Строит витрину рекомендаций друзей."""
    
    # Подписки
    subscriptions = extract_subscriptions(events)
    print(f"[A] subscriptions: {subscriptions.count()}")
    
    # self-join пары
    subs_a = subscriptions.alias("a")
    subs_b = subscriptions.alias("b")
    
    pairs = (
        subs_a.join(
            subs_b,
            (F.col("a.channel_id") == F.col("b.channel_id")) &
            (F.col("a.user_id") < F.col("b.user_id"))
        )
        .select(
            F.col("a.user_id").alias("user_left"),
            F.col("b.user_id").alias("user_right"),
            F.col("a.channel_id").alias("channel_id"),
        )
        .distinct()
    )
    print(f"[B] pairs: {pairs.count()}")
    
    # Исключаем тех, кто переписывался
    personal_messages_norm = (
        events
        .where("event_type = 'message' and event.message_to is not null")
        .select(
            F.least(F.col("event.message_from"), F.col("event.message_to")).alias("user_left"),
            F.greatest(F.col("event.message_from"), F.col("event.message_to")).alias("user_right"),
        )
        .distinct()
    )
    
    pairs_no_chat = pairs.join(
        personal_messages_norm,
        on=["user_left", "user_right"],
        how="left_anti"
    )
    print(f"[C] pairs без переписок: {pairs_no_chat.count()}")

    # Координаты + haversine + фильтр ≤1 км
    window = Window.partitionBy("user_id").orderBy(F.desc("event_time"))
    
    last_messages = (
        messages_with_city
        .withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .select(
            "user_id",
            "event_time",        
            "city_id",           
            "city",
            "timezone",
            F.col("msg_lat").alias("lat"),
            F.col("msg_lon").alias("lon"),
        )
    )
    
    # Координаты user_left
    pairs_left = pairs_no_chat.join(
        last_messages.select(
            F.col("user_id").alias("user_left"),
            F.col("event_time").alias("event_time_left"),    
            F.col("city_id").alias("city_id_left"),          
            F.col("lat").alias("lat_left"),
            F.col("lon").alias("lon_left"),
            F.col("city").alias("city_left"),
            F.col("timezone").alias("timezone_left"),
        ),
        on="user_left",
        how="inner"
    )
    
    # Координаты user_right
    pairs_both = pairs_left.join(
        last_messages.select(
            F.col("user_id").alias("user_right"),
            F.col("lat").alias("lat_right"),
            F.col("lon").alias("lon_right"),
        ),
        on="user_right",
        how="inner"
        ).cache()                     
    _ = pairs_both.count()        # материализуем кэш
    
    print(f"[1] пар с координатами: {pairs_both.count()}")
    
    # Расстояние
    pairs_with_dist = pairs_both.withColumn(
        "distance_km",
        haversine(
            F.col("lat_left"), F.col("lon_left"),
            F.col("lat_right"), F.col("lon_right")
        )
       ).cache()             
    _ = pairs_with_dist.count()   # материализуем кэш
    
    # Фильтр ≤1 км
    friend_mart_filtered = pairs_with_dist.where("distance_km <= 1.0")
    
    print(f"[2] пар ≤1 км: {friend_mart_filtered.count()}")
    

    friend_mart = friend_mart_filtered.select(
        "user_left",
        "user_right",
        F.current_timestamp().alias("processed_dttm"),
        F.col("city_id_left").alias("zone_id"),
        F.from_utc_timestamp(
            F.col("event_time_left"),
            F.col("timezone_left")
        ).alias("local_time"),
    )
    
    print(f"[3] friend_mart: {friend_mart.count()}")
    #friend_mart.printSchema()
    
    return friend_mart
    


if __name__ == "__main__":
    main()