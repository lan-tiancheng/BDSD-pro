import os
from datetime import datetime, timezone
from typing import Dict
from collections import defaultdict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, window, expr, to_timestamp, regexp_replace,
    when, unix_timestamp, lit
)
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

import tempfile
import pyspark

print("[启动] realtime_consumer 启动 …")
# ================= 环境变量 =================

def _get_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y")

# 动态推断 PySpark 版本与 Kafka 连接器坐标
SPARK_VERSION = pyspark.__version__
SPARK_SCALA_BINARY = os.getenv("SPARK_SCALA_BINARY", "2.12")
SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE",
    f"org.apache.spark:spark-sql-kafka-0-10_{SPARK_SCALA_BINARY}:{SPARK_VERSION}"
)

# 权重与参数
WEIGHT_VIEW = float(os.getenv("WEIGHT_VIEW", "0.05"))
WEIGHT_LIKE = float(os.getenv("WEIGHT_LIKE", "1.0"))
WEIGHT_COIN = float(os.getenv("WEIGHT_COIN", "1.2"))
WEIGHT_FAVORITE = float(os.getenv("WEIGHT_FAVORITE", "1.0"))
WEIGHT_DANMAKU = float(os.getenv("WEIGHT_DANMAKU", "1.1"))
WEIGHT_REPLY = float(os.getenv("WEIGHT_REPLY", "1.3"))
WEIGHT_SHARE = float(os.getenv("WEIGHT_SHARE", "1.4"))
DECAY_LAMBDA = float(os.getenv("DECAY_LAMBDA", "0.05"))
HALF_LIFE_HOURS = float(os.getenv("HALF_LIFE_HOURS", "6"))
COEF_LOG_VIEW = float(os.getenv("COEF_LOG_VIEW", "0.4"))
COEF_LOG_WEIGHTED = float(os.getenv("COEF_LOG_WEIGHTED", "0.4"))
COEF_RATIO = float(os.getenv("COEF_RATIO", "0.2"))
MOMENTUM_CLIP = float(os.getenv("MOMENTUM_CLIP", "0.5"))

# 运行配置
# 本地调试时使用 localhost:9092；如在容器/集群里再改回对应地址
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "bilibili_videos")
STARTING_OFFSETS = "earliest"  # 强制 earliest，确保能消费所有历史数据
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "24 hours")
WINDOW_SLIDE = os.getenv("WINDOW_SLIDE", "5 minutes")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "25 hours")
TRIGGER_SECONDS = int(os.getenv("CONSUMER_TRIGGER_SECONDS", "60"))
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", "24"))
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", "24"))
MAX_FRESH_HOURS = float(os.getenv("MAX_FRESH_HOURS", "720"))
NO_DECAY = _get_bool("NO_DECAY", "false")
DEBUG_METRICS = _get_bool("DEBUG_METRICS", "true")
ENABLE_VIDEO_TOPN = _get_bool("ENABLE_VIDEO_TOPN", "true")
TOPN = int(os.getenv("TOPN", "3"))
CONTROL_CATEGORY = os.getenv("CONTROL_CATEGORY", "__MARKER__")
SKIP_ZERO_WINDOWS = _get_bool("SKIP_ZERO_WINDOWS", "true")
MIN_NONZERO_CATEGORIES = int(os.getenv("MIN_NONZERO_CATEGORIES", "2"))
MIN_TOTAL_RAW_SUM = int(os.getenv("MIN_TOTAL_RAW_SUM", "10"))

# 将 checkpoint 放到本地可写目录
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.join(tempfile.gettempdir(), "ck_round_batch"))

print(f"[启动] 参数: window={WINDOW_DURATION}/{WINDOW_SLIDE} watermark={WATERMARK_DELAY} trigger={TRIGGER_SECONDS}s")

# ================= Spark 初始化 =================

builder = (SparkSession.builder
           .appName("BilibiliHotStreaming")
           .config("spark.sql.shuffle.partitions", "4")
           .config("spark.sql.session.timeZone", "UTC")
           .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
           )
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("[启动] Spark 会话创建成功")
print(f"[启动] 使用 Kafka 连接器: {SPARK_KAFKA_PACKAGE}")

# ================= 读取 Kafka =================

raw_df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
          .option("subscribe", TOPIC)
          .option("startingOffsets", STARTING_OFFSETS)
          .load())
print(f"[启动] Kafka 源已创建 topic={TOPIC} startingOffsets={STARTING_OFFSETS} bootstrap={KAFKA_BOOTSTRAP}")

schema = (StructType()
          .add("bvid", StringType())
          .add("title", StringType())
          .add("pubdate", StringType())
          .add("category", StringType())
          .add("duration", IntegerType())
          .add("view", IntegerType())
          .add("like", IntegerType())
          .add("coin", IntegerType())
          .add("favorite", IntegerType())
          .add("danmaku", IntegerType())
          .add("reply", IntegerType())
          .add("share", IntegerType())
          .add("engagement", IntegerType())
          .add("collect_time", StringType())
          .add("round_id", IntegerType())
          .add("marker", StringType()))

json_df = raw_df.selectExpr("CAST(value AS STRING) AS raw_value") \
    .select(from_json(col("raw_value"), schema).alias("data"))
base_df = json_df.select("data.*")

from pyspark.sql.window import Window

# 全局变量，存储上轮各bvid的各项数值
_last_metrics: Dict[str, dict] = defaultdict(dict)

def process_round(batch_df: DataFrame, batch_id: int):
    print(f"[批次{batch_id}] batch_df.count={batch_df.count()}", flush=True)
    if batch_df.rdd.isEmpty():
        print(f"[批次{batch_id}] 空批次，跳过", flush=True)
        return
    # 1. 过滤出24小时内的数据
    df = batch_df.withColumn(
        "collect_time_ts",
        to_timestamp(regexp_replace(col("collect_time"), "T", " "))
    )
    # 输出时间范围调试
    try:
        minmax = df.agg(expr('min(collect_time_ts)').alias('min'), expr('max(collect_time_ts)').alias('max')).collect()[0]
        print(f"[批次{batch_id}] collect_time_ts范围: min={minmax['min']} max={minmax['max']}", flush=True)
    except Exception as e:
        print(f"[批次{batch_id}] collect_time_ts范围统计异常: {e}", flush=True)
    df = df.filter(col("collect_time_ts") >= expr(f"current_timestamp() - interval {RETENTION_HOURS} hours"))
    # 只保留非 marker 数据
    data_df = df.filter(col("category") != CONTROL_CATEGORY)
    if data_df.rdd.isEmpty():
        print(f"[批次{batch_id}] 仅有 marker或无24小时内有效数据，跳过", flush=True)
        return
    # 找到最新一轮 round_id
    from pyspark.sql.functions import max as spark_max, row_number
    max_round = data_df.agg(spark_max("round_id").alias("max_round")).collect()[0]["max_round"]
    if max_round is None:
        print(f"[批次{batch_id}] 无 round_id，跳过", flush=True)
        return
    cur_df = data_df.filter(col("round_id") == max_round)
    count = cur_df.count()
    print(f"[批次{batch_id}] 处理 round_id={max_round} 数据量={count}", flush=True)
    if count == 0:
        print(f"[批次{batch_id}] 本轮无数据，跳过", flush=True)
        return
    # ========== 增量分数与时间衰减 ===========
    metric_cols = ["view", "like", "coin", "favorite", "danmaku", "reply", "share"]
    # 1. 收集本轮所有bvid及其各项数值
    cur_rows = cur_df.select(
        "bvid", "category", "title", "pubdate", "duration", "collect_time", *metric_cols
    ).collect()
    delta_data = []
    for row in cur_rows:
        bvid = row['bvid']
        now_metrics = {k: int(row[k]) if row[k] is not None else 0 for k in metric_cols}
        last_metrics = _last_metrics.get(bvid, {})
        delta_metrics = {k + "_delta": now_metrics[k] - int(last_metrics.get(k, 0)) for k in metric_cols}
        # 时间衰减
        collect_time = row['collect_time']
        try:
            if isinstance(collect_time, str):
                collect_dt = datetime.strptime(collect_time.replace("T", " "), "%Y-%m-%d %H:%M:%S")
            else:
                collect_dt = datetime.utcnow()
        except Exception:
            collect_dt = datetime.utcnow()
        hours_since = (datetime.utcnow() - collect_dt).total_seconds() / 3600.0
        decay = float(os.getenv("DECAY_LAMBDA", "0.05"))
        decay_factor = pow(2.71828, -decay * hours_since)
        merged = dict(row.asDict())
        merged.update(delta_metrics)
        merged["decay_factor"] = decay_factor
        delta_data.append(merged)
        # 更新全局上轮数据
        _last_metrics[bvid] = now_metrics
    from pyspark.sql import Row
    delta_df = spark.createDataFrame([Row(**d) for d in delta_data])
    # ========== 用增量分数和时间衰减 ===========
    expr_weight_delta = (
        f"{WEIGHT_VIEW}*coalesce(view_delta,0) + {WEIGHT_LIKE}*coalesce(like_delta,0) + {WEIGHT_COIN}*coalesce(coin_delta,0) + "
        f"{WEIGHT_FAVORITE}*coalesce(favorite_delta,0) + {WEIGHT_DANMAKU}*coalesce(danmaku_delta,0) + "
        f"{WEIGHT_REPLY}*coalesce(reply_delta,0) + {WEIGHT_SHARE}*coalesce(share_delta,0)"
    )
    scored = (delta_df
        .withColumn("weighted_engagement_delta", expr(expr_weight_delta))
        .withColumn("base_score", expr("weighted_engagement_delta * decay_factor"))
    )
    # 分类分数对比
    cat_agg = (scored.groupBy("category")
        .agg(expr("avg(base_score)").alias("avg_score"),
             expr("sum(view_delta+like_delta+coin_delta+favorite_delta+danmaku_delta+reply_delta+share_delta)").alias("sum_delta"),
             expr("count(*)").alias("cnt")))
    cat_rows = cat_agg.orderBy(col("avg_score").desc()).collect()
    print(f"[轮次{max_round}] 分类分数对比:", flush=True)
    for i, r in enumerate(cat_rows, 1):
        print(f"  TOP{i} {r['category']} avg_score={r['avg_score']:.4f} sum_delta={r['sum_delta']} cnt={r['cnt']}", flush=True)
    # 每个分区 TopN 视频
    from pyspark.sql.functions import row_number
    w = Window.partitionBy("category").orderBy(col("base_score").desc())
    topn = (scored.withColumn("rn", row_number().over(w)).filter(col("rn") <= TOPN)
        .orderBy(col("category"), col("rn")))
    topn_rows = topn.collect()
    print(f"[轮次{max_round}] 每个分区 Top{TOPN} 视频:", flush=True)
    last_cat = None
    for r in topn_rows:
        if r['category'] != last_cat:
            print(f"  分类: {r['category']}", flush=True)
            last_cat = r['category']
        print(f"    #{r['rn']} bvid={r['bvid']} title={r['title'][:40]} score={r['base_score']:.4f}", flush=True)

query = (base_df.writeStream
         .outputMode("append")
         .queryName("round_batch")
         .foreachBatch(process_round)
         .option("checkpointLocation", CHECKPOINT_DIR)
         .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
         .start())
print(f"[启动] 批处理流式查询已启动 (trigger={TRIGGER_SECONDS}s, checkpoint={CHECKPOINT_DIR})")

import threading, time

def monitor():
    while True:
        try:
            active = spark.streams.active
            for q in active:
                lp = q.lastProgress or {}
                print(f"[监控] name={q.name} active={q.isActive} inputRows={lp.get('numInputRows')} status={q.status.get('message')}", flush=True)
        except Exception as e:
            print(f"[监控] 异常: {e}", flush=True)
        time.sleep(60)

threading.Thread(target=monitor, daemon=True).start()

for q in spark.streams.active:
    q.awaitTermination()