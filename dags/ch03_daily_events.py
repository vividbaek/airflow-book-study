"""Chapter 3: process one daily event partition with Airflow 3.

Inspired by the scheduling/partitioning examples (not copied):
https://github.com/BasPH/data-pipelines-with-apache-airflow/tree/master/chapter03

Changes: public SDK imports, explicit data-interval timetable, explicit catchup,
interval templates instead of execution_date/next_ds, deterministic sample events
instead of a separate Flask API, and atomic partition replacement. Local demo:
the two tasks share /tmp inside this repository's single Airflow container.
"""

import csv
import io
import json
import os
import tempfile
from collections import Counter
from pathlib import Path

import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, CronDataIntervalTimetable

TZ = "Asia/Seoul"
# 실행한 오늘 날짜가 아니라, 담당 구간의 시작 날짜로 파일을 나눕니다.
PARTITION = "{{ data_interval_start.in_timezone('Asia/Seoul') | ds }}"
EVENTS_PATH = f"/tmp/ch03/events/{PARTITION}.json"
STATS_PATH = f"/tmp/ch03/stats/{PARTITION}.csv"


def _atomic_write(output_path, text):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        # 같은 파일시스템의 임시 파일에 끝까지 쓴 뒤 최종 파일을 교체합니다.
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(text)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _fetch_events(start, end, output_path):
    start, end = pendulum.parse(start), pendulum.parse(end)
    # 외부 API 대신 샘플 5건을 준비합니다. 구간 안은 3건, 구간 밖은 2건입니다.
    samples = [
        (start.subtract(hours=1), "outside"),
        (start.add(hours=8), "alice"),
        (start.add(hours=12), "alice"),
        (start.add(hours=13), "bob"),
        (end, "outside"),
    ]
    # 실제 API·SQL 조회에도 이 시작 포함·끝 제외 조건을 적용해야 합니다.
    events = [
        {"timestamp": timestamp.isoformat(), "user": user}
        for timestamp, user in samples
        if start <= timestamp < end
    ]
    _atomic_write(output_path, json.dumps(events, indent=2))
    print(f"interval=[{start.isoformat()}, {end.isoformat()}) events={len(events)}")
    print(f"wrote {output_path}")


def _calculate_stats(input_path, output_path):
    events = json.loads(Path(input_path).read_text(encoding="utf-8"))
    counts = Counter(
        (pendulum.parse(event["timestamp"]).in_timezone(TZ).to_date_string(), event["user"])
        for event in events
    )
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["date", "user", "events"])
    writer.writerows((date, user, count) for (date, user), count in sorted(counts.items()))
    _atomic_write(output_path, buffer.getvalue())
    print(buffer.getvalue(), end="")
    print(f"wrote {output_path}")


with DAG(
    dag_id="daily_events",
    # 자동 일정의 시작 기준입니다. 실제 조회 범위는 실행마다 전달받습니다.
    start_date=pendulum.datetime(2026, 10, 24, tz=TZ),
    # 한국 시간 하루 구간을 만들고, 그 하루가 끝난 뒤 실행합니다.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=TZ),
    # 처음 켤 때 최신 완료 구간만 처리합니다. 과거 날짜는 뒤에서 백필합니다.
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["book", "chapter03"],
):
    fetch_events = PythonOperator(
        task_id="fetch_events",
        python_callable=_fetch_events,
        op_kwargs={
            # Airflow의 구간 값을 조회 함수의 start·end 인자로 전달합니다.
            "start": "{{ data_interval_start.isoformat() }}",
            "end": "{{ data_interval_end.isoformat() }}",
            "output_path": EVENTS_PATH,
        },
    )
    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={"input_path": EVENTS_PATH, "output_path": STATS_PATH},
    )
    fetch_events >> calculate_stats
