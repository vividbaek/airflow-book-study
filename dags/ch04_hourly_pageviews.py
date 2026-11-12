"""Chapter 4: render hourly pageview paths with Airflow 3.

Inspired by (not copied):
https://github.com/BasPH/data-pipelines-with-apache-airflow/tree/master/chapter04

Uses a tiny local fixture instead of downloading the full Wikimedia dump.
All tasks share /tmp inside this repository's single Airflow container.
The Wikimedia file name marks the END of its UTC hour, so the URL uses
data_interval_end while the local partition uses data_interval_start.
"""

import gzip
import json
from pathlib import Path

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, CronDataIntervalTimetable

# Wikimedia 파일은 UTC 시각 기준입니다. 같은 시간은 같은 경로를 사용합니다.
PARTITION = "{{ data_interval_start.in_timezone('UTC').strftime('%Y%m%d-%H') }}"
ROOT = "/tmp/ch04/" + PARTITION
SAMPLE_PATH = ROOT + "/pageviews.gz"
SUMMARY_PATH = ROOT + "/summary.json"


def _prepare_sample(data_interval_end, output_path):
    # 이 인자는 Jinja 문자열이 아니라 PythonOperator가 전달한 날짜 객체입니다.
    # 위키미디어 파일 이름의 시각은 집계 구간의 끝이라 구간 끝으로 주소를 만듭니다.
    hour = data_interval_end.in_timezone("UTC")
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{hour:%Y}/{hour:%Y-%m}/pageviews-{hour:%Y%m%d-%H}0000.gz"
    )
    print(f"source_url={url} (sample only; no download)")
    print(f"context_type={type(data_interval_end).__name__}")
    # 함수 본문의 일반 문자열까지 Airflow가 찾아서 치환하지는 않습니다.
    print("ordinary_python_string={{ ds }}")
    # 실제 파일처럼 프로젝트·제목 순으로 정렬하고, 같은 쌍은 한 번만 둡니다.
    rows = (
        "en Amazon 8 0\n"
        "en Apple 15 0\n"
        "en Facebook 2 0\n"
        "en Google 7 0\n"
        "en Microsoft 4 0\n"
        "en.m Apple 5 0\n"
        "nl Apple 99 0\n"
    )
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(rows)
    print(f"wrote {path}")


def _summarize(input_path, output_path, pages, interval_start, interval_end):
    counts = dict.fromkeys(pages, 0)
    with gzip.open(input_path, "rt", encoding="utf-8") as stream:
        for line in stream:
            project, title, views, _ = line.split()
            if project == "en" and title in counts:
                counts[title] = int(views)
    result = {
        "interval_start": interval_start,
        "interval_end": interval_end,
        "pageviews": counts,
    }
    text = json.dumps(result, indent=2, sort_keys=True)
    Path(output_path).write_text(text + "\n", encoding="utf-8")
    print(text)
    print(f"wrote {output_path}")


with DAG(
    dag_id="hourly_pageviews",
    start_date=pendulum.datetime(2026, 10, 24, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["book", "chapter04"],
):
    prepare_sample = PythonOperator(
        task_id="prepare_sample",
        python_callable=_prepare_sample,
        op_kwargs={"output_path": SAMPLE_PATH},
        do_xcom_push=False,
    )
    inspect_template = BashOperator(
        task_id="inspect_template",
        # 날짜는 env에서 렌더링하고 셸에서는 변수를 따옴표로 감싸 사용합니다.
        bash_command='printf "partition=%s\nsample=%s\n" "$PARTITION" "$SAMPLE_PATH"; test -s "$SAMPLE_PATH"',
        env={"PARTITION": PARTITION, "SAMPLE_PATH": SAMPLE_PATH},
        append_env=True,
        do_xcom_push=False,
    )
    summarize = PythonOperator(
        task_id="summarize",
        python_callable=_summarize,
        op_kwargs={
            "input_path": SAMPLE_PATH,
            "output_path": SUMMARY_PATH,
            "pages": ["Apple", "Google", "Amazon"],
            "interval_start": "{{ data_interval_start.isoformat() }}",
            "interval_end": "{{ data_interval_end.isoformat() }}",
        },
        do_xcom_push=False,
    )
    prepare_sample >> inspect_template >> summarize
