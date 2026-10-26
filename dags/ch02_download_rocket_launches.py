"""2장 로켓 발사 사진 내려받기 DAG — Airflow 3 버전.

원본: https://github.com/BasPH/data-pipelines-with-apache-airflow/blob/master/chapter02/dags/download_rocket_launches.py
바뀐 점:
- days_ago(14) → 고정 날짜, schedule_interval → schedule
- 오퍼레이터 import: airflow.operators.* → airflow.providers.standard.operators.*
- from airflow import DAG → from airflow.sdk import DAG
- requests는 태스크 함수 안에서 불러오고, 사진 주소가 없는 발사는 건너뛰고, 요청에 timeout을 둠
"""

import json
import pathlib

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

LAUNCHES_URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming"
LAUNCHES_FILE = "/tmp/launches.json"
IMAGES_DIR = "/tmp/images"


def _get_pictures():
    # 파싱 때마다 불러오지 않도록 태스크 안에서 import (1장 '파싱과 실행')
    import requests

    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    launches = json.loads(pathlib.Path(LAUNCHES_FILE).read_text())

    for launch in launches["results"]:
        image_url = launch.get("image")
        if not image_url:
            print(f"No image for {launch['name']}")
            continue
        target_file = f"{IMAGES_DIR}/{image_url.split('/')[-1]}"
        try:
            response = requests.get(image_url, timeout=30)
        except requests.exceptions.ConnectionError:
            print(f"Could not connect to {image_url}.")
            continue
        pathlib.Path(target_file).write_bytes(response.content)
        print(f"Downloaded {image_url} to {target_file}")


with DAG(
    dag_id="download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    schedule="@daily",
):
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command=f"curl -o {LAUNCHES_FILE} -L '{LAUNCHES_URL}'",
    )

    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures,
    )

    notify = BashOperator(
        task_id="notify",
        bash_command=f'echo "There are now $(ls {IMAGES_DIR}/ | wc -l) images."',
    )

    download_launches >> get_pictures >> notify
