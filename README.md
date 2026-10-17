# airflow-book-study

《Apache Airflow 기반의 데이터 파이프라인》(원서 *Data Pipelines with Apache Airflow*)의 예제를 **Airflow 3**에서 직접 돌려 보는 실습 저장소입니다.

책은 Airflow 2.0 베타 시절에 쓰였기 때문에, 예제 상당수가 Airflow 3에서는 그대로 돌아가지 않습니다. 장마다 예제를 Airflow 3에 맞게 고치고, 무엇이 바뀌었는지 함께 기록합니다.

## 진행 현황

| 장 | 주제 | Airflow 3 예제 | 바뀐 점 |
| :---: | :--- | :--- | :--- |
| 1 | Apache Airflow 살펴보기 | [dags/ch01_umbrella.py](dags/ch01_umbrella.py) | `DummyOperator` → `EmptyOperator`, `days_ago` → 고정 날짜, `schedule_interval` → `schedule` |

## 기준 버전

- Airflow **3.3.2** (공식 Docker 이미지 `apache/airflow:3.3.2`로 확인)
- Airflow 3 호환성 검사: [ruff](https://docs.astral.sh/ruff/)의 AIR 규칙

```bash
# AIR 규칙은 ruff 0.13.1 이상 필요
uvx ruff@latest check --select AIR3 dags/
```

## 원본 예제

책의 원본 예제 코드는 저자 저장소 [BasPH/data-pipelines-with-apache-airflow](https://github.com/BasPH/data-pipelines-with-apache-airflow)에 있습니다. 이 저장소에는 원본을 복사하지 않고, Airflow 3용으로 직접 고친 버전만 둡니다.
