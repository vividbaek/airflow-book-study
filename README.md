# airflow-book-study

《Apache Airflow 기반의 데이터 파이프라인》(원서 *Data Pipelines with Apache Airflow*)의 예제를 **Airflow 3**에서 직접 돌려 보는 실습 저장소입니다.

책은 Airflow 2.0 베타 시절에 쓰였기 때문에, 예제 상당수가 Airflow 3에서는 그대로 돌아가지 않습니다. 장마다 예제를 Airflow 3에 맞게 고치고, 무엇이 바뀌었는지 함께 기록합니다.

## 진행 현황

| 장 | 주제 | Airflow 3 예제 | 바뀐 점 |
| :---: | :--- | :--- | :--- |
| 1 | Apache Airflow 살펴보기 | [dags/ch01_umbrella.py](dags/ch01_umbrella.py) | `DummyOperator` → `EmptyOperator`, `days_ago` → 고정 날짜, `schedule_interval` → `schedule` |

## 직접 실행하기

### 방법 1. 내 컴퓨터에서 (Docker)

[Docker](https://docs.docker.com/get-docker/)만 있으면 됩니다.

```bash
git clone https://github.com/vividbaek/airflow-book-study.git
cd airflow-book-study
docker compose up
```

1~2분 뒤 http://localhost:8080 에 들어가면 로그인 없이 Airflow 화면이 열립니다. `dags/` 폴더의 DAG는 켜진 상태로 올라와 바로 한 번 실행되고, ▶(Trigger) 버튼으로 다시 실행할 수 있습니다.

- 끄기: `Ctrl+C` 후 `docker compose down` (실행 기록도 함께 사라집니다)
- 8080 포트를 이미 쓰고 있다면: `AIRFLOW_PORT=8081 docker compose up` (Windows PowerShell은 `$env:AIRFLOW_PORT="8081"; docker compose up`)
- 화면은 내 컴퓨터(`127.0.0.1`)에서만 열립니다. 로그인 없이 관리자로 들어가는 설정이라, 같은 네트워크의 다른 기기에서는 접속되지 않게 묶어 두었습니다.

### 방법 2. 브라우저에서 바로 (GitHub Codespaces)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/vividbaek/airflow-book-study)

Docker를 설치하지 않아도 됩니다. 위 버튼으로 Codespace를 만들면 Airflow가 자동으로 뜨고, **포트(Ports)** 탭에서 8080을 열면 됩니다. 개인 계정은 매달 무료 사용량 안에서 쓸 수 있고, 다 쓰고 나면 Codespace를 중지하거나 삭제해 두세요.

> 이 실습 환경은 `airflow standalone`으로 컨테이너 하나에 모든 구성 요소를 띄웁니다. 로그인 없이 누구나 관리자로 들어가고 메타스토어도 SQLite라서, **로컬 실습 전용**입니다.

## 기준 버전

- Airflow **3.3.2** (공식 Docker 이미지 `apache/airflow:3.3.2`로 확인)
- Airflow 3 호환성 검사: [ruff](https://docs.astral.sh/ruff/)의 AIR 규칙

```bash
# AIR 규칙은 ruff 0.13.1 이상 필요
uvx ruff@latest check --select AIR3 dags/
```

## 원본 예제

책의 원본 예제 코드는 저자 저장소 [BasPH/data-pipelines-with-apache-airflow](https://github.com/BasPH/data-pipelines-with-apache-airflow)에 있습니다. 이 저장소에는 원본을 복사하지 않고, Airflow 3용으로 직접 고친 버전만 둡니다.
