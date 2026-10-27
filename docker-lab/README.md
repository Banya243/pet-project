# Docker-стенд для практики джоб

Изолированный Compose-проект `jobs-lab`: Spark 3.5.8, Flink 1.18.1,
Airflow 2.11.2, PostgreSQL 16 и MinIO с S3 API. Scala-проекты используют
Scala 2.12.18, sbt 1.10.11 и sbt-assembly 2.3.1. Java 11 находится внутри
контейнеров; локальные Java, Scala, Python и sbt для работы не нужны.

## Запуск

Нужен Docker Desktop в режиме Linux containers с Docker Compose v2.
Для всего стенда выдели Docker около 10 ГБ RAM и 15–20 ГБ диска под образы
и кеши. Первый запуск скачивает образы и зависимости.

MinIO собирается из официального исходного выпуска
`RELEASE.2025-04-22T22-12-26Z`: его исторические образы недоступны в реестрах.
Эта сборка выполняется один раз и кешируется Docker.

Из корня репозитория:

```powershell
Set-Location .\docker-lab
docker compose up -d --build --wait --wait-timeout 300
docker compose ps -a
```

Compose сам создаёт базы, пользователя Airflow, S3-бакеты `raw` и `processed`,
а также загружает `data/orders.csv` в `s3://raw/orders/2026-10-26/orders.csv`.
Повторный запуск сохраняет базы, объекты и существующий входной CSV.
Сервисы `airflow-init` и `minio-init` должны завершиться с кодом `0`.

| Сервис | Адрес на компьютере | Логин / пароль |
| --- | --- | --- |
| Airflow | http://localhost:18080 | `airflow` / `airflow` |
| Spark master | http://localhost:18081 | — |
| Spark worker | http://localhost:18082 | — |
| Flink | http://localhost:18083 | — |
| MinIO Console | http://localhost:19001 | `minioadmin` / `minioadmin` |
| S3 API | http://localhost:19000 | `minioadmin` / `minioadmin` |
| PostgreSQL | `localhost:15433`, база `warehouse` | `lab` / `lab` |
| Spark gateway SSH | `localhost:12222` | `spark` / `spark` |

Все опубликованные порты привязаны к `127.0.0.1`. Это локальный учебный стенд
с демонстрационными паролями. Если порт занят, скопируй `.env.example` в `.env`
и измени соответствующий порт. Адреса между контейнерами при этом не меняются.

## Проверка всей цепочки

В Airflow открой DAG `environment_check`, включи его и нажми Trigger DAG.
Его задачи: `wait_for_input` → `run_spark` → `verify_output`.
Airflow ждёт объект в MinIO, через SSH запускает PySpark на Spark-кластере,
затем проверяет Parquet в MinIO и запись в PostgreSQL.

Ту же проверку можно выполнить из терминала без ручного запуска в UI:

```powershell
docker compose exec airflow-scheduler airflow dags test environment_check 2026-10-26
docker compose exec postgres psql -U lab -d warehouse -c "SELECT * FROM public.environment_check;"
```

Ожидается `spark_version = 3.5.8`, `input_rows = 7`. Это проверка инфраструктуры;
агрегацию заказов для учебного задания нужно реализовать самостоятельно.

## Где писать джобы

| Каталог | Назначение |
| --- | --- |
| `dags/` | Python-файлы DAG для Airflow |
| `jobs/pyspark/` | PySpark-джобы |
| `jobs/scala/spark/src/main/scala/` | Spark-джобы на Scala |
| `jobs/scala/flink/src/main/scala/` | Flink-джобы на Scala |
| `jobs/scala/build.sbt` | Зависимости и сборка обоих Scala-проектов |
| `config/spark-defaults.conf` | Spark, S3 и параметры ресурсов |

Эти каталоги подключены к контейнерам: изменения кода сразу доступны.
После изменения Scala-кода нужно пересобрать JAR. После изменения Python-зависимостей
Airflow в `docker/airflow/requirements.txt` нужно пересобрать образ.

## Сборка Scala внутри Docker

```powershell
docker compose run --rm --build sbt "sparkJob/assembly" "flinkJob/assembly"
```

JAR-файлы появятся на компьютере:

- `jobs/scala/spark/target/scala-2.12/spark-job.jar`
- `jobs/scala/flink/target/scala-2.12/flink-job.jar`

Кеши sbt, Ivy и Coursier сохраняются в Docker volumes. Spark и Flink отмечены
в сборке как `Provided`, чтобы не включать движки в JAR. Flink-пример использует
Java DataStream API из Scala.

Запуск Scala-проверок:

```powershell
docker compose exec --user spark spark-gateway /usr/local/bin/submit-spark --class lab.spark.EnvironmentCheck /opt/lab/jobs/scala/spark/target/scala-2.12/spark-job.jar
docker compose exec flink-jobmanager flink run -c lab.flink.EnvironmentCheck /opt/lab/jobs/scala/flink/target/scala-2.12/flink-job.jar
```

Spark должен напечатать `Scala 2.12.18, Spark 3.5.8, rows=7`.
Flink должен завершить джобу; в `docker compose logs flink-taskmanager`
появятся значения `10`, `20`, `30` в произвольном порядке.

## Запуск своей PySpark-джобы

```powershell
docker compose exec --user spark spark-gateway /usr/local/bin/submit-spark /opt/lab/jobs/pyspark/environment_check.py
```

Вместо `environment_check.py` укажи свой файл и добавь его аргументы.
Spark UI драйвера доступен на http://localhost:14040 только пока джоба работает.
Образы Spark уже содержат S3A-коннектор и JDBC-драйвер PostgreSQL.

Внутри контейнеров используй адреса:

- Spark: `spark://spark-master:7077`.
- S3 API: `http://minio:9000`; Spark-пути начинаются с `s3a://`.
- PostgreSQL: `jdbc:postgresql://postgres:5432/warehouse`.
- Flink REST: `http://flink-jobmanager:8081`.

Airflow connections заданы через переменные окружения: `minio_s3`, `spark_ssh`,
`warehouse`. Они доступны операторам и hooks, но не отображаются в списке
connections в UI. Для S3 используется `S3KeySensor`.
Flink настроен сохранять checkpoints и savepoints в бакете `processed`.

## Логи и остановка

```powershell
docker compose logs --tail 100 airflow-scheduler spark-worker
docker compose logs --tail 100 flink-taskmanager
docker compose stop
docker compose up -d --wait --wait-timeout 300
```

`docker compose down` удаляет контейнеры и сеть, сохраняя данные в volumes.
Команда `docker compose down --volumes` также удалит базы, S3-объекты, логи
и кеши сборки этого стенда: используй её только для полного сброса.

Основа конфигурации: [Airflow 2.11.2 в Docker](https://airflow.apache.org/docs/apache-airflow/2.11.2/howto/docker-compose/index.html),
[официальные образы Spark](https://github.com/apache/spark-docker),
[Flink 1.18 в Docker](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/resource-providers/standalone/docker/).
