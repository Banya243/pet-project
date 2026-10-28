# Docker-стенд для практики джоб

Изолированный Compose-проект `jobs-lab`: Spark 3.5.8, Flink 1.18.1,
Airflow 2.11.2, PostgreSQL 16 и MinIO с S3 API. Scala-проекты используют
Scala 2.12.18, sbt 1.10.11 и sbt-assembly 2.3.1. Java 11 находится внутри
контейнеров; локальные Java, Scala, Python и sbt для работы не нужны.
Необязательный профиль `kafka` добавляет Kafka 3.9.1. Для потока Kafka → S3
реализована Scala/Flink-джоба `lab.kafka.KafkaToS3`; генератор синтетики
пользователь реализует самостоятельно.

## Запуск

Нужен Docker Desktop в режиме Linux containers с Docker Compose v2.
Для всего стенда выдели Docker около 10 ГБ RAM и 15–20 ГБ диска под образы
и кеши. Первый запуск скачивает образы и зависимости.
Для профиля Kafka предусмотрен дополнительный запас около 1 ГБ RAM.

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
| Kafka (профиль `kafka`) | `localhost:19092` | — |
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
| `jobs/scala/kafka-to-s3/src/main/scala/lab/kafka/` | Джоба Kafka → S3, параметры, envelope и распределение файлов по каталогам |
| `jobs/scala/kafka-to-s3/src/main/resources/` | Avro-схема сырой Kafka-записи |
| `jobs/scala/kafka-to-s3/src/test/scala/` | Тесты конфигурации, данных, сериализации и Parquet |
| `jobs/pyspark/verify_kafka_raw.py` | Проверка числа записей, tombstone и уникальности Kafka-координат в S3 |
| `jobs/scala/build.sbt` | Подпроекты `sparkJob`, `flinkJob` и `kafkaToS3` |
| `config/spark-defaults.conf` | Spark, S3 и параметры ресурсов |
| `config/kafka/` | Пример Kafka properties и локальные TLS/SASL-настройки |
| `docs/kafka-to-s3.md` | Контракт Kafka → S3, параметры и восстановление |

Эти каталоги подключены к контейнерам: изменения кода сразу доступны.
После изменения Scala-кода нужно пересобрать JAR. После изменения Python-зависимостей
Airflow в `docker/airflow/requirements.txt` нужно пересобрать образ.

## Сборка Scala внутри Docker

```powershell
docker compose run --rm --build -T sbt "sparkJob/assembly" "flinkJob/assembly" "kafkaToS3/assembly"
```

JAR-файлы появятся на компьютере:

- `jobs/scala/spark/target/scala-2.12/spark-job.jar`
- `jobs/scala/flink/target/scala-2.12/flink-job.jar`
- `jobs/scala/kafka-to-s3/target/scala-2.12/kafka-to-s3.jar`

Кеши sbt, Ivy и Coursier сохраняются в Docker volumes. Spark и Flink отмечены
в сборке как `Provided`, чтобы не включать движки в JAR. Flink-пример использует
Java DataStream API из Scala.
В JAR `kafkaToS3` включены Kafka/Avro/Parquet и необходимые Hadoop API;
Flink, FileSink и Scala library предоставляет кластер. S3 filesystem работает
через уже включённый плагин `flink-s3-fs-hadoop`.

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
- Kafka (профиль `kafka`): `kafka:9092`.

Airflow connections заданы через переменные окружения: `minio_s3`, `spark_ssh`,
`warehouse`. Они доступны операторам и hooks, но не отображаются в списке
connections в UI. Для S3 используется `S3KeySensor`.
Flink настроен сохранять checkpoints и savepoints в бакете `processed`.

## Поток Kafka → S3 на Scala/Flink

Реализована отдельная джоба `lab.kafka.KafkaToS3`: Kafka → сырой Parquet/Snappy
в MinIO, с сохранением payload, key, headers и Kafka-координат. Генератор данных
не входит в джобу: сообщения можно отправлять своим producer в любом формате.

Сборка и тесты:

```powershell
docker compose run --rm -T sbt "kafkaToS3/test" "kafkaToS3/assembly"
```

Артефакт: `jobs/scala/kafka-to-s3/target/scala-2.12/kafka-to-s3.jar`.
Необязательный профиль `kafka` добавляет отдельный брокер `apache/kafka:3.9.1`:
`localhost:19092` для producer на компьютере, `kafka:9092` для Flink.
Внешний порт переопределяется через `KAFKA_PORT` в `.env`.

Подготовка локального брокера и топика после первоначального запуска стенда:

```powershell
docker compose --profile kafka up -d --wait --wait-timeout 300 kafka flink-jobmanager flink-taskmanager
docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic orders.events --partitions 2 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete
```

Kafka использует KRaft и volume `jobs-lab_kafka-data`. Одноразовый `kafka-init`
назначает владельца каталога данных (`1000:1000`) и завершается с кодом `0`;
сам брокер работает как `appuser`. Автосоздание топиков отключено.
Профиль не включается обычной командой `docker compose up`.

Первый запуск джобы после сборки JAR:

```powershell
$jobJar = "/opt/lab/jobs/scala/kafka-to-s3/target/scala-2.12/kafka-to-s3.jar"
$jobArgs = @(
  "--bootstrap-servers", "kafka:9092",
  "--topics", "orders.events",
  "--group-id", "orders-raw-v1",
  "--output", "s3a://raw/kafka/orders-v1",
  "--checkpoint-dir", "s3a://processed/flink/checkpoints/orders-raw-v1"
)
docker compose exec -T flink-jobmanager flink run -d -c lab.kafka.KafkaToS3 $jobJar @jobArgs
```

Payload и key сохраняются как nullable binary; headers сохраняют порядок и
повторяющиеся ключи. Tombstone и невалидный JSON попадают в сырой слой без
фильтрации. Каталоги строятся по топику и часу приёма в UTC. На первом старте
читаются доступные записи с earliest; checkpoint по умолчанию проходит каждые
30 секунд, после успешного checkpoint и commit файлы становятся доступны в S3.

Для плановой остановки используй `flink stop --savepointPath ... JOB_ID`,
для продолжения — `flink run -s ...` с прежними аргументами. Обычный новый
`flink run` не восстанавливает состояние по одному лишь `--checkpoint-dir`.
Exactly-once требует согласованного состояния source и sink; для намеренного
replay выбирай новые output, checkpoint prefix и group-id.

Дополнительные TLS/SASL-настройки задаются через
`config/kafka/client.properties` по примеру `client.properties.example`.
Каталог подключён к обоим Flink-контейнерам как `/opt/lab/config/kafka:ro`;
локальные credentials и ключи игнорируются Git.

Проверка небольшого тестового префикса без изменения данных:

```powershell
docker compose exec -T --user spark spark-gateway /usr/local/bin/submit-spark /opt/lab/jobs/pyspark/verify_kafka_raw.py --input s3a://raw/kafka/TEST_PREFIX --expected-records 6 --expected-tombstones 1
```

При проверках **28.10.2026** прошли все 8 тестов и assembly. Подтверждены
запись Kafka → MinIO, восстановление после перезапуска TaskManager, остановка
с savepoint и запуск из него, а также конечный режим `--bounded true`.
Spark получил `records=6`, `unique_offsets=6`, `tombstones=1`.
По завершении тех проверок тестовые Flink-джобы были остановлены, а топик
`orders.events` создан пустым для пользовательского producer. Это результаты
того запуска; текущее состояние контейнеров проверяй через `docker compose ps -a`.

[Инструкция запуска, контракт данных и восстановление](docs/kafka-to-s3.md)
описывает создание топика, параметры джобы, checkpoints/savepoints,
ограничения exactly-once и настройку внешнего Kafka-кластера.

## Логи и остановка

```powershell
docker compose logs --tail 100 airflow-scheduler spark-worker
docker compose logs --tail 100 flink-taskmanager
docker compose stop
docker compose up -d --wait --wait-timeout 300
```

Для логов, остановки и повторного запуска стенда с Kafka:

```powershell
docker compose --profile kafka logs --tail 100 kafka
docker compose --profile kafka stop
docker compose --profile kafka up -d --wait --wait-timeout 300
```

Перед плановой остановкой Flink сохрани состояние работающей Kafka → S3 джобы
через stop-savepoint. Запуск контейнеров не отправляет остановленную джобу заново.
Kafka хранит сообщения в volume `jobs-lab_kafka-data`; исходный CSV, Parquet,
checkpoints и savepoints сохраняются в MinIO volume `jobs-lab_minio-data`.

`docker compose down` удаляет контейнеры и сеть, сохраняя данные в volumes.
Команда `docker compose down --volumes` также удалит базы, S3-объекты, логи
и кеши сборки этого стенда: используй её только для полного сброса.
Если использовался профиль Kafka, указывай его и при удалении контейнеров:
`docker compose --profile kafka down`. Добавление `--volumes` также удалит
сохранённые сообщения Kafka.

Основа конфигурации: [Airflow 2.11.2 в Docker](https://airflow.apache.org/docs/apache-airflow/2.11.2/howto/docker-compose/index.html),
[официальные образы Spark](https://github.com/apache/spark-docker),
[Flink 1.18 в Docker](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/resource-providers/standalone/docker/).
