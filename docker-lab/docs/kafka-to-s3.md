# Kafka → S3: сырой слой на Scala/Flink

`lab.kafka.KafkaToS3` переносит записи Kafka в Parquet со Snappy через
`KafkaSource` и `FileSink`. Это отдельный sbt-модуль `kafkaToS3`; существующие
проверки Spark и Flink собираются как раньше. Генератор синтетики пользователь
реализует самостоятельно.

## Данные

Payload не разбирается: JSON, Avro, Protobuf и произвольные байты проходят
одинаково. Невалидный JSON тоже сохраняется. Поэтому на этом слое нет DLQ:
проверка бизнес-схемы и карантин ошибочных событий относятся к следующей джобе.
Ошибка чтения Kafka или записи S3 завершает попытку с ошибкой, запись не пропускается.

Встроенная схема: [kafka-envelope-v1.avsc](../jobs/scala/kafka-to-s3/src/main/resources/kafka-envelope-v1.avsc).

| Поле | Содержимое |
| --- | --- |
| `schema_version` | Версия envelope: `1` |
| `topic`, `partition`, `offset` | Координаты исходной записи; идентификатор внутри одного Kafka-кластера и жизненного цикла топика |
| `kafka_timestamp_ms`, `timestamp_type` | Исходные Kafka timestamp и его тип; отсутствующий timestamp остаётся `-1` |
| `ingested_at_ms` | Время чтения во Flink, Unix milliseconds |
| `key`, `value` | Nullable binary; пустой массив отличается от null |
| `headers` | Массив `{key: string, value: nullable binary}` с сохранением порядка и повторяющихся ключей |

Kafka tombstone (`value=null`) сохраняется. Он не удаляет предыдущие записи
из сырого слоя. Бизнес-дубли, отправленные producer как разные Kafka-записи,
также сохраняются. Разбор JSON и бизнес-дедупликацию можно выполнить позже в Spark.

Пример объекта:

```text
s3a://raw/kafka/orders-v1/topic=orders.events/ingest_date=2026-10-28/ingest_hour=12/part-....parquet
```

Каталоги используют время приёма в UTC. Это не event time из payload.
Партиционирования по user ID, ключу сообщения или Kafka partition нет:
оно увеличивало бы число мелких файлов. Порядок строк в Parquet не гарантирован;
порядок внутри исходной Kafka partition восстанавливается по `offset`.

## Подготовка стенда

Все команды выполняются из `docker-lab/` в PowerShell.

```powershell
docker compose config --quiet
docker compose --profile kafka up -d --wait --wait-timeout 180 kafka
docker compose up -d --wait --wait-timeout 180 flink-jobmanager flink-taskmanager
docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic orders.events --partitions 2 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete
docker compose run --rm -T sbt "kafkaToS3/test" "kafkaToS3/assembly"
```

Профиль `kafka` необязателен и не запускается с обычным `docker compose up`.
Новый брокер изолирован в `jobs-lab_default`, использует KRaft и volume
`jobs-lab_kafka-data`. Автосоздание топиков отключено.
Одноразовый `kafka-init` назначает владельца корня volume (`1000:1000`)
и завершается с кодом `0`; сам брокер работает как непривилегированный `appuser`.

Для своего producer используй:

| Где producer | Bootstrap servers |
| --- | --- |
| На компьютере | `localhost:19092` или порт `KAFKA_PORT` из `.env` |
| В сети `jobs-lab_default` | `kafka:9092` |

Топик примера: `orders.events`. Можно отправлять UTF-8 JSON; обязательных
бизнес-полей нет. Для надёжной отправки задай producer `acks=all` и
`enable.idempotence=true`. Локальный брокер один, replication factor равен `1`;
это учебная конфигурация без отказоустойчивости брокера. Он добавляет примерно
1 ГБ к потреблению памяти стенда.

## Первый запуск

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

Сохрани JobID из вывода. Flink UI: http://localhost:18083.
На первом старте читаются все доступные записи (`earliest`). Запускай одну
активную копию на логический поток; одинаковый `group-id` не служит блокировкой
между двумя независимо запущенными KafkaSource.

Новые файлы появляются после успешного checkpoint и завершения commit.
По умолчанию checkpoint проходит каждые 30 секунд. На пустом потоке файлов
может не быть; `_SUCCESS` у непрерывной джобы не создаётся.

| Параметр | По умолчанию | Назначение |
| --- | --- | --- |
| `--bootstrap-servers` | Обязателен | Адреса, доступные обоим Flink-контейнерам |
| `--topics` | Обязателен | Один топик или список через запятую |
| `--group-id` | Обязателен | Отдельный consumer group для логического потока |
| `--output` | Обязателен | S3-префикс для Parquet |
| `--checkpoint-dir` | Обязателен | Отдельный S3-префикс состояния |
| `--starting-offsets` | `earliest` | `earliest`, `latest` или `committed`; только для состояния без сохранённых offsets |
| `--parallelism` | `2` | От `1` до `128`; в стенде всего два slots |
| `--checkpoint-interval-seconds` | `30` | Период checkpoint и закрытия Parquet-файлов |
| `--checkpoint-timeout-seconds` | `120` | Лимит времени checkpoint, не меньше интервала |
| `--max-file-size-mb` | `128` | Порог дополнительного закрытия файлов; приблизительный из-за буферизации Parquet |
| `--bounded` | `false` | `true`: конечное чтение до offsets, определённых при старте |
| `--kafka-properties` | Нет | Файл настроек TLS/SASL и Kafka tuning |

`--starting-offsets latest` пропускает историю, существовавшую до старта.
`committed` требует уже имеющихся group offsets, иначе запуск завершается
ошибкой. При восстановлении offsets из состояния Flink имеют приоритет.
При выходе offsets за retention автоматический reset запрещён.

## Остановка и восстановление

Плановая остановка с savepoint:

```powershell
$flinkJobId = "JOB_ID_ИЗ_ВЫВОДА_ЗАПУСКА"
docker compose exec -T flink-jobmanager flink stop --savepointPath s3a://processed/flink/savepoints/orders-raw-v1 $flinkJobId
```

Возьми полный путь savepoint из ответа и восстанови с теми же `jobArgs`:

```powershell
$savepoint = "s3a://processed/flink/savepoints/orders-raw-v1/savepoint-..."
docker compose exec -T flink-jobmanager flink run -d -s $savepoint -c lab.kafka.KafkaToS3 $jobJar @jobArgs
```

При сбое TaskManager Flink автоматически восстанавливается из последнего
успешного checkpoint; допускаются три рестарта за десять минут с паузой 10 секунд.
Checkpoint сохраняет offsets и состояние незавершённых файлов согласованно.
Он остаётся в S3 при отмене задачи. После потери JobManager в этом standalone
стенде нужно вручную подать `flink run -s` с последним завершённым checkpoint
(`.../<job-id>/chk-N`) или с актуальным stop-savepoint. Обычный повторный
`flink run` не подхватывает checkpoint только по `--checkpoint-dir`.

Гарантия exactly-once относится к восстановлению одной линии исполнения
с согласованным состоянием KafkaSource/FileSink. Сохраняй прежние output,
topics, Kafka-кластер, operator UID и совместимую схему. Новая задача без
восстановления, откат на старое состояние после последующих commit или параллельная
запись в тот же output могут привести к повторам или ошибке восстановления.
Offsets consumer group используются для мониторинга и выбора начальной позиции;
они не заменяют состояние FileSink.

Для намеренного replay/backfill используй новые output, checkpoint prefix и
group-id. `--bounded true` подходит для конечного снимка, но не превращает
повторный запуск в идемпотентный. Условие завершения фиксируется при старте:
данные, поступившие позже, остаются для следующей обработки.

## Файлы, наблюдаемость и внешние кластеры

Bulk FileSink закрывает файлы на каждом checkpoint, а не ждёт строго 128 МБ.
При небольшом потоке получатся маленькие файлы. Для больших объёмов подбирай
интервал checkpoint и parallelism по нагрузке; downstream compaction можно
реализовать отдельной Spark-джобой. Row group Parquet ограничен 32 МиБ.

FileSink использует multipart upload через уже включённый
`flink-s3-fs-hadoop`. Для output явно используется `s3a://`.
Читай только завершённые `.parquet`-объекты; commit нескольких файлов не является
атомарным снимком всей таблицы. Для транзакционного чтения таблицы и управления
схемой следующий шаг — табличный формат, например Iceberg.
Lifecycle очистки незавершённых multipart uploads должен учитывать максимальный
период простоя и восстановления из savepoint.

В Flink UI наблюдай Completed/Failed Checkpoints, duration, Kafka lag,
backpressure и перезапуски. У source есть счётчики `raw_records` и
`raw_tombstones`; они диагностические и могут повторно увеличиваться при replay.
В production нужны алерты на отсутствие успешных checkpoint, рост lag,
ошибки записи S3, недоступность брокеров и исчерпание retention.

Для внешней Kafka профиль `kafka` не нужен. Передай адреса, которые доступны
Flink, включая все broker addresses из `advertised.listeners`.
`localhost` внутри контейнера означает сам контейнер. Для брокера на Windows
обычно используется `host.docker.internal` с соответствующим advertised listener.

Скопируй `config/kafka/client.properties.example` в `client.properties`, настрой
TLS/SASL и добавь `--kafka-properties /opt/lab/config/kafka/client.properties`.
Каталог подключён к обоим Flink-контейнерам только для чтения; файлы с секретами
и ключами игнорируются Git. Джоба фиксирует `read_committed`, отключает
auto-commit и отправляет Kafka offsets на checkpoint. В properties эти параметры
переопределять нельзя. Не передавай пароли через CLI.

Демонстрационные MinIO credentials задаёт существующий Compose. Для AWS S3
настрой filesystem plugin на AWS endpoint и IAM role через конфигурацию
развёртывания, без секретов в Scala. Для production потребуются HA JobManager,
репликация Kafka, защищённые connections и минимальные права на конкретные
topics и S3-префиксы. Учебный Compose эти свойства не обеспечивает.

## Проверки реализации

`kafkaToS3/test` проверяет параметры, запрет небезопасных overrides,
бинарные данные, tombstone, повторяющиеся headers, UTC bucket, Flink/Avro
serialization и настоящий цикл записи/чтения Parquet со Snappy.

Для небольшого тестового S3-префикса можно независимо проверить данные в Spark:

```powershell
docker compose exec -T --user spark spark-gateway /usr/local/bin/submit-spark /opt/lab/jobs/pyspark/verify_kafka_raw.py --input s3a://raw/kafka/TEST_PREFIX --expected-records 3 --expected-tombstones 1
```

Проверка читает завершённые Parquet, сравнивает число записей и tombstone,
проверяет уникальность `(topic, partition, offset)`. Она ничего не записывает.

Фактически проверено 28.10.2026: все 8 тестов и assembly прошли;
Kafka и MinIO приняли поток, TaskManager восстановился из checkpoint,
`flink stop` создал savepoint, новая задача восстановилась из него.
Запись, отправленная во время остановки, была прочитана даже с аргументом
`--starting-offsets latest`, поскольку использовались offsets из savepoint.
Независимое чтение через Spark получило `records=6`, `unique_offsets=6`,
`tombstones=1`. Конечный `--bounded true` завершился успешно и в отдельном
S3-префиксе дал те же показатели. Проверочные задачи остановлены; топик
`orders.events` создан пустым для пользовательского producer.

Документация используемых API:
[KafkaSource](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/kafka/),
[FileSink](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/filesystem/),
[savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/savepoints/),
[официальный Docker-образ Kafka](https://kafka.apache.org/39/getting-started/docker/).
