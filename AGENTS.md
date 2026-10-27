# Контекст проекта для следующих чатов

Последнее обновление: **27 октября 2026 года**.

## Назначение и текущее состояние

Это учебный проект для практики разработки джоб на Scala и PySpark,
обработки данных в Apache Spark и Apache Flink и оркестрации в Apache Airflow 2.
Пользователь общается на русском языке и хочет самостоятельно тренироваться
на практических заданиях.

Docker-стенд уже создан, собран, запущен и проверен. Основная реализация
находится в `docker-lab/`. В проекте есть проверки инфраструктуры и исходные
данные; первая бизнес-джоба агрегации заказов пока не реализована.

Рабочая среда при настройке: Windows, PowerShell, Docker Desktop с Linux
containers. Исходный путь репозитория: `C:\projects\pet projects`.
Пути в командах следует разрешать относительно фактического корня репозитория.
Для выполнения джоб локальные установки Scala, sbt, Java и Python не нужны:
сборка и исполнение проходят внутри контейнеров.

Этот файл описывает состояние и договорённости; новое задание определяет пользователь.
Фактическое состояние запущенных контейнеров проверяй через `docker compose ps -a`.

## Версии и зависимости

| Компонент | Версия / источник |
| --- | --- |
| Scala для учебных джоб | `2.12.18`, `docker-lab/jobs/scala/build.sbt` |
| sbt | `1.10.11`, launcher в Docker и `project/build.properties` |
| sbt-assembly | `2.3.1`, `project/plugins.sbt` |
| Spark / PySpark | `3.5.8`, выбранная конкретная версия Spark 3 |
| Flink | `1.18.1`, образ `flink:1.18.1-scala_2.12-java11` |
| Java для движков и сборки | `11`; patch-версии зависят от базового образа |
| Airflow | `2.11.2`, база `apache/airflow:2.11.2-python3.11` |
| Python в Airflow | `3.11` |
| Airflow Amazon provider | `9.22.0` |
| Airflow PostgreSQL provider | `6.6.0` |
| Airflow SSH provider | `4.3.1` |
| PostgreSQL | major `16`, образ `postgres:16-bookworm` |
| MinIO | исходный выпуск `RELEASE.2025-04-22T22-12-26Z` |
| Hadoop AWS / S3A | `3.3.4` |
| AWS Java SDK bundle в Spark | `1.12.262` |
| PostgreSQL JDBC driver | `42.7.5` |

Точные Python-зависимости Airflow находятся в
`docker-lab/docker/airflow/requirements.txt`. При сборке применяются официальные
constraints Airflow `2.11.2` для Python `3.11`, затем выполняется `pip check`.
`PythonOperator` импортируется из `airflow.operators.python`.

Версия Scala, которую sbt скачивает для собственной работы, может отличаться
от Scala проекта. Учебные джобы компилируются с `2.12.18`; эта же версия
подтверждена запуском Scala-примера в Spark.

## Структура проекта

```text
.
|-- AGENTS.md                       # Основной файл контекста и инструкций для агента
|-- LICENSE                         # MIT для репозитория
|-- .gitignore                      # Общие исключения Git
|-- docker-lab/
|   |-- README.md                   # Инструкция по эксплуатации стенда
|   |-- compose.yaml                # Compose-проект jobs-lab
|   |-- .env.example                # Необязательные настройки внешних портов
|   |-- .gitignore                  # .env, target/, Python-кеши и файлы IDE
|   |-- .gitattributes              # LF для shell-скриптов, Dockerfile и .conf
|   |-- config/
|   |   `-- spark-defaults.conf     # Spark-кластер, ресурсы и подключение S3A
|   |-- dags/
|   |   `-- environment_check.py    # Проверка связки Airflow/S3/SSH/Spark/JDBC
|   |-- data/
|   |   `-- orders.csv              # Семь исходных записей заказов
|   |-- docker/
|   |   |-- airflow/
|   |   |   |-- Dockerfile
|   |   |   `-- requirements.txt
|   |   |-- minio/
|   |   |   `-- Dockerfile          # Сборка MinIO из официального source tag
|   |   |-- sbt/
|   |   |   |-- Dockerfile          # Java 11 и sbt launcher 1.10.11
|   |   |   `-- repositories        # Maven Central через repo.maven.apache.org
|   |   `-- spark/
|   |       |-- Dockerfile          # Spark + SSH + S3A + JDBC
|   |       |-- entrypoint.sh       # Режимы master, worker, gateway
|   |       |-- submit-spark.sh     # Настройка Java/Python и spark-submit
|   |       `-- sshd_config         # SSH-доступ к Spark gateway
|   |-- init/
|   |   |-- minio.py                # Создание бакетов и начальная загрузка CSV
|   |   `-- postgres.sql            # Пользователь lab и база warehouse
|   `-- jobs/
|       |-- pyspark/
|       |   `-- environment_check.py
|       `-- scala/
|           |-- build.sbt           # Подпроекты sparkJob и flinkJob
|           |-- project/
|           |   |-- build.properties
|           |   `-- plugins.sbt
|           |-- spark/src/main/scala/lab/spark/EnvironmentCheck.scala
|           `-- flink/src/main/scala/lab/flink/EnvironmentCheck.scala
`-- .idea/                          # Локальные настройки IDE, игнорируются Git
```

`.git/` содержит метаданные репозитория. `target/` и JAR-файлы в нём генерируются
сборкой, игнорируются Git и не являются исходниками.

## Архитектура Docker-стенда

Имя Compose-проекта: **`jobs-lab`**. Его контейнеры имеют префикс `jobs-lab-`,
сеть — `jobs-lab_default`.

| Сервис Compose | Назначение |
| --- | --- |
| `postgres` | Метаданные Airflow и отдельная учебная база `warehouse` |
| `minio` | Локальное S3-хранилище и web-консоль |
| `minio-init` | Создание бакетов и загрузка исходного CSV; одноразовый сервис |
| `spark-master` | Spark standalone master |
| `spark-worker` | Один worker: 2 ядра, 1536 MB памяти для executors |
| `spark-gateway` | SSH-сервер, запуск `spark-submit` и Spark driver |
| `airflow-init` | Миграции БД и создание пользователя; одноразовый сервис |
| `airflow-webserver` | Web-интерфейс Airflow |
| `airflow-scheduler` | Планировщик и выполнение задач через `LocalExecutor` |
| `flink-jobmanager` | Flink JobManager и REST/UI |
| `flink-taskmanager` | Один TaskManager с двумя slots |
| `sbt` | Одноразовый контейнер сборки, профиль `tools` |

Airflow использует `LocalExecutor`, `parallelism=2`, максимум две активные задачи
на DAG и два процесса webserver. Redis и Celery в этом стенде не требуются.
S3-сенсор работает с `deferrable=False`, отдельного Airflow triggerer нет.

Airflow подключает `dags/` в `/opt/airflow/dags` только для чтения.
Spark и Flink подключают `jobs/` в `/opt/lab/jobs` только для чтения.
Контейнер sbt подключает `jobs/scala/` с правом записи в `/workspace/scala`.
После правки Python-файла он сразу виден в контейнере; Scala-код нужно пересобрать.

Spark запускает driver в `spark-gateway`: master
`spark://spark-master:7077`, deploy mode `client`, driver host `spark-gateway`.
Driver и executor используют по `512m`, `spark.cores.max=2`,
`spark.sql.shuffle.partitions=2`. При прямом запуске используй `--user spark`.
Для SSH предусмотрен пользователь `spark` с домашним каталогом `/home/spark`.

Flink использует Java 11, parallelism `2`, JobManager `1024m`, TaskManager `1536m`.
Включён встроенный плагин `flink-s3-fs-hadoop-1.18.1.jar`.
Интервал checkpoints настроен на `30s`; пути:
`s3://processed/flink/checkpoints` и `s3://processed/flink/savepoints`.
Проверен запуск конечной Flink-джобы; восстановление длительного потока
из checkpoint пока отдельно не проверялось.

## Адреса и учебные учётные данные

Все опубликованные порты привязаны к `127.0.0.1`.
Эти пароли — демонстрационные значения, заданные в конфигурации локального стенда.

| Назначение | Адрес на компьютере | Логин / пароль |
| --- | --- | --- |
| Airflow UI | http://localhost:18080 | `airflow` / `airflow` |
| Spark master UI | http://localhost:18081 | — |
| Spark worker UI | http://localhost:18082 | — |
| Flink UI | http://localhost:18083 | — |
| MinIO Console | http://localhost:19001 | `minioadmin` / `minioadmin` |
| S3 API | http://localhost:19000 | access key / secret key: `minioadmin` / `minioadmin` |
| Учебный PostgreSQL | `localhost:15433`, БД `warehouse` | `lab` / `lab` |
| БД метаданных Airflow | `localhost:15433`, БД `airflow` | `airflow` / `airflow` |
| SSH Spark gateway | `localhost:12222` | `spark` / `spark` |
| Spark driver UI | http://localhost:14040, пока джоба работает | — |

Внешние порты можно переопределить в `docker-lab/.env`, взяв за основу
`.env.example`. Устанавливать `.env` для обычного запуска не обязательно.
Внутри Docker используются имена сервисов и внутренние порты:

| Система | Адрес внутри сети стенда |
| --- | --- |
| Spark master | `spark://spark-master:7077` |
| Spark gateway SSH | `spark-gateway:22` |
| MinIO S3 API | `http://minio:9000`, region `us-east-1`, path-style addressing |
| Учебный JDBC | `jdbc:postgresql://postgres:5432/warehouse` |
| Метаданные Airflow | `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow` |
| Flink REST | `http://flink-jobmanager:8081` |

В Airflow через `AIRFLOW_CONN_*` заданы connections:

- `minio_s3`: AWS credentials и endpoint MinIO.
- `spark_ssh`: пользователь `spark`, хост `spark-gateway`, порт `22`.
- `warehouse`: пользователь `lab`, база `warehouse`, хост `postgres`, порт `5432`.

Эти connections используются hooks и операторами, но не отображаются
в списке connections в UI, поскольку заданы через переменные окружения.

## Исходные данные и готовые примеры

`docker-lab/data/orders.csv` содержит заголовок
`order_id,user_id,order_date,amount,status` и **7 строк данных**.
Есть одинаковый дубль заказа `2`, отменённый заказ `3`, отрицательная сумма
у заказа `4` и заказ `6` за следующий день, `2026-10-27`.

`minio-init` создаёт бакеты `raw` и `processed`, затем загружает CSV в
`s3://raw/orders/2026-10-26/orders.csv`, только если объект ещё отсутствует.
Изменение локального CSV само по себе не перезаписывает уже загруженный объект.
Для Spark используется схема URI `s3a://`, для S3Hook и Flink — соответствующие S3 API/URI.

Готовые проверки:

- DAG `environment_check`: `wait_for_input` (`S3KeySensor`) → `run_spark`
  (`SSHOperator`) → `verify_output` (`PythonOperator`). Расписание отсутствует,
  запуск ручной; `catchup=False`, `max_active_runs=1`, одна повторная попытка.
  Новые DAG по умолчанию создаются на паузе.
- PySpark `jobs/pyspark/environment_check.py`: читает все семь строк из MinIO,
  сохраняет строку `(spark_version, input_rows)` в Parquet
  `s3a://processed/environment-check` и таблицу `warehouse.public.environment_check`.
  Используется `overwrite`; ожидается `('3.5.8', 7)` и S3-объект `_SUCCESS`.
- Scala `lab.spark.EnvironmentCheck`: читает CSV из MinIO, проверяет `rows=7`
  и печатает версии Scala и Spark.
- Scala `lab.flink.EnvironmentCheck`: через Java DataStream API преобразует
  `1, 2, 3` в `10, 20, 30` и выводит результат в логи TaskManager.

## Команды эксплуатации и разработки

Следующие команды выполняются из `docker-lab/`. Из корня репозитория сначала:

```powershell
Set-Location .\docker-lab
```

Проверка конфигурации, сборка и запуск всего стенда:

```powershell
docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 300
docker compose ps -a
```

Нормально, что `airflow-init` и `minio-init` имеют статус `Exited (0)`.
Они должны завершаться, а не работать постоянно.
Для первого запуска нужен доступ к реестрам Docker, GitHub и Maven/PyPI/Go-зависимостям.
Ориентир для Docker: около 10 GB RAM и 15–20 GB диска под образы и кеши.

Сборка Scala-JAR:

```powershell
docker compose run --rm --build -T sbt "sparkJob/assembly" "flinkJob/assembly"
```

Артефакты:

- `jobs/scala/spark/target/scala-2.12/spark-job.jar`.
- `jobs/scala/flink/target/scala-2.12/flink-job.jar`.

Spark и Flink — `Provided`-зависимости. Spark-JAR не включает Scala library,
поскольку её предоставляет Spark; Flink-пример использует Java API из Scala
и собирается со стандартными настройками включения Scala в assembly.
sbt-контейнер ограничен двумя активными CPU и Java heap `1536m`.

Запуск Scala-джоб:

```powershell
docker compose exec -T --user spark spark-gateway /usr/local/bin/submit-spark --class lab.spark.EnvironmentCheck /opt/lab/jobs/scala/spark/target/scala-2.12/spark-job.jar
docker compose exec -T flink-jobmanager flink run -c lab.flink.EnvironmentCheck /opt/lab/jobs/scala/flink/target/scala-2.12/flink-job.jar
```

Запуск PySpark и проверка DAG:

```powershell
docker compose exec -T --user spark spark-gateway /usr/local/bin/submit-spark /opt/lab/jobs/pyspark/environment_check.py
docker compose exec -T airflow-scheduler airflow dags test environment_check 2026-10-26
docker compose exec -T postgres psql -U lab -d warehouse -c "SELECT * FROM public.environment_check;"
```

В UI Airflow можно включить DAG `environment_check` и выполнить Trigger DAG.
При добавлении Python-пакетов меняется `docker/airflow/requirements.txt`
и пересобирается образ. При добавлении Scala-зависимостей меняется `jobs/scala/build.sbt`
и заново выполняется assembly.

Диагностика и остановка:

```powershell
docker compose logs --tail 100 airflow-scheduler spark-worker
docker compose logs --tail 100 flink-taskmanager
docker compose stop
docker compose up -d --wait --wait-timeout 300
```

`docker compose down` удаляет контейнеры и сеть, сохраняя volumes.
`docker compose down --volumes` полностью удаляет данные и кеши этого стенда;
это команда полного сброса, а не обычной остановки.

## Хранение данных

Compose объявляет volumes `postgres-data`, `minio-data`, `airflow-logs`,
`sbt-cache`, `ivy-cache`, `coursier-cache`. Их полные Docker-имена имеют
префикс `jobs-lab_`.

В них сохраняются базы, S3-объекты, логи Airflow и кеши сборки.
Исходники и сгенерированные Scala-JAR находятся в подключённых каталогах проекта.
Не добавляй `target/`, `.env` и кеши в Git.

## Важные решения при настройке

1. На компьютере уже работали другие Docker-проекты: Airflow, Jupyter,
   PostgreSQL, Kafka Connect и другие сервисы. Обычные порты пересекались
   с ними; для этого стенда выбраны отдельные внешние порты `18080–18083`,
   `19000–19001`, `15433`, `12222`, `14040`. Используй Compose-файл этого
   проекта и не останавливай посторонние контейнеры.
2. Исторические готовые образы MinIO во время настройки были недоступны
   в Docker Hub и Quay. Рабочее решение — `docker/minio/Dockerfile`,
   который собирает официальный source tag через Go `1.24.2` и копирует
   бинарник и лицензию в Alpine `3.21`. Go-кеши сохраняет BuildKit.
   Для заполнения S3 используется Python/boto3 из образа Airflow, отдельный `mc` не нужен.
3. Общие образы собираются по одному разу: Airflow — сервисом `airflow-init`,
   Spark — сервисом `spark-master`. Остальные потребители используют тот же
   `image` с `pull_policy: never`. Не дублируй `build` у каждого потребителя:
   при настройке это вызвало конфликт одновременного экспорта одного image tag.
4. Используются явные версии из таблицы. Изменение версии движка требует
   согласовать Docker-образ, Scala-зависимости и ожидаемую версию в проверочном DAG.
5. Текстовые файлы записаны в UTF-8. В Windows PowerShell для чтения русского
   текста используй `Get-Content -Encoding UTF8`; стандартная кодировка старого
   PowerShell может искажать отображение корректного файла.

## Что фактически проверено

Проверки выполнены **27.10.2026** в процессе подготовки стенда:

- `docker compose config --quiet` завершился успешно; Python-файлы прошли
  проверку синтаксиса, shell-скрипты имели LF.
- `docker compose up ... --wait` успешно завершился; основные сервисы были
  запущены, healthchecks прошли, оба init-сервиса завершились с кодом `0`.
- sbt `1.10.11` с плагином sbt-assembly `2.3.1` собрал оба Scala-JAR.
- DAG `environment_check` завершился со статусом `success`:
  S3-сенсор, SSH-аутентификация, удалённый Spark driver, executor на worker,
  чтение CSV, запись Parquet в MinIO и JDBC-запись в PostgreSQL работали.
  Проверка получила `('3.5.8', 7)` и объект `_SUCCESS`.
- Scala/Spark-пример завершился с кодом `0` и вывел
  `Scala 2.12.18, Spark 3.5.8, rows=7`.
- Scala/Flink-пример был отправлен на кластер и успешно завершился;
  CLI сообщил `Program execution finished` и статус finished для JobID.

Это результаты предыдущего запуска, а не обещание текущего состояния Docker.
Для изменений документации повторная сборка контейнеров не нужна.

## Первое учебное упражнение: предложено, ещё не реализовано

Дневная статистика оплаченных заказов на Spark/Scala:

1. Параметры `--date`, `--input`, `--output`.
2. Явная схема CSV, деньги типа `Decimal`; обрабатывать выбранную дату.
3. Строки с неположительной суммой сохранять отдельно как ошибочные.
4. Удалять полностью одинаковые дубли по `order_id`, выбирать `status=paid`.
5. Считать количество заказов и сумму по пользователю, сохранять Parquet.
6. Повторный запуск за день даёт тот же результат без накопления дублей.

Ожидаемый результат за `2026-10-26`:

| user_id | orders_count | total_amount |
| --- | --- | --- |
| 10 | 2 | 150.00 |
| 20 | 1 | 200.00 |

После этой джобы можно тренировать её оркестрацию через Airflow, повторные
попытки и пересчёт дня, затем обработку потока во Flink. Эти этапы остаются
предложениями для обучения и не запускаются автоматически при чтении контекста.

## Как контекст используется следующими чатами

Основной файл контекста и инструкций для агента — `AGENTS.md` в корне репозитория.
Перед началом работы прочитай его и учитывай описанные здесь договорённости.
Автоматическое обнаружение `AGENTS.md` описано в
[официальной документации OpenAI](https://learn.chatgpt.com/docs/agent-configuration/agents-md).
Новый чат должен быть открыт с этим репозиторием как рабочим проектом.
Подробная эксплуатационная инструкция: [docker-lab/README.md](docker-lab/README.md).
При изменениях окружения актуализируй этот файл и README; исторические результаты
проверок обновляй только после фактического выполнения соответствующих команд.
