# XKCD Searchable Database

Prüfungsleistung im fünften Semester an der DHBW Stuttgart im Studiengang Informatik im Wahlfach Big Data ([Kursrepo](https://github.com/marcelmittelstaedt/BigData)).
Aufgabenstellung: [Use XKCD API To Build A Searchable Database](https://github.com/marcelmittelstaedt/BigData/blob/master/slides/winter_semester_2026-2027/5_Practical%20Exam.pdf)

![Comic: Eine Strichfigur erklärt stolz den dreistufigen ETL-Workflow, die zweite fragt "Und wo läuft das?" - "Auf einer VM, die ich nach der Prüfung vergessen habe.](docs/comic.png)
*Comic: eigene Zeichnung, angelehnt an [xkcd](https://xkcd.com) von Randall Munroe ([CC BY-NC 2.5](https://creativecommons.org/licenses/by-nc/2.5/))*

## Überblick

Das Projekt baut eine durchsuchbare Datenbank der xkcd-Comics auf:
1. Über die JSON-API (<https://xkcd.com/info.0.json>) wird der aktuellste Comic abgerufen. Ausgehend von dessen Nummer werden die vorherigen Comics automatisch durchlaufen, und die JSON-Dateien werden in HDFS nach Erscheingungsjahr abgelegt.
2. Ein zweiter ETL-Schritt liest die JSON-Dateien aus, transformiert sie und legt das Ergebnis als CSV in HDFS ab.
3. Ein dritter ETL-Schritt lädt die CSV-Dateien in die MySQL-Tabelle `comics`.
4. Ein Flask-Frontend ermöglicht die Volltextsuche über diese Tabelle.

## Architektur

![Architektur: Der Airflow-DAG xkcd_etl holt Comics von der xkcd-API, speichert sie in HDFS RAW, transformiert sie mit PySpark nach HDFS Final und lädt sie in MySQL. Ein Flask-Frontend fragt MySQL per FULLTEXT-Suche ab.](docs/fig_architektur.png)
*Vereinfachte Darstellung der Architektur: oben der ETL-Workflow (Schritt 1 bis 6), unten der Suchablauf (Schritt A bis D)*

Der komplette Workflow ist als Airflow-DAG `xkcd_etl` (dags/xkcd_etl.py) implementiert und läuft automatisch täglich (`@daily`).
Task-Kette: `extract_xkcd >> transform_final >> load_mysql`.

| Schritt | Beschreibung |
|---|---|
| 1 | `extract_xkcd` ruft die API per HTTP GET ab (nur fehlende Comics) |
| 2 | Die JSON-Dateien werden per HDFS Put in `/user/hadoop/xkcd/raw/<jahr>/` gespeichert |
| 3 | `transform_final` liest die JSON-Dateien mit Spark |
| 4 | Spark schreibt das bereinigte CSV nach `/user/hadoop/xkcd/final/` |
| 5 | `load_mysql` liest das CSV mit Spark |
| 6 | UPSERT der Daten in die MySQL-Tabelle `comics`|
| A-D | Suchanfrage des Nutzers → SQL-Query des Frontends → Ergebnis von MySQL → Anzeige der Treffer |

## Voraussetzungen

- **Betriebssystem:** Linux (getestet auf Ubuntu 20.04 / GCP-VM)
- **Hardware:** mind. 4 GB RAM frei für die Container (empfohlen 6 GB; der Stack betreibt Hadoop/Spark, Airflow, MySQL und Web parallel)
- **Docker inkl. Compose-Plugin:**

```bash
curl -fsSL https://get.docker.com | sudo sh
```
    
- **git**
- **Freie Ports** auf dem Host: `80` (Web), `8080` (Airflow UI), `9870`/`8088` (HDFS/YARN UI)
- **Internetzugriff** (xkcd-API, Docker Hub) 

## Setup & Start
```bash
git clone https://github.com/inf24234/xkcd-search-database.git
cd xkcd-search-database
docker compose up -d --build
```
Beim allerersten Start initialisieren alle Dienste, Status prüfen:

```bash
docker compose ps
docker compose logs -f airflow      # bis der Webserver läuft
```

**Bedienung:**

| Dienst | URL |
|---|---|
| Airflow UI | `http://<host>:8080` |
| Web-Frontend | `http://<host>` |
| HDFS NameNode UI | `http://<host>:9870` |
| YARN UI | `http://<host>:8088` |

Der DAG `xkcd_etl` läuft täglich automatisch. Für den ersten Durchlauf kann er in der Airflow UI manuell gestartet werden.

## ETL-Workflow im Detail

### 1. extract_xkcd (etl/extract_xkcd.py)
Lädt nur die fehlenden Comics, die noch nicht unter `/user/hadoop/xkcd/raw/<jahr>` als JSON in HDFS liegen. Der neueste Comic und seine Nummer stammen von <https://xkcd.com/info.0.json>; von dort werden die vorherigen Nummern durchlaufen.
- Fehlgeschlagene Requests werden mit exponentiellen Backoff wiederholt.
- HTTP-Statuscode 404 werden übersprungen.
- Zwischen zwei Request liegt eine Pause (Standard 0,5s), um die API zu schonen.
- Die JSON-Dateien werden nach ihrem Erscheingungsjahr partioniert abgelegt.

### 2. transform_final (etl/transform_final.py)
PySpark auf dem Hadoop-Cluster:
- Das Publikationsdatum (`pub_date`) wird aus `year`, `month` und `day` zusammengesetzt.
- `title` dient als Fallback, falls `safe_title` fehlt.
- Deduplizierung über `num`.
- `search_text` wird auf `title` und `alt` zusammengesetzt.

### 3. load_mysql (etl/load_mysql.py)
Spark liest das CSV partitioniert. `foreachPartition` schreibt die Daten mit `pymysql` in MySQL, als UPSERT über `ON DUPLICATE KEY UPDATE`. Dadurch kann der DAG beliebig oft laufen, ohne dass Comics doppelt in der Datenbank landen.


### 4. Web-Frontend (web/)
Flask-Anwendung mit FULLTEXT-Suche auf `search_text`. Die Treffer werden nach num absteigend sortiert (neuste zuerst), nicht nach Relevanz. Pro Suche werden maximal 100 Treffer angezeigt.

## Datenmodell

| Layer | Speicherort | Inhalt |
|---|---|---|
| Raw | HDFS, partitioniert nach Jahr | Orginal-JSON der API |
| Final | HDFS | CSV mit `num`, `pub_year`, `title`, `link`, `img`, `alt`, `search_text` |
| End-User-DB | MySQL-Tabelle `comics` (sql/comics.sql) | Daten des Final-Layers, FULLTEXT-Index auf search_text

## Projektstruktur
```text
.
├── airflow-image
│   ├── Dockerfile
│   └── startup.sh
├── dags
│   └── xkcd_etl.py
├── docker-compose.yml
├── docs
│   ├── comic.png
│   ├── fig_architektur.drawio
│   └── fig_architektur.png
├── etl
│   ├── extract_xkcd.py
│   ├── load_mysql.py
│   └── transform_final.py
├── hadoop-image
│   ├── Dockerfile
│   └── startup.sh
├── README.md
├── sql
│   └── comics.sql
└── web
    ├── app.py
    ├── Dockerfile
    └── templates
```

## Lizenz und Quellen
- Die Comics und Metadaten stammen von [xkcd.com](https://xkcd.com) (Randall Munroe) und stehen unter [CC BY-NC 2.5](https://creativecommons.org/licenses/by-nc/2.5/).
- Die Nutzung der API folgt der [xkcd-Dokumentation](https://xkcd.com/json.html).