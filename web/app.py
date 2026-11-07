import os
from flask import Flask, render_template, request

MYSQL = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "user": os.environ.get("MYSQL_USER", "xkcd"),
    "password": os.environ.get("MYSQL_PASSWORD", "xkcd"),
    "database": os.environ.get("MYSQL_DATABASE", "xkcd"),
    "charset": "utf8mb4",
}

app = Flask(__name__)

SEARCH = """SELECT num, title, link, img, pub_date
            FROM comics
            WHERE MATCH(search_text) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY num DESC
            LIMIT 100"""

def search_comics(phrase):
    conn = pymysql.connect(**MYSQL)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(SEARCH, (phrase,))
            results = cursor.fetchall()
    finally:
        conn.close()

@app.route("/", methods=["GET", "POST"])
def index():
    phrase = request.args.get("q", "").strip()
    results = search_comics(phrase) if phrase else []
    return render_template("index.html", phrase=phrase, results=results)