from datetime import timedelta
import io
import json
import os
import zipfile

import boto3
import pandas as pd
import pendulum
import requests
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


# Config
CRICSHEET_ZIP_URL = "https://cricsheet.org/downloads/ipl_json.zip"
TMP_DIR = "/tmp/ipl_pipeline"
ZIP_PATH = f"{TMP_DIR}/ipl_json.zip"
NEW_MATCH_CSV = f"{TMP_DIR}/match_new.csv"
NEW_BALL_CSV = f"{TMP_DIR}/ball_new.csv"
UPDATED_MANIFEST_PATH = f"{TMP_DIR}/processed_files_updated.json"


def download_zip():
    os.makedirs(TMP_DIR, exist_ok=True)
    with requests.get(CRICSHEET_ZIP_URL, stream=True, timeout=90) as r:
        r.raise_for_status()
        with open(ZIP_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    return ZIP_PATH


def transform_new_files():
    context = get_current_context()
    ti = context["ti"]
    zip_path = ti.xcom_pull(task_ids="download_zip")

    bucket = "de-ipl-daily-bucket"
    manifest_key = "processed_files.json"

    s3 = boto3.client("s3")

    # Read manifest from S3 (first run => empty list)
    try:
        obj = s3.get_object(Bucket=bucket, Key=manifest_key)
        manifest = json.loads(obj["Body"].read().decode("utf-8"))
    except:
        manifest = {"processed_files": []}


    processed_files = set(manifest.get("processed_files", []))

    def safe_first(x, default=None):
        return x[0] if isinstance(x, list) and len(x) > 0 else default

    def sort_key(name):
        base = os.path.splitext(os.path.basename(name))[0]
        return (0, int(base)) if base.isdigit() else (1, base.lower())

    match_rows = []
    ball_rows = []
    new_processed = []

    with zipfile.ZipFile(zip_path, "r") as z:
        json_files = [n for n in z.namelist() if n.lower().endswith(".json")]
        json_files = sorted(json_files, key=sort_key)
        new_json_files = [n for n in json_files if n not in processed_files]

        for jf in new_json_files:
            with z.open(jf) as f:
                match = json.load(f)

            info = match.get("info", {})
            innings_list = match.get("innings", [])
            teams = info.get("teams", [])

            match_id = os.path.splitext(os.path.basename(jf))[0]
            match_date = safe_first(info.get("dates", []))
            season = info.get("season")
            venue = info.get("venue")
            toss_winner = info.get("toss", {}).get("winner")
            toss_decision = info.get("toss", {}).get("decision")
            player_of_match = safe_first(info.get("player_of_match", []))

            match_rows.append(
                {
                    "match_id": match_id,
                    "match_date": match_date,
                    "season": season,
                    "venue": venue,
                    "team1": teams[0] if len(teams) > 0 else None,
                    "team2": teams[1] if len(teams) > 1 else None,
                    "toss_winner": toss_winner,
                    "toss_decision": toss_decision,
                    "player_of_match": player_of_match,
                }
            )

            for innings_no, innings in enumerate(innings_list, start=1):
                batting_team = innings.get("team")
                bowling_team = None
                if len(teams) == 2 and batting_team in teams:
                    bowling_team = teams[0] if teams[1] == batting_team else teams[1]

                for over_obj in innings.get("overs", []):
                    over_no = over_obj.get("over")
                    for ball_no_in_over, d in enumerate(over_obj.get("deliveries", []), start=1):
                        runs = d.get("runs", {})
                        extras = d.get("extras", {}) if isinstance(d.get("extras"), dict) else {}
                        wickets = d.get("wickets", []) if isinstance(d.get("wickets"), list) else []

                        wicket_flag = 1 if wickets else 0
                        dismissal_kind = None
                        player_out = None
                        fielders = None

                        if wicket_flag:
                            w = wickets[0]
                            dismissal_kind = w.get("kind")
                            player_out = w.get("player_out")
                            f_list = w.get("fielders", [])
                            names = []
                            if isinstance(f_list, list):
                                for fx in f_list:
                                    if isinstance(fx, dict):
                                        names.append(fx.get("name"))
                                    elif isinstance(fx, str):
                                        names.append(fx)
                            fielders = ", ".join([x for x in names if x])

                        ball_rows.append(
                            {
                                "match_id": match_id,
                                "match_date": match_date,
                                "season": season,
                                "innings_no": innings_no,
                                "over_no": over_no,
                                "ball_no_in_over": ball_no_in_over,
                                "batter": d.get("batter"),
                                "non_striker": d.get("non_striker"),
                                "bowler": d.get("bowler"),
                                "batting_team": batting_team,
                                "bowling_team": bowling_team,
                                "runs_batter": runs.get("batter", 0),
                                "runs_extras": runs.get("extras", 0),
                                "runs_total": runs.get("total", 0),
                                "extras_byes": extras.get("byes", 0),
                                "extras_legbyes": extras.get("legbyes", 0),
                                "extras_noballs": extras.get("noballs", 0),
                                "extras_wides": extras.get("wides", 0),
                                "extras_penalty": extras.get("penalty", 0),
                                "wicket": wicket_flag,
                                "dismissal_kind": dismissal_kind,
                                "player_out": player_out,
                                "fielders": fielders,
                            }
                        )

            new_processed.append(jf)

    match_df_new = pd.DataFrame(match_rows)
    ball_df_new = pd.DataFrame(ball_rows)

    # Small cleanup
    for df in [match_df_new, ball_df_new]:
        if "season" in df.columns:
            df["season"] = df["season"].astype(str).str.strip()
            df["season"] = df["season"].replace({"2007/08": "2008", "2009/10": "2010", "2020/21": "2020"})
            df["season"] = df["season"].str.extract(r"(\d{4})", expand=False)

    team_map = {
        "Delhi Daredevils": "Delhi Capitals",
        "Deccan Chargers": "Sunrisers Hyderabad",
        "Kings XI Punjab": "Punjab Kings",
        "Rising Pune Supergiants": "Rising Pune Supergiant",
        "Pune Warriors": "Pune Warriors India",
        "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
    }

    for col in ["team1", "team2", "toss_winner", "batting_team", "bowling_team"]:
        if col in match_df_new.columns:
            match_df_new[col] = match_df_new[col].replace(team_map)
        if col in ball_df_new.columns:
            ball_df_new[col] = ball_df_new[col].replace(team_map)

    match_df_new.to_csv(NEW_MATCH_CSV, index=False)
    ball_df_new.to_csv(NEW_BALL_CSV, index=False)

    updated_manifest = {
        "last_updated_utc": pendulum.now("UTC").to_iso8601_string(),
        "processed_files": sorted(list(processed_files.union(set(new_processed))), key=sort_key),
    }
    with open(UPDATED_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(updated_manifest, f, indent=2)

    return {
        "new_file_count": len(new_processed),
        "match_new_csv": NEW_MATCH_CSV,
        "ball_new_csv": NEW_BALL_CSV,
        "updated_manifest_path": UPDATED_MANIFEST_PATH,
    }


def upload_to_s3():
    context = get_current_context()
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="transform_new_files")

    
    bucket = "de-ipl-daily-bucket"
    manifest_key = "processed_files.json"
    match_key = "match_df.csv"
    ball_key = "ball_by_ball_df.csv"

    s3 = boto3.client("s3")

    def read_csv_or_empty(key):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(io.StringIO(obj["Body"].read().decode("utf-8")))
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"NoSuchKey", "404"}:
                return pd.DataFrame()
            raise

    try:
        if payload["new_file_count"] > 0:
            old_match_df = read_csv_or_empty(match_key)
            old_ball_df = read_csv_or_empty(ball_key)

            new_match_df = pd.read_csv(payload["match_new_csv"])
            new_ball_df = pd.read_csv(payload["ball_new_csv"])

            final_match_df = pd.concat([old_match_df, new_match_df], ignore_index=True)
            if "match_id" in final_match_df.columns:
                final_match_df = final_match_df.drop_duplicates(subset=["match_id"], keep="last")

            final_ball_df = pd.concat([old_ball_df, new_ball_df], ignore_index=True)
            ball_pk = ["match_id", "innings_no", "over_no", "ball_no_in_over"]
            if all(c in final_ball_df.columns for c in ball_pk):
                final_ball_df = final_ball_df.drop_duplicates(subset=ball_pk, keep="last")

            s3.put_object(Bucket=bucket, Key=match_key, Body=final_match_df.to_csv(index=False).encode("utf-8"))
            s3.put_object(Bucket=bucket, Key=ball_key, Body=final_ball_df.to_csv(index=False).encode("utf-8"))

        with open(payload["updated_manifest_path"], "r", encoding="utf-8") as f:
            s3.put_object(Bucket=bucket, Key=manifest_key, Body=f.read().encode("utf-8"))
    finally:
        # local cleanup
        for path in [ZIP_PATH, NEW_MATCH_CSV, NEW_BALL_CSV, UPDATED_MANIFEST_PATH]:
            if os.path.exists(path):
                os.remove(path)


with DAG(
    dag_id="ipl_incremental_dag",
    start_date=pendulum.datetime(2026, 5, 1, tz="Asia/Kolkata"),
    schedule="0 11 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=3)},
) as dag:
    t1 = PythonOperator(task_id="download_zip", python_callable=download_zip)
    t2 = PythonOperator(task_id="transform_new_files", python_callable=transform_new_files)
    t3 = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)

    t1 >> t2 >> t3
