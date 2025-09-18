import subprocess
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def wait_for_postgres(host, max_retries=10, delay_seconds=10):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True 
            )
            if "accepting connections" in result.stdout.lower():
                logging.info(f"Successfully connected to Postgres host: {host}")
                return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Error connecting to Postgres host {host}: {e}")
            retries += 1
            logging.warning(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})"
                )
            time.sleep(delay_seconds)
    logging.critical(f"Max retries reached for host {host}. Exiting.")
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)

if not wait_for_postgres(host="destination_postgres"):
    exit(1)

logging.info("Starting ELT script")

source_config = {
    "dbname": "source_db",
    "user": "postgres",
    "password": "secret",
    "host": "source_postgres"
}

destination_config = {
    "dbname": "destination_db",
    "user": "postgres",
    "password": "secret",
    "host": "destination_postgres"
}

dump_command = [
    "pg_dump",
    "-h", source_config["host"],
    "-U", source_config["user"],
    "-d", source_config["dbname"],
    "-f", "data_dump.sql",
    "-w"
]

subprocess_env = dict(PGPASSWORD=source_config["password"])

try:
    subprocess.run(dump_command, env=subprocess_env, check=True)
    logging.info("Database dump successful.")
except subprocess.CalledProcessError as e:
    logging.error(f"Error during database dump: {e}")
    exit(1)

load_command = [
    "psql",
    "-h", destination_config["host"],
    "-U", destination_config["user"],
    "-d", destination_config["dbname"],
    "-a", "-f", "data_dump.sql"
]

subprocess_env = dict(PGPASSWORD=destination_config["password"])

try:
    subprocess.run(load_command, env=subprocess_env, check=True)
    logging.info("Database load successful.")
except subprocess.CalledProcessError as e:
    logging.error(f"Error during database load: {e}")
    exit(1)

logging.info("Ending ELT script...")
