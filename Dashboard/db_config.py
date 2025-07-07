import yaml
import psycopg2
import os

def obtener_config_dw(path="config/warehouse_db.yml"):
    with open(path, "r") as file:
        config = yaml.safe_load(file)
    return config

def obtener_conexion():
    config = obtener_config_dw()

    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"]
    )
    return conn
