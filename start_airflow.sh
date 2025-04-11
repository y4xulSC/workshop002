#!/bin/bash

echo "Inicializando entorno Airflow..."

export AIRFLOW_HOME="$(pwd)/airflow"
echo "AIRFLOW_HOME configurado en: $AIRFLOW_HOME"

echo "Ejecutando Airflow en modo standalone..."
airflow standalone