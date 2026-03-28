**Ejecución de los Jobs**
---
Tal cual esta, se descarla la carpeta en el directorio  raiz.

    cd app

Ejecución del job 1.

    /opt/spark/bin/spark-submit \
    --master spark://172.31.68.230:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    limpieza.py

Ejecución del job 2.

    /opt/spark/bin/spark-submit \
      --master spark://172.31.68.230:7077 \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      --jars /root/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar \
      analisis.py
