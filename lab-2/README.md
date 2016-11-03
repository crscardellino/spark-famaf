# Sessionization Job Spark Submit

Para correr el sessionization job mediante Spark Submit, el siguiente comando es el indicado:

    spark-submit --class SessionizationJob --master local[2] path/to/SessionizationJob.jar -d path/to/dataset -s <minutos>

