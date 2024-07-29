download_jars:
	mkdir jars
	curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
	curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
	curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.4.3/postgresql-42.4.3.jar
	curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar
	mv *.jar jars/

run_all:
	docker compose -f docker-compose.yaml up -d
	docker compose -f stream-docker-compose.yaml up -d
	docker compose -f airflow-docker-compose.yaml up -d --build

stop_all:
	docker compose -f docker-compose.yaml down
	docker compose -f stream-docker-compose.yaml down
	docker compose -f airflow-docker-compose.yaml down