

## Build
mvn compile assembly:single

## Upload a file to ECS bucket

java -jar ecs-s3-example.jar -b **_bucket-name_** -n **_namespace_** -u **_http://ecs-s3-uri:9020_** -f **_filename.txt_**

## Upload a file to ECS bucket and set 60 second retention

java -jar ecs-s3-example.jarcom.dell.jarvis.ECSClient -b **_bucket-name_** -n **_namespace_** -u **_http://ecs-s3-uri:9020_** -f **_filename.txt_** -r **_60_**