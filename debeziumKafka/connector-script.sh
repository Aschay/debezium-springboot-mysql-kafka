curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json"  localhost:8083/connectors/ -d '{ 
    "name": "app-db-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "tasks.max": "1",
      "database.hostname": "mysql",
      "database.user": "root",
      "database.password": "root",
      "database.port": "3306",
      "database.dbname": "customerdb",
      "database.server.id": "20178",  
      "topic.prefix": "dbserver" ,
      "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",  
      "schema.history.internal.kafka.topic": "schema-changes.customer"  
    }
}'
curl -i -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d @mysql-connector.json 

curl -i -X GET localhost:8083/connectors

curl -i -X DELETE localhost:8083/connectors/app-db-connector/
