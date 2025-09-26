
### Run ibm db2 docker container

```bash
    docker run -itd --name mydb2 --privileged=true -p 50000:50000 -e LICENSE=accept -e DB2INST1_PASSWORD=root -e DBNAME=testdb -v data:/database ibmcom/db2
```