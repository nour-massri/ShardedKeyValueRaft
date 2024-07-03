# Sharded Key-Value Raft based Storage Service
Implemented from the ground up based on the Raft Consensus Algorithm [Raft](https://raft.github.io/raft.pdf) with an HTTP interface. 

## Quick start
Clone the repo:
```bash
git clone git@github.com:nour-massri/ShardedKeyValueRaft.git
cd ShardedKeyValueRaft/src
```
Make sure you have Go installed: 
```bash
go version
```
Then Run the webserver
go run webserver.go

It will start 3 replica groups each having 3 machines, you will see something like this:
```none
...
Replica Group 0 started
Replica Group 1 started
Replica Group 2 started
Starting server on :8080...
...
```


Now you can make requests to set and get keys:

```bash
########### Get value

~/ > curl http://localhost:8080/get?key=name

{"value":""}  # empty, we don't have anything yet

########### Put value

~/ > curl -X POST -H 'Content-Type: application/json' -d '{"name":"Nour"}' http://localhost:8080/put

########### Get value again

~/ > curl http://localhost:8080/get?key=name

{"value":"Nour"}  # hooray!

########### Append value

~/ > curl -X POST -H 'Content-Type: application/json' -d '{"name":" Massri"}' http://localhost:8080/append

{"value":"Nour Massri"}  # appended
```

## HTTP API description

API is very simple:

```none
POST /put

    Headers:
        Content-Type: application/jspn

    Request:

        {
            "some-key": "some-value"
        }

---------------------------------

POST /append

    Headers:
        Content-Type: application/jspn

    Request:

        {
            "some-key": "some-value"
        }

---------------------------------

GET /key?key=some-key

    Response:
        {
            "value": "some-value"
        }
```

## TODO
* Dockersize the machines and the testing framework
* More documentation on implmentation details
* Webserver GUI 
