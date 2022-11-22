# WF_kakfa

Preparation
---
```
python3 -m venv venv
. ./venv/bin/activate # or ./venv/scripts/activate if windows
python3 -m pip install -U pip pip-tools wheel
pip-sync

Confluence setup;
Have a Confluence cloud account created and have setup to create a cluster and topic. (topic_3)
	

Mongo DB setup;
In your local mongo DB app create a new DB with name "kafkaDB" and collection with the name "collection".
if you are importing the users.json under any different db or collection make sure to update the mydb and mycol parameters in mongo_collection() function in __main__.py.
```

Execution
---
```
And have the configuration details provided on both producer and consumer code.

Run the consumer code first and then run producer code to not to miss any messages produced.
```
