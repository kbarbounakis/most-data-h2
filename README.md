# most-data-h2
Most Web Framework H2 Data Adapter
##Install
$ npm install most-data-h2
##Usage
Register H2 adapter on app.json as follows:

    "adapterTypes": [
        ...
        { "name":"H2 Data Adapter", "invariantName": "h2", "type":"most-data-h2" }
        ...
    ],
    adapters: [
        ...
        { "name":"development", "invariantName":"h2", "default":true,
            "options": {
               "url": "jdbc:h2:~/test;AUTO_SERVER=true;AUTO_RECONNECT=true",
               "properties": {
                   "user" : "sa",
                   "password": ""
               }
           }
        }
        ...
    ]

If you are intended to use H2 data adapter as the default database adapter set the property "default" to true.
