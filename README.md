# Connect-Multiple-Websocket-Stream-and-Pull-Data-to-PostgreSQL

Connect multiple Websocket Streams at the same time using python threads.
This code made for create custom data storage. 

-in this code,
we are connectting multiple websocket streams, each stream has its own on_message() function. The real time data coming in that functions.
It allow us to make different operations each on_message() individually. Create websocket connection threads as daemon threads, because main
thread ends the demon threads will stop-or terminate-or killed automatically. Otherwise, ctrl+c will never work to terminate program.
Then, Saving data to PostgreSQL in Python script.

NOTE :
This project made for taking real-time data from Binance websocket stream. The company may change their Websocket URLs. 
You should add the current URLs in the URLS list.
