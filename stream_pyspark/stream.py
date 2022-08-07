

port_listen = spark.readStream.format("socket").option("host","localhost").option("port","4855").load()

port_output = port_listen.writeStream.format("console").start()

