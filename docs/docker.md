### Docker
If you want to get started right away, you can bootstrap a marketstore db instance using our latest [docker image](https://hub.docker.com/r/alpacamarkets/marketstore/tags/). The image comes pre-loaded with the default mkts.yml file and declares the VOLUME `/data`, as its root directory. To run the container with the defaults:
```sh
docker run -i -p 5993:5993 alpacamarkets/marketstore:latest
```

If you want to run a custom `mkts.yml` you can create a new container
and load your mkts.yml file into it:
```sh
docker create --name mktsdb -p 5993:5993 alpacamarkets/marketstore:latest
docker cp mkts.yml mktsdb:/etc/mkts.yml
docker start -i mktsdb
```

You can also [bind mount](https://docs.docker.com/storage/bind-mounts/)
the container to a local host config file: a custom `mkts.yml`:
```sh
docker run -v /full/path/to/mkts.yml:/etc/mkts.yml -i -p 5993:5993 alpacamarkets/marketstore:latest
```
This allows you to test out the image [included
plugins](https://github.com/alpacahq/marketstore/tree/master/plugins#included)
with ease if you prefer to skip the copying step suggested above.

By default the container will not persist any written data to your
container's host storage. To accomplish this, bind the `data` directory to
a local location:
```sh
docker run -v "/path/to/store/data:/data" -i -p 5993:5993 alpacamarkets/marketstore:latest
```
Once data is written to the server you should see a file tree layout
like the following that will persist across container runs:
```sh
>>> tree /<path_to_data>/marketstore
/<path_to_data>/marketstore
├── category_name
├── WALFile.1590868038674814776.walfile
├── SYMBOL_1
├── SYMBOL_2
├── SYMBOL_3
```

If you have built the
[cmd](https://github.com/alpacahq/marketstore/tree/master/cmd) package
locally, you can open a session with your running docker instance using:
```sh
marketstore connect --url localhost:5993
```
