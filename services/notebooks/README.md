# Jupyter notebooks

## Howto
When the server starts, you'll get a notification that the notebooks web server can be reached from, e.g.:
```
localhost:8888/?token=2e12afd535c6bed4381fd95bc0cc834573f5b55a78b074da
```
To connect from outside the container simply swap the port with whatever port is used to connect to the docker container, defined in the docker-compose file.

If you are running the container with the root docker-compose file, the connection information mentioned above can be hard to read form the docker-compose output logs. However, you can first get the name of the image and print its logs:
```
docker ps
docker logs <image_name>
```
In our case you will want to replace the port number with the exposed port of the container, which is defined in docker-compose (8001):
```
localhost:8001/?token=...
```
