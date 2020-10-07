# distributed-file-system

This project implements a simple Distributed File System (DFS). It is a service, where user can store his files, which will allocated in several services, and, in case of falling one of it, user will still have an opportunity to get his files. Also, this system is well-scalable: you can endlessly run new storage servers and system will automatically expanded. Our project has an [official wiki](https://github.com/kezzyhko/distributed-file-system/wiki), look it, it is interesting. :)

# Installation

Both Name Server and storage server can be installed by using [docker engine](https://docs.docker.com/engine/install/). You can pull gocker image for Name Server from **kezzyhko/dfs-name-server** and image for Storage server from **bogotolec/dfs-storage-server**. Note that by default, Name Server uses port 1234 and Storage server uses port 6666.

```
docker pull kezzyhko/dfs-name-server
docker run -p 1234:1234 --name name-server -it kezzyhko/dfs-name-server
```

and

```
docker pull bogotolec/dfs-storage-server
docker run -p 6666:666 --name storage-server -it bogotolec/dfs-storage-server
```
You can see more variants of installation and configuration on our [official wiki](https://github.com/kezzyhko/distributed-file-system/wiki/Servers-installation)

# Protocols

For this project, we have used self-implemented protocols. 

First of all, Client knows address of the Name Server and send requests directly to it.

![Client sends request to Name Server](https://github.com/kezzyhko/distributed-file-system/wiki/images/NCC_1.png)

If Client needs to interact with Storage Servers directly, he can ask information about Storage Servers from Name Server.

![Client ask info about Storage Server](https://github.com/kezzyhko/distributed-file-system/wiki/images/CSC_1.png)

![Client ask info about Storage Server](https://github.com/kezzyhko/distributed-file-system/wiki/images/CSC_2.png)

For more comprehensive and technical description, you can refer to [this wiki page](https://github.com/kezzyhko/distributed-file-system/wiki/Protocols)

# Contribution

This project has been made by 3 people

## Ilya Makarenko ([bogotolec](https://github.com/bogotolec))

 - Project management
 - Protocols development
 - Documentaion
 - Storage Server development

## Sergey Semushin ([kezzyhko](https://github.com/kezzyhko))

 - Name Server development
 - Architecture development
 - Docker containers creation

## Ruslan Sahibgareev ([rusya-ink](https://github.com/rusya-ink))

 - Client development
 - Visual design
 - Traffic analysis
