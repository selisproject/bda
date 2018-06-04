SELIS Big Data Analytics Module - Docker
========================================


About
-----
This set of Dockerfiles is intended for setting up a local development 
environment. All required databases and modules are containerized and 
reasonable defaults are provided. This setup is not for production usage.


Getting Started
---------------
Assuming that we have a running `docker` installation and access to the 
internet we can setup the SELIS BDA locally by executing 

```
./sls.sh run all
```

This command will pull, build required images, create volumes, create a network
and launch containers for the BDA's components.

A certain convention is used for naming things, for more details look 
at `./sls.sh`.

Keycloak Setup
--------------
The steps to setup Keycloak server, after executing `./sls.sh run all`, are:

1. Go [here](http://127.0.0.1:8989/auth/), choose `Administration Console` and
   login using the credentials: `selis-admin/123456`.

2. Create a new `realm` by choosing `Add realm` and name it `selis-bda-realm`.

3. Create a new `Client` by choosing `Clients > Create` and name it
   `selis-bda-app`.

4. Set `Authorization Enabled` to `ON`.

5. Set `Valid Redirect URIs` to `http://127.0.0.1:9999/*`.

6. Go to `Installation` tab, choose the `Keycloak OIDC JSON` format and click
   `Download`.

7. Save the created `.json` locally and copy the `secret` to  `bda.properties`.

8. Go to `Roles` and click `Add Role`, set the role name to `selis-user-role`.

9. Go to `Users`, click `Add user` and set username to `selis-user` 
   and password to `123456`.

10. In the `Role Mappings` tab add a `selis-user-role` to the user.

11. Go to `Clients > Service Account Roles` and add the `selis-user-role` to
    the assigned roles.

12. Compile and run the BDA server.
