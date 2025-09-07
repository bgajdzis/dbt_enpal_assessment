## Setup

1. Download Docker Desktop (if you don’t have installed) using the official website, install and launch.
2. Clone the repo to your device.
3. Open your Command Prompt or Terminal, navigate to that folder, and run the command `docker compose up`.
4. Now you have launched a local Postgres database with the following credentials:
```
    Host: localhost
    User: admin
    Password: admin
    Port: 5432 
```
5. Connect to the db via a preferred tool.
6. Install `dbt-core`, `dbt-postgres`, `sqlfluff`, and `sqlfluff-templater-dbt` using pip (if you don’t have) on your preferred environment, using the most recent version of python.
7. Now you can run `dbt seed` to populate the static lookup tables and `dbt run` to build the project.
