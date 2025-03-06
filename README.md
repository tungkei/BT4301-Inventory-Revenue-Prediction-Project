# Assignment 1

## Dependencies

- Docker
- Python
- [WWI dataset](https://www.kaggle.com/datasets/pauloviniciusornelas/wwimporters)

## Structure

```
.
├── airflow/
│   ├── dags/
│   ├── Dockerfile
│   └── requirements.txt
├── mlflow/
├── scripts/
│   └── load_data/
│       ├── Dockerfile
│       ├── main.py
│       └── requirements.txt
├── compose.yaml
└── README.md
```

- The `./airflow` directory is for airflow related files and configuration
- The `./mlflow` directory is for mlflow related files and configuration
- The `./scripts` directory is for adhoc scripts to be ran on the data

## How to use

- Run the Docker compose file

  ```bash
  docker compose up
  ```

- Go to <localhost:8080> for the airflow GUI

- Go to \<...> for the mlflow GUI

## Development

### View MySQL database

- Get the container ID for the MySQL instance

  ```bash
  docker ps
  # look for the MySQL instance and copy it
  ```

- Enter the MySQL container using the following command

  ```bash
  docker exec -it "MySQL container ID here" bash
  ```

- Find the MySQL executable in the MySQL container

  ```bash
  cd /var/lib/mysql
  ```

- Login to the MySQL instance with the root user

  ```bash
  mysql -u root -p
  # enter password as "password"
  ```

### Airflow DAGs

- Create a Python virtual environment

```bash
python3 -m venv .venv
```

- Activate the Python virtual environment

```bash
source ./.venv/bin/activate
```

- Install the development dependencies

```bash
pip3 install -r ./requirements.txt
```

- Write DAGs in the `./dags` folder

> \[!NOTE\]
> When the airflow docker compose file is ran, ownership of mounted volumes may be incorrect. Run `chown` command to allow for hot-reload of DAGs.
>
> ```bash
> sudo chown -R <user-here> ./dags
> ```
