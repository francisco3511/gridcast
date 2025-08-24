from gridcast.database_handler import update
from gridcast.preprocess import save_hourly_data


def main():
    update()
    save_hourly_data()
    return


if __name__ == "__main__":
    main()
