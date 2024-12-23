import random
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq


def generate_user_id():
    return f"u{random.randint(1, 1000000):06d}"


def generate_timestamp():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    return start_date + timedelta(
        seconds=random.randint(0, int((end_date - start_date).total_seconds()))
    )


def generate_action_type():
    return random.choice(["page_view", "edit", "create", "delete", "share"])


def generate_page_id():
    return f"p{random.randint(1, 1000000):06d}"


def generate_duration_ms():
    return random.randint(100, 300000)


def generate_app_version():
    major = random.randint(5, 7)
    minor = random.randint(0, 9)
    patch = random.randint(0, 9)
    return f"{major}.{minor}.{patch}"


def generate_join_date():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))


def generate_country():
    return random.choice(["US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX"])


def generate_device_type():
    return random.choice(
        ["iPhone", "iPad", "Android Phone", "Android Tablet", "Windows", "Mac"]
    )


def generate_subscription_type():
    return random.choice(["free", "basic", "premium", "enterprise"])


def generate_user_interactions(num_records, filename, chunk_size=100000):
    writer = None
    for start in range(0, num_records, chunk_size):
        end = min(start + chunk_size, num_records)
        chunk_records = end - start

        user_id_list = []
        timestamp_list = []
        action_type_list = []
        page_id_list = []
        duration_ms_list = []
        app_version_list = []

        for _ in range(chunk_records):
            user_id_list.append(generate_user_id())
            timestamp_list.append(generate_timestamp())
            action_type_list.append(generate_action_type())
            page_id_list.append(generate_page_id())
            duration_ms_list.append(generate_duration_ms())
            app_version_list.append(generate_app_version())

        data = {
            "user_id": user_id_list,
            "timestamp": timestamp_list,
            "action_type": action_type_list,
            "page_id": page_id_list,
            "duration_ms": duration_ms_list,
            "app_version": app_version_list,
        }

        table = pa.Table.from_pydict(data)

        if writer is None:
            writer = pq.ParquetWriter(filename, table.schema)
        writer.write_table(table)

    if writer:
        writer.close()


def generate_user_metadata(num_records, filename, chunk_size=100000):
    writer = None
    for start in range(0, num_records, chunk_size):
        end = min(start + chunk_size, num_records)
        chunk_records = end - start

        user_id_list = []
        join_date_list = []
        country_list = []
        device_type_list = []
        subscription_type_list = []

        for _ in range(chunk_records):
            user_id_list.append(generate_user_id())
            join_date_list.append(generate_join_date())
            country_list.append(generate_country())
            device_type_list.append(generate_device_type())
            subscription_type_list.append(generate_subscription_type())

        data = {
            "user_id": user_id_list,
            "join_date": join_date_list,
            "country": country_list,
            "device_type": device_type_list,
            "subscription_type": subscription_type_list,
        }

        table = pa.Table.from_pydict(data)

        if writer is None:
            writer = pq.ParquetWriter(filename, table.schema)
        writer.write_table(table)

    if writer:
        writer.close()


if __name__ == "__main__":
    generate_user_interactions(1000000, "data/user_interactions_sample.parquet")
    generate_user_metadata(1000000, "data/user_metadata_sample.parquet")

    print("Sample datasets generated successfully.")
