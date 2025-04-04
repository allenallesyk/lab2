import boto3
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP  # Імпортуємо Decimal та округлення

# Завантаження CSV-файлу
df = pd.read_csv("exchange.csv")

# Функція для конвертації float у Decimal з округленням
def safe_decimal(value):
    return Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

# Перетворюємо всі float значення в Decimal
df["rate"] = df["rate"].apply(safe_decimal)

# Підключення до DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('ExchangeRates')

# Додавання записів у таблицю
for _, row in df.iterrows():
    table.put_item(
        Item={
            'cc': row['cc'],
            'r030': int(row['r030']),
            'txt': row['txt'],
            'rate': row['rate'],  # Округлений Decimal
            'exchangedate': row['exchangedate']
        }
    )

print("Дані успішно завантажені в DynamoDB!")
