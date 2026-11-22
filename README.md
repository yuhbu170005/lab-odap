# lab-odap

## Cau 1 - Kafka producer

Producer mo phong giao dich POS bang cach doc tung dong trong file CSV va gui JSON
len Kafka topic `credit-card-transactions` voi khoang delay ngau nhien tu 1 den 5 giay.

### Chuan bi

```powershell
python -m pip install -r requirements-kafka.txt
```

Kafka can dang chay tren `localhost:9092`. Tao topic neu cluster khong tu dong tao topic:

```powershell
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic credit-card-transactions --partitions 1 --replication-factor 1
```

### Chay producer

```powershell
python kafka\producer.py
```

### Lay ty gia USD sang VND

Producer co the lay ty gia Vietcombank va them hai thong tin vao moi giao dich:

- `Exchange_Rate_USD_VND`: ty gia Transfer.
- `Amount_VND`: so tien da quy doi sang VND.
- `Exchange_Rate_Source`: `API` hoac `WEB_SCRAPING`.
- `Exchange_Rate_Date`: thoi gian ty gia duoc cong bo.

Ung dung uu tien XML API Vietcombank. Khi API loi, ung dung tu dong fallback
toi trang ty gia bang BeautifulSoup.

Co the tuy chinh:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_TOPIC="credit-card-transactions"
$env:CSV_PATH="User0_credit_card_transactions.csv"
$env:MIN_DELAY_SECONDS="1"
$env:MAX_DELAY_SECONDS="5"
$env:LOOP="false"
python kafka\producer.py
```