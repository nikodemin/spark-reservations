# Reservations app
## Data format
merchant_dataset.csv:
```
root
 |-- merchant_id: string (nullable = true)
 |-- merchant_name: string (nullable = true)
```
reservation_dataset.csv:
```
root
 |-- reservation_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- guest_count: long (nullable = true)
 |-- merchant_email: string (nullable = true)
 |-- starts_at: date (nullable = true)
 |-- ends_at: date (nullable = true)
 |-- created_at: date (nullable = true)
 |-- has_promo_code: boolean (nullable = true)
 |-- customer_id: string (nullable = true)
```
The input schema described in `Main.scala` object

## Tasks completed
1. Exclude all the reservations with badly formatted email addresses. Note that the email addresses have been anonymized on purpose.
  
2. Print the average number of seated guests
 
3. Display the name of the merchant with the highest amount of seated guests from the merchant_csv dataset. Reservations with only 1 seated guest shouldn’t be considered for this analysis.

4. Display the name of the merchant with the highest amount of reservations for each quarter of the year (January, February, March;  April, May, June ...).

## How to run
You can use one of the following variants:
* run in Intellij IDEA or Eclipse IDE using `Main.scala` class
* run `sbt run`
