# Big-Data-Management-Project




## Data Cleaning

During the transformation phase, several data quality checks were applied to remove clearly invalid or corrupted trip records while preserving potentially valid edge cases.

### Cleaning Rules

The following rules were applied:

1. **Missing timestamps**

   Rows with missing `tpep_pickup_datetime` or `tpep_dropoff_datetime` were removed.  
   These fields are essential for describing a taxi trip and are required for later calculations such as trip duration.

2. **Invalid timestamp order**

   Rows where `tpep_dropoff_datetime <= tpep_pickup_datetime` were removed, because a trip cannot end before or at the exact moment it starts.

3. **Trips longer than 24 hours**

   Rows where the trip duration exceeded **24 hours** were removed.  
   Such values are unrealistic for standard taxi trips and are likely caused by data entry errors.

4. **Non-positive trip distance**

   Rows where `trip_distance <= 0` were removed.  
   A completed taxi trip should always have a positive traveled distance.

5. **Negative fare amounts**

   Rows where `fare_amount < 0` were removed.  
   Negative fares are considered invalid entries. Trips with a fare of `0` were kept, since promotional or no-charge rides may occur.

6. **Negative total amounts**

   Rows where `total_amount < 0` were removed.  
   Similar to fare amounts, negative totals are considered invalid, while zero totals were allowed.

7. **Negative passenger counts**

   Rows where `passenger_count < 0` were removed.

   A stricter rule requiring `passenger_count >= 1` was initially tested, but it removed over **1 million rows**, indicating that this field may not be consistently reliable in the dataset. Therefore, only clearly invalid negative values were removed.

8. **Invalid location identifiers**

   Rows where the pickup or dropoff location identifiers were invalid were removed:

   - `PULocationID <= 0`
   - `DOLocationID <= 0`

   Taxi zone identifiers must be positive integers according to the dataset specification.

---

### Cleaning Summary

Across all cleaning rules, rows that violated one or more of the above conditions were removed to improve overall data quality while avoiding excessive data loss.




## Deduplication

To ensure that the dataset does not contain duplicate trip records, a deduplication step was applied during the transformation phase.

### Deduplication Key

Duplicates were identified using the following composite key:

- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `trip_distance`
- `fare_amount`

A row was considered a duplicate if all of these fields had identical values to another record.

### Rationale

These fields were selected because together they uniquely describe the essential characteristics of a taxi trip:

- **Pickup and dropoff timestamps** define when the trip occurred.
- **Pickup and dropoff location IDs** define where the trip started and ended.
- **Trip distance** represents the physical length of the trip.
- **Fare amount** represents the base price of the trip.

Combining these attributes creates a sufficiently specific identifier for a taxi trip while avoiding reliance on fields that may be less reliable or frequently repeated, such as `passenger_count`.

Using this composite key allows the pipeline to safely remove accidental duplicate rows without discarding legitimately distinct trips that happen to share similar characteristics.