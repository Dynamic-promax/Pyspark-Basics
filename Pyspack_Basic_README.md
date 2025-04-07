
# PySpark Basics

This project demonstrates basic PySpark operations including reading CSV files, filtering, joining, renaming columns, SQL queries, and working with RDDs. It serves as a starting point for understanding how to manipulate data using PySpark in a local development environment.

## Features

- Create SparkSession
- Load CSV data
- Show and explore data schema
- Select and filter specific columns
- Rename columns
- Optimize queries with broadcasting
- Join datasets
- Save data as Parquet
- Execute SQL queries
- Create and convert RDDs to/from DataFrames

## Technologies Used

- Python 3.x
- Apache Spark (PySpark)
- Parquet format for optimized storage

## Getting Started

### Prerequisites

- Java JDK 8 or 11
- Apache Spark installed
- Python 3.x
- PySpark library (`pip install pyspark`)

### How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/pyspark-basics.git
   cd pyspark-basics
   ```

2. Download the sample dataset:
   [Download taxi_zone_lookup.csv](https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv)  
   *(Replace with your actual data link if available)*

3. Update the file path in the script if needed.

4. Run the script:
   ```bash
   python "Pyspark Basics.py"
   ```

## Sample Output

- First five rows of the dataset
- Data schema
- Filtered rows for "Queens"
- Joined and broadcasted data
- SQL query results
- RDD to DataFrame conversions

## Output Location

The processed data is saved as a Parquet file at:
```
C:/Users/DELL/Desktop/Data Engineering NEW/NEW Python Codes/output
```
*(You can modify the path in the script to suit your system)*

## Author

**John Benedict Bello**  
[LinkedIn Profile](https://www.linkedin.com/in/bello-john-493b15155)

## License

This project is open-source and available under the [MIT License](LICENSE).
