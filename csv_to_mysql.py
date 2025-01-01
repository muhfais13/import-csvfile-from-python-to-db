import mysql.connector


def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host='localhost', 
            port=3306, 
            user='root', 
            password='root12345',
            database='daily_sales_de')
        
        print("Connection Success")
        return connection

    except mysql.connector.Error as error:
        print("Connection Failed".format(error))


def create_table(conn):
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS daily_sales (
        id INT AUTO_INCREMENT PRIMARY KEY,
        transaction_id VARCHAR(255),
        product_id INT,
        quantity INT,
        price DECIMAL(10, 2) NULL,
        transaction_date DATE,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
    
    try:
        cursor.execute(create_table_query)
        print("Table created successfully!")
    except mysql.connector.Error as err:
        print(f"Failed to create table: {err}")
    finally:
        cursor.close()


def insert_data(conn, data):
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO daily_sales (transaction_id, product_id, quantity, price, transaction_date)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"{cursor.rowcount} records inserted successfully.")

    except mysql.connector.Error as err:
        conn.rollback()
        print(f"Error: {err}")

    finally:
        cursor.close()


def main():
    file_path = '/Users/muhfais_/Documents/PT. All Data International/Tecnical Test/daily_sales.csv'
    
    data = []
    invalid_rows = []
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for index, line in enumerate(lines[1:], start=2):
            try:
                transaction_id, product_id, quantity, price, transaction_date = line.strip().split(',')
                quantity = int(quantity)
                price = float(price) if price.strip() else None
                data.append((transaction_id, product_id, quantity, price, transaction_date))
            
            except ValueError as e:
                invalid_rows.append((index, line.strip(), str(e)))
    
    if invalid_rows:
        print("The following rows are invalid and were skipped:")
        for row in invalid_rows:
            print(f"Line {row[0]}: {row[1]} (Error: {row[2]})")
    
    conn = connect_to_db()
    if conn:
        create_table(conn)
        insert_data(conn, data)
        conn.close()

if __name__ == '__main__':
    main()