import psycopg2
import os

# --- DATABASE CONNECTION ---
# It's best practice to use environment variables for connection details.
# Replace with your actual database credentials.
DB_NAME = "00"
DB_USER = "your_db_user"
DB_PASS = "NAVEEN2302"
DB_HOST = "localhost"
DB_PORT = "5432"

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

# --- CRUD OPERATIONS FOR Customers ---

# CREATE
def add_customer(name, email, age, city):
    """Adds a new customer to the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO Customers (name, email, age, city) VALUES (%s, %s, %s, %s)",
            (name, email, age, city)
        )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback() # Rollback on error
        raise e
    finally:
        cur.close()
        conn.close()


# READ
def view_all_customers():
    """Retrieves all customers from the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT customer_id, name, email, age, city FROM Customers ORDER BY customer_id ASC")
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return customers

# UPDATE
def update_customer(customer_id, name, email, age, city):
    """Updates an existing customer's details."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE Customers SET name=%s, email=%s, age=%s, city=%s WHERE customer_id=%s",
            (name, email, age, city, customer_id)
        )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# DELETE
def delete_customer(customer_id):
    """Deletes a customer from the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM Customers WHERE customer_id=%s", (customer_id,))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# Helper function to get a single customer by ID
def get_customer_by_id(customer_id):
    """Retrieves a single customer by their ID."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, email, age, city FROM Customers WHERE customer_id = %s", (customer_id,))
    customer = cur.fetchone()
    cur.close()
    conn.close()
    return customer


# --- BUSINESS INSIGHTS ---

def get_total_customers():
    """Returns the total number of customers."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Customers")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

def get_total_revenue():
    """Returns the sum of all revenue generated."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT SUM(revenue_generated) FROM Interactions WHERE conversion_status = TRUE")
    total_revenue = cur.fetchone()[0]
    cur.close()
    conn.close()
    return total_revenue or 0 # Return 0 if None

def get_average_customer_age():
    """Returns the average age of all customers."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT AVG(age) FROM Customers")
    avg_age = cur.fetchone()[0]
    cur.close()
    conn.close()
    return avg_age

def get_campaign_with_max_budget():
    """Returns the campaign with the highest budget."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT campaign_name, budget FROM Campaigns ORDER BY budget DESC LIMIT 1")
    campaign = cur.fetchone()
    cur.close()
    conn.close()
    return campaign

def get_most_profitable_campaign():
    """Returns the campaign that generated the most revenue."""
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
    SELECT c.campaign_name, SUM(i.revenue_generated) as total_revenue
    FROM Campaigns c
    JOIN Interactions i ON c.campaign_id = i.campaign_id
    WHERE i.conversion_status = TRUE
    GROUP BY c.campaign_name
    ORDER BY total_revenue DESC
    LIMIT 1;
    """
    cur.execute(query)
    campaign = cur.fetchone()
    cur.close()
    conn.close()
    return campaign