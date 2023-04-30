# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 08:44:53 2023

@author: Akshay.Nimbalkar
"""

from flask import Flask, request, jsonify
from flask_mysqldb import MySQL
import boto3
import json
from datetime import datetime


session  = boto3.Session(aws_access_key_id='AKIARD4HLV3BEOSGK2WA', 
                         aws_secret_access_key = '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'
                         )
secrets_client = session.client('secretsmanager', region_name='eu-central-1')
secret_name = 'udemy-ecom-db'
response = secrets_client.get_secret_value(SecretId=secret_name)
secret = json.loads(response['SecretString'])

app = Flask(__name__)
app.config['MYSQL_HOST'] = secret['host']
app.config['MYSQL_USER'] = secret['username']
app.config['MYSQL_PASSWORD'] = secret['password']
app.config['MYSQL_DB'] = secret['dbInstanceIdentifier']

mysql = MySQL(app)


@app.route('/create_order', methods=['POST', 'GET'])
def create_order():
    if request.method == 'POST':
        data = request.json

        # Extract the data fields from the JSON
        user_id = data.get('user_id')
        created_at = datetime.now()
        num_items = data.get('num_items')
        status = "Processing"
        products = request.json['products']

        # Validate the data
        if not user_id or not num_items:
            return jsonify({'message': 'Invalid request'}), 400

        cur = mysql.connection.cursor()

        cur.execute("SELECT MAX(order_id) FROM orders")
        result = cur.fetchone()
        order_id = result[0]+1 if result[0] else 1
        
        # Insert the new order into the orders table
            
        cur.execute("INSERT INTO orders (order_id, user_id, status, num_items, created_at) VALUES (%s, %s, %s, %s, %s)", (order_id, user_id, status, num_items, created_at))
        mysql.connection.commit()
        
        
        for product in products:
            cur.execute("SELECT MAX(id) FROM order_items")
            
            result1 = cur.fetchone()
            item_id = result1[0]+1 if result1[0] else 1
            
            
            product_id = product['product_id']
            sales_price = product['sales_price']
            
            #insert order into oredr item table
            cur.execute("INSERT INTO order_items (id, order_id, product_id, sales_price) VALUES (%s, %s, %s, %s)", (item_id, order_id, product_id, sales_price ))
            mysql.connection.commit()
            
        cur.close()

        return jsonify({'message': 'Order created successfully'})

    else:
        # If the request is not a POST request, return an error message
        return jsonify({'message': 'Invalid request'}), 400




@app.route('/update_order_status', methods=['POST', 'GET'])
def update_order_status():
    if request.method == 'POST':
        
        data = request.json
        order_id = data.get('order_id')
        status = data.get('status')
        
        if not order_id or not status:
            return jsonify({'message': 'Invalid request'}), 400
        
        cur = mysql.connection.cursor()
        cur.execute("SELECT * from orders where order_id = %s", (order_id,))
        order = cur.fetchone()
        
        if not order:
            return jsonify({'message': 'Order not found'}), 404
        
        if status == "Processing":
            cur.execute("UPDATE orders SET status=%s WHERE order_id=%s", (status, order_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Order status updated to Processing'})
        
        elif status == "Returned":
            #check if the request includes a returned_at value
            returned_at = data.get('returned_at')
            if not returned_at:
                return jsonify({'message': 'Invalid request. Missing returned_at value'}), 400
            # Update the order status to Returned and set the returned_at value
            cur.execute("UPDATE orders SET status=%s, returned_at=%s WHERE order_id=%s", (status, returned_at, order_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Order status updated to Returned'})

        elif status == 'Shipped':
            # Check if the request includes a shipped_at value
            shipped_at = data.get('shipped_at')
            if not shipped_at:
                return jsonify({'message': 'Invalid request. Missing shipped_at value'}), 400
            # Update the order status to Shipped and set the shipped_at value
            cur.execute("UPDATE orders SET status=%s, shipped_at=%s WHERE order_id=%s", (status, shipped_at, order_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Order status updated to Shipped'})

        elif status == 'Complete':
            # Check if the request includes a delivered_at value
            delivered_at = data.get('delivered_at')
            if not delivered_at:
                return jsonify({'message': 'Invalid request. Missing delivered_at value'}), 400
            # Update the order status to Complete and set the delivered_at value
            cur.execute("UPDATE orders SET status=%s, delivered_at=%s WHERE order_id=%s", (status, delivered_at, order_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Order status updated to Complete'})

        elif status == 'Cancelled':
            # Update the order status to Cancelled and clear any associated timestamps
            cur.execute("UPDATE orders SET status=%s, returned_at=NULL, shipped_at=NULL, delivered_at=NULL WHERE order_id=%s", (status, order_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Order status updated to Cancelled'})        


        return jsonify({'message': 'Please Enter Valid Order Status (Processing, Returned, Shipped, Complete, Cancelled) '})
    
    else:
        return jsonify({'message': "Invalid Method"})


def execute_sql(sql):
    cur = mysql.connection.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    return data

def get_order_id(table, order_id):
    sql = "SELECT * FROM "+table+" where order_id = {}".format(order_id)
    return sql


# READ operation
@app.route('/get_order_details',methods=['POST', 'GET'])
def get_order_details():  
    if request.method == 'POST':
        try:
            order_id = int(request.json['order_id'])
        except ValueError:
            return jsonify({'message': 'Enter valid Integer order ID'}), 400        
        sql = get_order_id("orders", order_id)
        data = execute_sql(sql)        
        if data:
            orders_list = []
            for d in data:
                order_dict = {'order_id':order_id, 'user_id': d[1], 'status' : d[2],'created_at': d[3], 
                                      'returned_at': d[4], 'shipped_at' : d[5], 'delivered_at' : d[6], 'num_items': d[7]}
                orders_list.append(order_dict)
            return jsonify(orders_list)           

        else:
            return jsonify({'message': 'Order not found'})
    else:
        return jsonify({'message': "Invalid Method"})



@app.route('/get_order_items',methods=['POST', 'GET'])
def get_order_items():

    if request.method == 'POST':
        try:
            order_id = int(request.json['order_id'])
        except ValueError:
            return jsonify({'message': 'Enter valid Integer order ID'}), 400 
        sql = get_order_id("order_items", order_id)
        data = execute_sql(sql)
        if data:
            items_list = []
            for d in data:
                order_dict = {'id':d[0], 'order_id':d[1],
                          'product_id': d[2], 'sales_price': float(d[3]), 'created_at': d[4]}
                items_list.append(order_dict)
            return jsonify(items_list)
        else:
            return jsonify({'message': 'Order not found'})
    else:
        return jsonify({'message': "Invalid Method"})



if __name__ == '__main__':
    #app.run()
    app.run(host='0.0.0.0', port=8080)