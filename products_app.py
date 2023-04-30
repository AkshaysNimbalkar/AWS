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


@app.route('/insert_product', methods=['POST', 'GET'])
def insert_product():
    if request.method == 'POST':
        data = request.json

        # Extract the data fields from the JSON
        try:
            cost = float(data.get('cost'))
            retail_price = float(data.get('retail_price'))
        except ValueError:
            return jsonify({'message': 'Enter only numbers'}), 400        

        category = data.get('category')
        name = data.get('name')
        brand = data.get('brand')
        department = data.get('department')
        
        if not cost or not retail_price or not category or not name or not brand or not department:
            return jsonify({'message': 'Enter All the attributes (cost, retail_price, category, name, brand, department)'}), 400
        
        cur = mysql.connection.cursor()
        cur.execute("SELECT MAX(id) FROM products")
        result = cur.fetchone()
        product_id = result[0]+1 if result[0] else 1
        
        # Insert the new product into the Products table
            
        cur.execute("INSERT INTO products (id, cost, category, name, brand, department, retail_price) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                    (product_id, cost, category, name, brand, department, retail_price))
        mysql.connection.commit()
            
        cur.close()

        return jsonify({'message': 'Product added successfully'})

    else:
        return jsonify({'message': 'Invalid request'}), 400
 
    
    
def execute_sql(sql):
    cur = mysql.connection.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    return data


def product_return(data):
    if data:
        products_list = []
        for d in data:
            product_dict = {'id':d[0], 'cost': float(d[1]), 'category' : d[2],'name': d[3], 
                          'brand': d[4], 'department' : d[5], 'retail_price' : float(d[6])}
            products_list.append(product_dict)
        return products_list
    else:
        return jsonify({'message': 'Details Not found'})    

def get_product_id(product_id):
    sql = "SELECT * FROM products where id = {}".format(product_id)
    return sql


@app.route('/update_product', methods=['POST', 'GET'])
def update_product():
    if request.method == 'POST':
        
        data = request.json
        product_id = data.get('id')
        
        if not product_id:
            return jsonify({'message': 'Invalid request'}), 400
        
        cur = mysql.connection.cursor()
        cur.execute("SELECT * from products where id = %s", (product_id,))
        product = cur.fetchone()

        
        if not product:
            return jsonify({'message': 'Product not found'}), 404
        
        if 'cost' in request.json :
            cost = request.json['cost']
            cur.execute("UPDATE products SET cost=%s WHERE id=%s",(cost, product_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Product Cost updated'})
            
        elif 'retail_price' in request.json :
            retail_price = request.json['retail_price'] 
            cur.execute("UPDATE products SET retail_price=%s WHERE id=%s",(retail_price, product_id))
            mysql.connection.commit()
            cur.close()
            return jsonify({'message': 'Product Retail Price updated'})
        else:
            return jsonify({'message': 'Enter cost or retail_price to update'})
        
    else:
        return jsonify({'message': "Invalid Method"})




@app.route('/get_products',methods=['POST', 'GET'])
def get_products():  
    if request.method == 'POST':
       sql = """select * from products where 1=1 """
       if 'category' in request.json :
           category = request.json['category']
           sql += " and category='{0}'".format(category)
           data = execute_sql(sql)
           products_list = product_return(data)
           return jsonify(products_list)
       
       elif 'department' in request.json :
           department = request.json['department']
           sql += " and department='{0}'".format(department)
           data = execute_sql(sql)
           products_list = product_return(data)
           return jsonify(products_list)
       
       elif 'limit' in request.json :
           limit = request.json['limit']
           sql += " limit'{0}'".format(limit)
           data = execute_sql(sql)
           products_list = product_return(data)
           return jsonify(products_list)
       else:
          return jsonify({'message': "Enter only Category or Department or Limit"})
       
    else:
        return jsonify({'message': "Invalid Method"})



# READ operation
@app.route('/get_product_details',methods=['POST', 'GET'])
def get_product_details():  
    if request.method == 'POST':
        try:
            product_id = int(request.json['id'])
        except ValueError:
            return jsonify({'message': 'Enter valid Integer order ID'}), 400
        sql = get_product_id(product_id)
        data = execute_sql(sql)
        product = product_return(data)
        return jsonify(product)
    
    else:
        return jsonify({'message': "Invalid Method"})



if __name__ == '__main__':
    #app.run()
    app.run(host='0.0.0.0', port=8081)