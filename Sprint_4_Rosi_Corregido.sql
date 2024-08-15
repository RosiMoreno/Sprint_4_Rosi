
# Nivel 1
-- Creación de la nueva base de datos llamada "R2TRANS" --
# DROP DATABASE R2TRANS;
CREATE DATABASE R2TRANS;
show databases;

-- Creación de las tablas donde importar los archivos .csv --
-- TABLA DE TARJETAS DE CRÉDITO --
CREATE TABLE IF NOT EXISTS credit_cards (
	id varchar(15) NOT NULL,
    user_id VARCHAR(10),
    iban VARCHAR(50),
    pan VARCHAR (30),
    pin VARCHAR (4),
    cvv VARCHAR (3),
    track1 VARCHAR(255),
    track2 VARCHAR(255),
    expiring_date VARCHAR (15) 
);
-- TABLA DE COMPAÑÍAS --
CREATE TABLE IF NOT EXISTS company (
        company_id VARCHAR(15) NOT NULL,
        company_name VARCHAR(255),
        phone VARCHAR(15),
        email VARCHAR(100),
        country VARCHAR(100),
        website VARCHAR(255)
    );
  -- TABLA DE TRANSACCIONES --  
 CREATE TABLE IF NOT EXISTS transaction (
        id VARCHAR(255) NOT NULL,
        card_id VARCHAR(15),
        business_id VARCHAR(20),
        timestamp VARCHAR(50),
        amount VARCHAR(20),
        declined VARCHAR(2),
        product_ids VARCHAR(255),
        user_id VARCHAR(20),
        lat VARCHAR(30),
        longitude VARCHAR(30)
    );
-- TABLA DE USUARIOS --
CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(255) NOT NULL,
        name VARCHAR(20),
        surname VARCHAR(20),
        phone VARCHAR(20),
        email VARCHAR(50),
        birth_date VARCHAR(20),
        country VARCHAR(50),
        city VARCHAR(50),
        postal_code VARCHAR(10),
        address VARCHAR(255)
    );
show tables;
SELECT * FROM users;

-- Importar los archivos .csv en la base de datos --
LOAD DATA LOCAL INFILE '/Users/rosi/Desktop/SQL_ITAcademy/SQL/Sprint_4/users_ca.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Intentos para poder habilitar la opción de carga de archivos desde local --
show VARIABLES LIKE 'secure_file_priv';
SET GLOBAL local_infile = 1;
show VARIABLES LIKE 'local_infile';

-- Comandos para estudiar los campos de las tablas que tenemos --
SELECT * FROM users;
SELECT * FROM transaction;
SELECT * FROM credit_cards;
SELECT * FROM company;
SELECT count(id) FROM credit_cards;
show columns from credit_cards;

-- Creación de relaciones entre tablas --
# company 1:N transaction
-- creación de Clave primaria en tabla company --
ALTER TABLE company
ADD CONSTRAINT company_id  Primary key(company_id);
show columns from company;
-- creación de Clave foránea en tabla transaction --
ALTER TABLE transaction
ADD CONSTRAINT business_id  FOREIGN KEY (business_id)REFERENCES company(company_id);
show columns from transaction;
-- Creación de la PK en la tabla transaction --
ALTER TABLE transaction
ADD CONSTRAINT id Primary key(id);
show columns from transaction;

# users 1:N transaction
-- Creación PK en la tabla users --
ALTER TABLE users
ADD CONSTRAINT id  Primary key(id);
show columns from users;
-- creación de Clave foránea en tabla transaction --

ALTER TABLE transaction
ADD CONSTRAINT user_id  FOREIGN KEY (user_id) REFERENCES users(id);
show columns from transaction;

# credit_cards 1:N transaction
-- creación de Clave primaria en tabla credit_cards --
ALTER TABLE credit_cards
ADD CONSTRAINT id  Primary key(id);
show columns from credit_cards;
-- creación de Clave foránea en tabla transaction --
ALTER TABLE transaction
ADD CONSTRAINT card_id  FOREIGN KEY (card_id)REFERENCES credit_cards(id);
show columns from transaction;

# users 1:1 credit_cards
-- Creación de la restricción UNIQUE a la que será la FK de la tabla credit_cards --
ALTER TABLE credit_cards
ADD CONSTRAINT user_id UNIQUE(user_id);
describe credit_cards;
-- creación de Clave foránea en tabla credit_cards --
ALTER TABLE credit_cards
ADD CONSTRAINT fk_user_id  FOREIGN KEY (user_id)REFERENCES users(id);
show columns from credit_cards;

# Ejercicio 1
-- Muestra los usuarios que hayan realizado más de 30 transacciones --
-- con SUBCONSULTA --
SELECT users.name
FROM users
WHERE (SELECT COUNT(transaction.id)
	   FROM transaction
	   WHERE transaction.user_id = users.id) > 30
GROUP BY users.name;

-- con JOIN --
SELECT users.name, count(transaction.id) AS NumTransaccions
FROM users
JOIN transaction 
ON users.id = transaction.user_id
GROUP BY users.id
HAVING NumTransaccions >30
ORDER BY NumTransaccions DESC;

# Ejercicio 2
-- Cantidad media por IBAN de las tarjetas de la compañía Donec Ltd. --

SELECT company_name, iban, round(avg(amount),2) AS MediaCompraDonec
FROM credit_cards
JOIN transaction 
ON transaction.card_id = credit_cards.id
JOIN company
ON transaction.business_id = company.company_id
WHERE company_name = "Donec Ltd"
GROUP BY iban, company_name;

# Nivel 2
# Ejercicio 1 
-- Creación de una tabla con el estado de las tarjetas --
# ACTIVA si ha tenido 2 o menos transacciones denegadas en las últimas tres transacciones
# DESACTIVA si las tres últimas transacciones han sido denegadas.
-- Creación de una tabla --
CREATE TABLE EstadoTarjetas (
    card_id VARCHAR(10),
    Estado_Tarjeta VARCHAR(10)
);

-- Insertar los datos en la nueva tabla --
INSERT INTO EstadoTarjetas (card_id, Estado_Tarjeta)
SELECT card_id,
	CASE WHEN Sum(declined) = 3 THEN 'DESACTIVA'
		 ELSE 'ACTIVA' END AS Estado_Tarjeta
FROM (SELECT card_id, timestamp, declined
		FROM (SELECT card_id, timestamp, declined, 
			ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) as rn
		FROM transaction ) as subquery
		WHERE rn<= 3
		ORDER BY card_id, timestamp) AS ultimas3transacciones
GROUP BY card_id;
SELECT * FROM EstadoTarjetas;

-- Cuantas tarjetas están activas --
SELECT count(Estado_Tarjeta)
FROM EstadoTarjetas
WHERE Estado_Tarjeta = 'ACTIVA';

# Nivel 3
# Ejercicio 1
-- CReación de la tabla "products" desde el archivo products.csv --
-- TABLA DE PRODUCTOS --
CREATE TABLE IF NOT EXISTS products (
        id VARCHAR(10) NOT NULL,
        product_name VARCHAR(50),
        price VARCHAR(20),
        colour VARCHAR(20),
        weight VARCHAR(10),
        warehouse_id VARCHAR(10)
    );
show tables;
SELECT * FROM products;

# products 1:N transaction
-- creación de Clave primaria en tabla products --
ALTER TABLE products
ADD CONSTRAINT id  Primary key(id);
show columns from products;

-- DESCOMPONER VALORES DEL CAMPO product_ids DE LA TABLA TRANSACTION --
# 1. Creación de una tabla Temporal para almacenar los resultados descompuestos

CREATE TABLE desglose_transaction (
    id VARCHAR(36),
    product_id INT
);

-- Crea una tabla de números si no la tienes ya
CREATE TABLE numbers (
    n INT
);

-- Llena la tabla de números con algunos valores
INSERT INTO numbers (n) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
SELECT * FROM numbers;

-- Ahora usa la tabla de números para dividir los ids de productos --
INSERT INTO desglose_transaction (id, product_id)
SELECT 
    id, 
    CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', numbers.n + 1), ',', -1) AS UNSIGNED)
FROM 
    transaction
JOIN 
    numbers ON CHAR_LENGTH(product_ids) - CHAR_LENGTH(REPLACE(product_ids, ',', '')) >= numbers.n;

SELeCT id, card_id, product_ids FROM transaction
WHERE id = '02C6201E-D90A-1859-B4EE-88D2986D3B02';

SELECT card_id, product_id,id FROM desglose_transaction
WHERE id = '02C6201E-D90A-1859-B4EE-88D2986D3B02';

-- Cálculo de las veces que se ha vendido un prodcuto --

# Cambio el tipo de dato del campo ID de la tabla PRODUCTS para poder crear la clave foránea --
ALTER TABLE products CHANGE id id INT;
show columns from products;

# creación de Clave foránea en tabla desglose_transaction con tabla products --
ALTER TABLE desglose_transaction
ADD CONSTRAINT fk2_product_id  FOREIGN KEY (product_id)REFERENCES products(id);


# creación de Clave foránea en tabla desglose_transaction con tabla transaction --
ALTER TABLE desglose_transaction
ADD CONSTRAINT id  FOREIGN KEY (id)REFERENCES transaction(id);
show columns from desglose_transaction;

# ¿Cuántas veces se ha vendido cada prodcuto? --
SELECT product_name, count(product_id) AS VecesVendido
FROM products
JOIN desglose_transaction
ON products.id = desglose_transaction.product_id
GROUP BY product_name
ORDER BY VecesVendido DESC;