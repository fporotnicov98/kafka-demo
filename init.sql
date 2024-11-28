CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  product_name VARCHAR(255),
  barcode VARCHAR(255),
  quantity INT,
  price DECIMAL
);
