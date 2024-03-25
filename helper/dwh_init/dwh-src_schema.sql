-- Public
CREATE TABLE category (
    category_id serial primary key NOT NULL,
    "name" varchar(255) NOT NULL,
    description text NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE customer (
    customer_id serial primary key NOT NULL,
    first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    email varchar(255) NOT NULL,
    phone varchar(100) NULL,
    address text NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE orders (
    order_id varchar(50) primary key NOT NULL,
    customer_id int4 NULL,
    order_date date NOT NULL,
    status varchar(50) NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE order_detail (
    order_detail_id serial primary key NOT NULL,
    order_id varchar(50) NULL,
    product_id varchar(100) NULL,
    quantity int4 NOT NULL,
    price numeric(10, 2) NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE subcategory (
	subcategory_id serial primary key NOT NULL,
	"name" varchar(255) NOT NULL,
	category_id int4 NULL,
	description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE product (
    product_id varchar(100) primary key NOT NULL,
    "name" text NOT NULL,
    subcategory_id int4 NULL,
    price numeric(10, 2) NOT NULL,
    stock int4 NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

ALTER TABLE orders
ADD FOREIGN KEY (customer_id) REFERENCES customer(customer_id);

ALTER TABLE order_detail
ADD FOREIGN KEY (order_id) REFERENCES orders(order_id);

ALTER TABLE order_detail
ADD FOREIGN KEY (product_id) REFERENCES product(product_id);

ALTER TABLE subcategory
ADD FOREIGN KEY (category_id) REFERENCES category(category_id);

ALTER TABLE product
ADD FOREIGN KEY (subcategory_id) REFERENCES subcategory(subcategory_id);