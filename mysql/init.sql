CREATE TABLE IF NOT EXISTS employees (
    id          INT            NOT NULL AUTO_INCREMENT,
    first_name  VARCHAR(50)    NOT NULL,
    last_name   VARCHAR(50)    NOT NULL,
    email       VARCHAR(100)   NOT NULL UNIQUE,
    department  VARCHAR(50)    NOT NULL,
    job_title   VARCHAR(80)    NOT NULL,
    salary      DECIMAL(10,2)  NOT NULL,
    hire_date   DATE           NOT NULL,
    is_active   TINYINT(1)     NOT NULL DEFAULT 1,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO employees (first_name, last_name, email, department, job_title, salary, hire_date, is_active) VALUES
('Alice',   'Johnson',   'alice.johnson@company.com',    'Engineering', 'Senior Software Engineer',   95000.00, '2019-03-15', 1),
('Bob',     'Martinez',  'bob.martinez@company.com',     'Engineering', 'DevOps Engineer',            88000.00, '2020-06-01', 1),
('Carol',   'Thompson',  'carol.thompson@company.com',   'Marketing',   'Marketing Manager',          82000.00, '2018-11-20', 1),
('David',   'Lee',       'david.lee@company.com',        'Engineering', 'Backend Developer',          79000.00, '2021-01-10', 1),
('Emma',    'Wilson',    'emma.wilson@company.com',      'HR',          'HR Business Partner',        71000.00, '2017-07-22', 1),
('Frank',   'Anderson',  'frank.anderson@company.com',   'Finance',     'Financial Analyst',          74000.00, '2019-09-05', 1),
('Grace',   'Taylor',    'grace.taylor@company.com',     'Engineering', 'Data Engineer',              91000.00, '2020-02-14', 1),
('Henry',   'Brown',     'henry.brown@company.com',      'Sales',       'Account Executive',          67000.00, '2022-03-01', 1),
('Iris',    'Davis',     'iris.davis@company.com',       'Engineering', 'Frontend Developer',         84000.00, '2021-08-19', 1),
('James',   'Garcia',    'james.garcia@company.com',     'Product',     'Product Manager',           105000.00, '2016-05-30', 1),
('Karen',   'Miller',    'karen.miller@company.com',     'Marketing',   'Content Strategist',         62000.00, '2023-01-15', 1),
('Liam',    'Rodriguez', 'liam.rodriguez@company.com',   'Engineering', 'Machine Learning Engineer', 112000.00, '2020-10-07', 1),
('Mia',     'Clark',     'mia.clark@company.com',        'Finance',     'Senior Accountant',          78000.00, '2018-04-11', 1),
('Noah',    'Lewis',     'noah.lewis@company.com',       'Sales',       'Sales Manager',              93000.00, '2017-12-03', 1),
('Olivia',  'Walker',    'olivia.walker@company.com',    'HR',          'Talent Acquisition Lead',    69000.00, '2022-07-25', 1),
('Paul',    'Hall',      'paul.hall@company.com',        'Engineering', 'Platform Engineer',          98000.00, '2019-06-18', 1),
('Quinn',   'Allen',     'quinn.allen@company.com',      'Product',     'UX Researcher',              73000.00, '2021-11-09', 1),
('Rachel',  'Young',     'rachel.young@company.com',     'Finance',     'CFO',                       175000.00, '2015-02-01', 1),
('Samuel',  'Hernandez', 'samuel.hernandez@company.com', 'Engineering', 'QA Engineer',                76000.00, '2020-04-22', 1),
('Tina',    'King',      'tina.king@company.com',        'Marketing',   'Digital Marketing Analyst',  65000.00, '2023-05-08', 0);

CREATE TABLE IF NOT EXISTS products (
    id          INT            NOT NULL AUTO_INCREMENT,
    sku         VARCHAR(40)    NOT NULL UNIQUE,
    name        VARCHAR(120)   NOT NULL,
    category    VARCHAR(50)    NOT NULL,
    price       DECIMAL(10,2)  NOT NULL,
    in_stock    TINYINT(1)     NOT NULL DEFAULT 1,
    created_at  DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO products (sku, name, category, price, in_stock) VALUES
('SKU-1001', 'Wireless Mouse',          'Electronics',   24.99, 1),
('SKU-1002', 'Mechanical Keyboard',     'Electronics',   89.50, 1),
('SKU-1003', '27" 4K Monitor',          'Electronics',  349.00, 1),
('SKU-1004', 'USB-C Hub (7-in-1)',      'Electronics',   42.75, 1),
('SKU-2001', 'Standing Desk',           'Furniture',    420.00, 1),
('SKU-2002', 'Ergonomic Chair',         'Furniture',    315.00, 1),
('SKU-3001', 'Notebook A5 (pack of 3)', 'Stationery',    14.20, 1),
('SKU-3002', 'Gel Pens (12-pack)',      'Stationery',     8.99, 1),
('SKU-4001', 'Cold Brew Maker',         'Kitchen',       38.00, 1),
('SKU-4002', 'Espresso Machine',        'Kitchen',      610.00, 0);

CREATE TABLE IF NOT EXISTS orders (
    id           INT            NOT NULL AUTO_INCREMENT,
    employee_id  INT            NOT NULL,
    product_id   INT            NOT NULL,
    quantity     INT            NOT NULL,
    total        DECIMAL(10,2)  NOT NULL,
    status       VARCHAR(20)    NOT NULL,
    created_at   DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (employee_id) REFERENCES employees(id),
    FOREIGN KEY (product_id)  REFERENCES products(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO orders (employee_id, product_id, quantity, total, status, created_at) VALUES
( 1,  1, 2,    49.98, 'completed', '2026-01-12 09:14:00'),
( 2,  3, 1,   349.00, 'completed', '2026-01-15 14:02:00'),
( 4,  2, 1,    89.50, 'completed', '2026-02-03 11:45:00'),
( 7,  5, 1,   420.00, 'shipped',   '2026-02-18 16:20:00'),
(10,  6, 1,   315.00, 'completed', '2026-03-01 10:08:00'),
( 3,  7, 5,    71.00, 'completed', '2026-03-09 13:30:00'),
(12,  9, 1,    38.00, 'cancelled', '2026-03-22 08:55:00'),
( 5,  4, 2,    85.50, 'completed', '2026-04-04 15:10:00'),
(15,  8, 3,    26.97, 'pending',   '2026-04-21 12:00:00'),
( 9,  1, 1,    24.99, 'shipped',   '2026-05-02 09:40:00');
