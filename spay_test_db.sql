# spay db

DROP TABLE bank_account;

CREATE TABLE bank_account(

id SERIAL PRIMARY KEY,

bank_code VARCHAR(200),

status INT ,

username VARCHAR(200),

password VARCHAR(200),

latest_ref_code VARCHAR(100),

create_at TIMESTAMP DEFAULT (now() at time zone 'utc'),

create_by VARCHAR(100),

update_at TIMESTAMP,

update_by VARCHAR(100)

);

INSERT INTO bank_account

(id, bank_code, status, username, password) VALUES

(1, 'VCB', 1, '0886330802', 'Cbn10646!');

drop table bank_account_statement;

CREATE TABLE bank_account_statement(

id SERIAL PRIMARY KEY,

bank_bank_account_id INT,

transaction_date TIMESTAMP,

ref_code VARCHAR(100),

description VARCHAR(200),

amount DECIMAL,

create_at TIMESTAMP DEFAULT (now() at time zone 'utc')

);