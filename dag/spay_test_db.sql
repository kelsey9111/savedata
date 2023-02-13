

-- status INT , //1: enable, 2: disable
-- job_status INT, // 1: ready, 3:runing, 3:failed
-- select * from bank_account;
-- update bank_account set job_status = 1, latest_ref_code = 'ss';
-- delete from bank_account_statement;


CREATE TABLE bank(
   id SERIAL PRIMARY KEY,
   code VARCHAR(200),
   name VARCHAR(200),
   currency        VARCHAR(200),
   created_at TIMESTAMP  NOT NULL DEFAULT (now() at time zone 'utc'),
   created_by VARCHAR(100),
   updated_at TIMESTAMP,
   updated_by VARCHAR(100)
);

DELETE from bank;
INSERT INTO bank
	(id, code, name, currency) VALUES
	(1, 'VCB', 'Vietcombank', 'VND');
	ALTER SEQUENCE bank_id_seq RESTART WITH 2;



CREATE TABLE bank_account(
   id SERIAL PRIMARY KEY,
   bank_id     INT,
   member_account_id     INT,
   account_no VARCHAR(200),
   code VARCHAR(200),
   status            INT DEFAULT '1', 
   job_status INT DEFAULT '1', 
   username        VARCHAR(200),
   password VARCHAR(200),
   error_message VARCHAR(200),
   salt        VARCHAR(50),
   created_at TIMESTAMP  NOT NULL DEFAULT (now() at time zone 'utc'),
   created_by VARCHAR(100),
   updated_at TIMESTAMP,
   updated_by VARCHAR(100)
);

DELETE FROM bank_account;
INSERT INTO bank_account 
			(id, bank_id, member_account_id, code, account_no, username, password, salt, error_message) VALUES
			(1, 1, 1, 'VCB35598','1024093559', '0886055093', 'YlJJbk1KWnMyWnFxTDc4bX/lc9AauVyT', 'bRInMJZs2ZqqL78m', '');
			ALTER SEQUENCE bank_account_id_seq RESTART WITH 2;


CREATE TABLE bank_account_statement(
   id SERIAL PRIMARY KEY,
   bank_account_id INT,
   ref_code_hash 	VARCHAR(100),
   bank_description_hash VARCHAR(100),
   transaction_date  TIMESTAMP,
   ref_code VARCHAR(100),
   bank_description 	VARCHAR(100),
   amount  DECIMAL,
   created_at TIMESTAMP  NOT NULL DEFAULT (now() at time zone 'utc'),
   created_by VARCHAR(100),
   updated_at TIMESTAMP,
   updated_by VARCHAR(100)
);

DELETE FROM bank_account_statement;
INSERT INTO bank_account_statement
	(id, bank_account_id, ref_code_hash, bank_description_hash, transaction_date, ref_code, bank_description,amount,created_by,updated_by,created_at,updated_at) VALUES
	(1, 1, 'ref_code_hash_test', 'bank_description_hash_test', '2023-02-03 09:48:44', 'ref_code_test','bank_description_test',1000,'testuser','testuser', '2023-02-03 09:48:44','2023-02-03 09:48:44');

