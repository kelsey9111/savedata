

-- status INT , //1: enable, 2: disable
-- job_status INT, // 1: ready, 3:runing, 3:failed
-- select * from bank_account;
-- update bank_account set status = 1, latest_ref_code = 'ss';
-- delete from bank_account_statement;


DROP TABLE bank_account;
CREATE TABLE bank_account(
   id SERIAL PRIMARY KEY,
   bank_code           VARCHAR(200),
   status            INT DEFAULT '1', 
   username        VARCHAR(200),
   password VARCHAR(200),
   job_status INT DEFAULT '1', 
   latest_ref_code VARCHAR(100),
   create_at TIMESTAMP  NOT NULL DEFAULT (now() at time zone 'utc'),
   create_by VARCHAR(100),
   update_at TIMESTAMP,
   update_by VARCHAR(100)
);

INSERT INTO bank_account
        (bank_code, username, password) VALUES
        ('VCB','0886330802', 'Cbn10646!');


drop table bank_account_statement;

CREATE TABLE bank_account_statement(
   id SERIAL PRIMARY KEY,
   bank_account_id INT,
   transaction_date           TIMESTAMP,
   ref_code 			VARCHAR(100),
   description		VARCHAR(200),
   amount  DECIMAL,
   create_at TIMESTAMP NOT NULL DEFAULT (now() at time zone 'utc')
);

