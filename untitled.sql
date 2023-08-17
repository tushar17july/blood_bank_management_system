-----------------------------------------------------Database-----------------------------------------------------------------
-- First creating Blood Bank Database

CREATE DATABASE blood_bank_management_system ;

------------------------------------------------------Schema------------------------------------------------------------------
--Now we create Schema for Bloodbank

CREATE SCHEMA blood_bank_management_system ;


------------------------------------------------------Tables-----------------------------------------------------------------

-- creating a Donation Camp Table that will provide us with registered Donation Camp from where We can receive blood
--Primary Key is Camp_code which will be unique identification code for each bank

CREATE TABLE blood_bank_management_system.donor_camp_table (
	camp_name varchar(70) NULL,
	camp_address text NULL,
	camp_period varchar(20) NULL,
	camp_code varchar(20) NOT NULL,
	camp_location varchar(100) NULL,
	CONSTRAINT primary_key_camp PRIMARY KEY (camp_code)
);

/* Donor mst is table that include all the active donors that have ever donated blood to any of our camp
Primary key here is Donor mobile which is treated as unique*/


CREATE TABLE blood_bank_management_system.donor_mst (
	donor_name varchar(50) NULL,
	donor_age varchar(50) NULL,
	camp_code varchar(50) NULL,
	blood_id varchar(50) NULL,
	donor_mobile varchar(50) NOT NULL,
	CONSTRAINT donor_mst_pkey PRIMARY KEY (donor_mobile),
	CONSTRAINT donor_mst_fkey FOREIGN KEY (camp_code) REFERENCES blood_bank_management_system.donor_camp_table(camp_code)
);

/*Reciver hospital Table will be used to register all the blood bank affliated hospital to whom this blood will be transfered
Primary key will be Hospital code which is used for unique identification of Hospital
Foreign key is camp_code because each hospital will have a direct relation ship with camp from where donation will be recived*/

CREATE TABLE blood_bank_management_system.reciver_hospital_table (
	hospital_name varchar(70) NULL,
	hospital_address text NULL,
	hospital_period varchar(20) NULL,
	hospital_code varchar(20) NOT NULL,
	camp_code varchar(20) NULL,
	hospital_location varchar(100) NULL,
	CONSTRAINT primary_key_hospital PRIMARY KEY (hospital_code),
	CONSTRAINT foreign_key_hospital FOREIGN KEY (camp_code) REFERENCES blood_bank_management_system.donor_camp_table(camp_code)
);

/*This table will have record of all the donations that are going to happen*/
CREATE TABLE blood_bank_management_system.reciver_transaction_table (
	hospital_code varchar(70) NULL,
	reciver_name varchar(70) NULL,
	reciver_age int4 NULL,
	date_of_donation_released date NULL,
	blood_code varchar(100) NULL,
	receiver_count int4 NULL,
	gaurdian_mobile varchar NULL,
	CONSTRAINT reciver_transaction_table_fkey FOREIGN KEY (hospital_code) REFERENCES blood_bank_management_system.reciver_hospital_table(hospital_code)
);
/*This table will have record of all the donations that is provided to the bank*/

CREATE TABLE blood_bank_management_system.donation_transaction_table (
	camp_code varchar(70) NULL,
	donor_code varchar(70) NULL,
	date_of_donation date NULL,
	transaction_id varchar(500) NULL
);


--------------------------------------------------------------View----------------------------------------------------------------

/*This View is used to check the availibilty of Blood according to location and Blood_Group */

CREATE OR REPLACE VIEW blood_bank_management_system.view_blood_availibilty
AS SELECT tb.camp_code,
    tb.blood_id,
    sum(
        CASE
            WHEN tb.status = 'Total Donation'::text THEN tb.count
            ELSE '-1'::integer * tb.count
        END) AS sum
   FROM ( SELECT 'Total Donation'::text AS status,
            dtt.camp_code,
            dm.blood_id,
            count(1) AS count
           FROM blood_bank_management_system.donation_transaction_table dtt
             JOIN blood_bank_management_system.donor_mst dm ON dtt.donor_code::text = dm.donor_mobile::text
          GROUP BY dtt.camp_code, dm.blood_id
        UNION
         SELECT 'Total Donated'::text AS status,
            rht.camp_code,
            rtt.blood_code,
            sum(rtt.receiver_count) AS sum
           FROM blood_bank_management_system.reciver_transaction_table rtt
             JOIN blood_bank_management_system.reciver_hospital_table rht ON rht.hospital_code::text = rtt.hospital_code::text
          GROUP BY rht.camp_code, rtt.blood_code) tb
  GROUP BY tb.camp_code, tb.blood_id;

-------------------------------------------------------------Function---------------------------------------------------------------

/*This function is used to register new Hospital and Donation Camp in Database according
Case1 : when flag is ='C' then this service fetch details from object data and register 
Case2 : when flag is ='H' then it checks that provided location matched with any registered Camp
if not then it shows a message telling "There is No Donation Camp for Suggested Location"
else  hospital will be registered*/

CREATE OR REPLACE FUNCTION blood_bank_management_system.function_insert_camp_and_hospital(x_object_data text, flag character varying)
 RETURNS refcursor
 LANGUAGE plpgsql
AS $function$
declare
x_result refcursor:='cur';
var_camp_name varchar(100);
var_camp_address text;
var_camp_code varchar(100);
var_camp_location  varchar(100);
var_hospital_name varchar(100);
var_hospital_address text;
var_hospital_code varchar(100);
var_hospital_location varchar(100);
var_count_1 int;
begin

if flag='H' then

select (x_object_data)::json->>'hospital_location' hospital_location,
(x_object_data)::json->>'hospital_name' hospital_name,
(x_object_data)::json->>'hospital_address' hospital_address
into 
var_hospital_location,
var_hospital_name,
var_hospital_address ;

select count(1) into var_count_1 from blood_bank_management_system.donor_camp_table dct
where upper(camp_location)=upper(var_hospital_location);

if var_count_1>0 then

select camp_code into var_camp_code from blood_bank_management_system.donor_camp_table dct
where upper(camp_location)=upper(var_hospital_location) limit 1;

select replace('HOS-'||to_char(MAX(substring(rht.hospital_code,5)::int)+1,'000'),' ','')
into var_hospital_code
from blood_bank_management_system.reciver_hospital_table rht;

insert into reciver_hospital_table   
(hospital_name ,
hospital_address ,
hospital_period ,
hospital_code ,
camp_code,
hospital_location)
values
(var_hospital_name,
var_hospital_address,
to_char(current_timestamp::date,'YYYY')||'-'||to_char(current_timestamp::date+interval '10year','YYYY'),
var_hospital_code,
var_camp_code,
var_hospital_location);

open x_result for 
select jsonb_build_object('RESPONSE','New Hospital is added',
                          'CODE','01');
else

open x_result for
	select jsonb_build_object('RESPONSE','There is No Donation Camp for Suggested Location',
                              'CODE','00');
end if;



elsif flag='C' then

select (x_object_data)::json->>'camp_name' camp_name,
(x_object_data)::json->>'camp_address' camp_address,
(x_object_data)::json->>'camp_location' camp_location
into 
var_camp_name,
var_camp_address,
var_camp_location 
;

select replace('HR-'||to_char(MAX(substring(dct.camp_code,4)::int)+1,'000'),' ','') 
into var_camp_code 
from blood_bank_management_system.donor_camp_table dct;

insert into donor_camp_table 
(camp_name,camp_address,camp_period,camp_location,camp_code)
values
(var_camp_name,var_camp_address,to_char(current_timestamp::date,'YYYY')||'-'||to_char(current_timestamp::date+interval '10year','YYYY'),var_camp_location,var_camp_code);

open x_result for
select jsonb_build_object('RESPONSE','New Donation Camp is added',
                          'CODE','01');
end if;


return x_result;
end;
$function$
;



/*This Function is used to insert donor donation details 
it basically works as if the donor is new then first registeration of donor occurs and then trasanction of blood occurs*/

CREATE OR REPLACE FUNCTION blood_bank_management_system.function_insert_donor_transaction(x_object_data text)
 RETURNS refcursor
 LANGUAGE plpgsql
AS $function$
declare
x_result refcursor:='cur';
var_donor_name varchar(50);	
var_donor_age  varchar(50);	
var_camp_code  varchar(50);
var_blood_id  varchar(50);
var_donor_mobile  varchar(50);
var_location   varchar(50);
var_count int;
var_count_2 int;
begin
	select (x_object_data)::json->>'donor_name' donor_name,
(x_object_data)::json->>'donor_age' donor_age,
(x_object_data)::json->>'Location' "Location",
(x_object_data)::json->'blood_group'->>'code' blood_group,
(x_object_data)::json->>'donor_mobile' donor_mobile
into 
var_donor_name,	
var_donor_age,
var_location,
var_blood_id,
var_donor_mobile 
;

if var_donor_age::int<18 then
open x_result for
select jsonb_build_object('RESPONSE_MESSAGE','Donor Criteria Failed As Age Below 18',
                          'RESPONSE_CODE','00' ) as "object_data";
else
select count(1) into var_count from blood_bank_management_system.donor_mst dm 
where dm.donor_mobile= var_donor_mobile;

if var_count=0 then

select camp_code  into var_camp_code from donor_camp_table dct where upper(camp_location)=upper(var_location);

insert into donor_mst(donor_name,donor_age,camp_code,blood_id,donor_mobile)
values (var_donor_name,	var_donor_age,var_camp_code,var_blood_id,var_donor_mobile);
insert into donation_transaction_table (camp_code,donor_code,date_of_donation,transaction_id)
values (var_camp_code,var_donor_mobile,current_timestamp::date,'TXN'||replace(replace(replace(current_timestamp::time::varchar,'-','' ),'.','' ),':','' )||(random()*100)::int);
open x_result for
select jsonb_build_object('RESPONSE_MESSAGE','Donation & Donor Details Inserted Sucessfully',
                          'RESPONSE_CODE','01' ) as "object_data";
else 
select count(1) into var_count_2 from donation_transaction_table dtt where dtt.donor_code =var_donor_mobile
and 
age(date_of_donation,now()::date) <'3 months'; 
 
if var_count_2>0 then
	
open x_result for
select jsonb_build_object('RESPONSE_MESSAGE','Donor Criteria Failed As Donation had been done in Past 3 Months',
                          'RESPONSE_CODE','00' ) as "object_data";
else 
select camp_code  into var_camp_code from donor_camp_table dct where upper(camp_location)=upper(var_location);
insert into donation_transaction_table (camp_code,donor_code,date_of_donation,transaction_id)
values (var_camp_code,var_donor_mobile,current_timestamp::date,'TXN'||replace(replace(replace(current_timestamp::time::varchar,'-','' ),'.','' ),':','' )||(random()*100)::int);
open x_result for
select jsonb_build_object('RESPONSE_MESSAGE','Donation Details Inserted Sucessfully',
                          'RESPONSE_CODE','01' ) as "object_data";
                         end if;

end if;                  
end if;


return x_result;
end;
$function$
;

/*This Function is used to create transaction of the donation provided to hospital 
this registers transaction only and only if blood is available for that location camp with that blood group*/

CREATE OR REPLACE FUNCTION blood_bank_management_system.function_insert_receiver_transaction(x_object_data text)
 RETURNS refcursor
 LANGUAGE plpgsql
AS $function$
declare
x_result refcursor:='cur';
var_hospital_code varchar(50);	
var_reciver_name  varchar(50);	
var_reciver_age  varchar(50);
var_blood_id  varchar(50);
var_receiver_count  varchar(50);
var_gaurdian_mobile varchar(50);
var_count int;
var_count_2 int;
begin
	select (x_object_data)::json->>'hospital_code' hospital_code,
(x_object_data)::json->>'reciver_name' reciver_name,
(x_object_data)::json->>'Location' reciver_age,
(x_object_data)::json->'blood_group'->>'code' blood_id,
(x_object_data)::json->>'receiver_count' receiver_count,
(x_object_data)::json->>'mobile_no' mobile_no
into 
var_hospital_code,	
var_reciver_name,
var_reciver_age,
var_blood_id,
var_receiver_count,
var_gaurdian_mobile
;

select * into var_count from blood_bank_management_system.view_blood_availibilty vba  
where camp_code =(select camp_code from blood_bank_management_system.reciver_hospital_table rht where hospital_code=var_hospital_code);
 if  var_count<var_receiver_count then
open x_result for 
select jsonb_build_object('RESPONSE','Currently we have no. Blood under '||var_blood_id||' group.',
                           'CODE','00');
else 
 INSERT INTO blood_bank_management_system.reciver_transaction_table
(hospital_code, reciver_name, reciver_age, date_of_donation_released, blood_code, receiver_count,gaurdian_mobile)
VALUES(var_hospital_code, var_reciver_name, var_reciver_age, current_timestamp::date, var_blood_id, var_receiver_count,var_gaurdian_mobile);
 	
open x_result for 
select jsonb_build_object('RESPONSE','Registeration is Successful please wait for collection in collection room.',
                           'CODE','01');
 end if;

return x_result;
end;
$function$
;

-----------------------------------------------------------Stored Procedure---------------------------------------------------------------

/*Id there is issue that blood is not provided we can use this stored procedure to get potential donors that are registered in table and 
have no donation history of 3 months atleast*/

CREATE OR REPLACE FUNCTION blood_bank_management_system.stored_pro_get_potential_donor_list(x_hospital_code character varying, OUT list_donor json)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
declare
var_camp_code varchar(100);
begin
select rht.camp_code into  var_camp_code from blood_bank_management_system.reciver_hospital_table rht where hospital_code =x_hospital_code;
select json_build_object('NAME',donor_name,
                         'MOBILE',donor_mobile) into list_donor from blood_bank_management_system.donor_mst where camp_code =var_camp_code
                        and exists (select 1 from blood_bank_management_system.donation_transaction_table where 
                       age(now()::date,date_of_donation)>='3months');
end;
$function$
;
