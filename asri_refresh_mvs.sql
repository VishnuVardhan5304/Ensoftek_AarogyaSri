--drop materialized view dwh.active_empnl_hosp_specialities_mv;

create materialized view dwh.active_empnl_hosp_specialities_mv as			 
select
*, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(SELECT
ah.hosp_id,hosp_name as hospital_name, hosp_disp_code as hospital_display_code, hosp_empnl_ref_num as HSIN_Number, hosp_contact_no as authorized_person_number , status as hosp_status_code, cmb_dtl_name as hosp_status, hosp_bed_strength, hosp_email as hosp_email_id, 
case when ah.pan_number is not null then ah.pan_number
    when ah.pan_number is null and hp_info.pannumber is not null then hp_info.pannumber
else ' ' end as pan_card_number, speciality_id as speciality_code, dis_main_name as speciality_name,
 case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hospital_type, govt_hosp_type, /* NABH Number*/hosp_empnl_date,hosp_addr1 as hospital_address,mandal as mandal_code, md_loc.loc_name as mandal_name,md_loc.lgd_code as mandal_lgd_code,constituency_code, /*contituency_name */
city_code, city_name as hospital_city,district_code, d_loc.loc_name as district_name,d_loc.lgd_code as district_lgd_code, 
hospital_state, /*stop payment */ tds_exemp_status as exemption_status, nh.nabh_no , nh.active_yn , nh.valid_from , nh.valid_to
 FROM 
(SELECT 
hosp_id,hosp_empnl_ref_num, hosp_contact_no, hosp_name, hosp_disp_code,hosp_email, hosp_type, govt_hosp_type, hosp_empnl_date, hosp_addr1, hosp_city,  tds_exemp_status, pan_number ,dist_id
FROM dwh.asri_hospitals_dm where isactive_ap='Y') ah 
LEFT JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
/*LEFT JOIN (SELECT  hosp_id, speciality_id   FROM dwh.asri_hosp_speciality_dm where renewal=9) hp_sp ON hp_sp.hosp_id = ah.hosp_id*/
LEFT JOIN (select distinct hosp_id, speciality_id from
		(SELECT  hosp_id, speciality_id, crt_dt, ROW_NUMBER() OVER(partition by hosp_id, speciality_id, crt_dt ) as rwn   FROM dwh.asri_hosp_speciality_dm where renewal=9 and is_active_flg='Y')
		where rwn=1
)hp_sp ON hp_sp.hosp_id = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name,lgd_code from dwh.asri_locations_dm ) md_loc ON md_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select city_id , city_name from asri_major_city_dm )amcd on amcd.city_id  = hp_info.city_code
LEFT JOIN (SELECT dis_main_id, dis_main_name FROM dwh.asri_disease_main_cd) adm ON hp_sp.speciality_id = adm.DIS_MAIN_ID
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = hp_info.status
left join (select distinct hosp_id , nabh_no , active_yn , valid_from , valid_to  from dwh.asri_hosp_nabh_dtls_dm where valid_to>= CURRENT_TIMESTAMP ) nh on nh.hosp_id=ah.hosp_id
);





drop materialized view dwh.empnl_hosp_medco_dtls_mv;

create materialized view dwh.empnl_hosp_medco_dtls_mv as 
select 
ah.hosp_id, hosp_name as hospital_name,regno as regnum, hosp_disp_code as hospital_display_code, hosp_empnl_ref_num as HSIN_Number, hosp_contact_no as authorized_person_number , status as hosp_status_code, cmb.cmb_dtl_name as hosp_status, hosp_bed_strength, hosp_email as hosp_email_id,
case when ah.pan_number is not null then ah.pan_number
    when ah.pan_number is null and hp_info.pannumber is not null then hp_info.pannumber
else ' ' end as pan_card_number,
case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hospital_type, govt_hosp_type,hosp_empnl_date,hosp_addr1 as hospital_address,mandal as mandal_code, md_loc.loc_name as mandal_name,md_loc.lgd_code as mandal_lgd_code,constituency_code,   city_code, city_name as hospital_city,district_code, d_loc.loc_name as district_name,d_loc.lgd_code as district_lgd_code, hospital_state,au.user_role as user_role_code, cmb1.cmb_dtl_name as user_role,
mu.user_id as medco_user_id,au.login_name as medco_login_name,au.user_name as medco_name, active_yn,doctor_mapped_speciality_name,
case when eff_end_dt is null then 'Working'
else 'Not Working' end as medco_status,
eff_start_dt as medco_job_started_date, eff_end_dt as medco_job_ended_date, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(SELECT 
hosp_id,hosp_empnl_ref_num, hosp_contact_no, hosp_name, hosp_disp_code,hosp_email, hosp_type, govt_hosp_type, hosp_empnl_date, hosp_addr1, hosp_city,  tds_exemp_status, pan_number, isactive_ap, dist_id
FROM dwh.asri_hospitals_dm where isactive_ap='Y') ah 
left JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, hosp_state, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
left join (select distinct user_id, hosp_id, eff_start_dt,eff_end_dt from dwh.asri_nwh_users_dm ) mu on mu.hosp_id = ah.hosp_id
inner join (select distinct regno,user_id, login_name ,(NVL(first_name,'') + ' ' +NVL(last_name,'')) as user_name,active_yn, user_role, crt_dt, lst_upd_dt  from dwh.asri_users_dm where active_yn='Y' and user_role='CD9' ) au on au.user_id = mu.user_id
left JOIN (SELECT loc_id, loc_name,lgd_code from dwh.asri_locations_dm ) md_loc ON md_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = hp_info.status
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb1 ON cmb1.cmb_dtl_id = au.user_role
left join (select distinct city_id , city_name from dwh.asri_major_city_dm )amcd on amcd.city_id  = hp_info.city_code
left join (select distinct reg_num as ds_regnum,spclty_code from dwh.asri_doctor_splty_dm where is_activeyn='Y') sds on sds.ds_regnum = au.regno
LEFT JOIN ( SELECT dis_main_id, dis_main_name as doctor_mapped_speciality_name  FROM dwh.asri_disease_main_cd ) dm_p ON dm_p.dis_main_id = sds.spclty_code;





 drop materialized view dwh.asri_active_mithra_details_mv;

create materialized view dwh.asri_active_mithra_details_mv as
select aud.user_id, aud.new_emp_code , (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS M_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  ,  aud.active_yn , aud.cug ,
ahd.dist_id  as hosp_dist , ald.loc_name as hosp_dist_name , hospital_state as hosp_state_name,ahd.hosp_id , ahd.hosp_name, 
case when ahd.hosp_type='C' then 'Corporate' when ahd.hosp_type='G' then 'Government' end as hosp_type , ahd.govt_hosp_type , amud.eff_end_dt  as end_dt,  MAX(amud.eff_start_dt) as start_dt ,
  'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM MAX(amud.eff_start_dt)) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM MAX(amud.eff_start_dt)) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM MAX(amud.eff_start_dt)), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM MAX(amud.eff_start_dt)), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM MAX(amud.eff_start_dt)) + 1, 100), 'FM00') END) AS FY_work_start_date ,CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  
       from dwh.asri_users_dm where active_yn ='Y' and user_role IN ('CD10','CD11')) aud 
inner join (select user_id , hosp_id , eff_start_dt, eff_end_dt from asri_mit_users_dm where eff_end_dt is null ) amud on amud.user_id = aud.user_id
left join (select hosp_id , hosp_name ,  hosp_type ,  govt_hosp_type , dist_id  from asri_hospitals_dm  ) ahd on ahd.hosp_id  = amud.hosp_id 
left join (select loc_id , loc_name, loc_parnt_id  from asri_locations_dm )ald on ald.loc_id = ahd.dist_id 
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = ald.loc_parnt_id
group by aud.user_id, aud.new_emp_code , (NVL(aud.first_name , '') || ' ' || NVL(aud.last_name, '')) , aud.gender , aud.active_yn , aud.cug ,
ahd.dist_id , ald.loc_name ,hospital_state, ahd.hosp_id , ahd.hosp_name, ahd.hosp_type , ahd.govt_hosp_type , amud.eff_end_dt ;






drop materialized view dwh.asri_igrt_mv;

create materialized view dwh.asri_igrt_mv as 
SELECT
    acf.case_id,
    acf.pool_id,
    apd.ration_card_no,
    apd.uhidvalue,
    apd.age,
    CASE
        WHEN apd.age <= 5 THEN '0-5'
        WHEN apd.age BETWEEN 6 AND 10 THEN '06-10'
        WHEN apd.age BETWEEN 11 AND 15 THEN '11-15'
        WHEN apd.age BETWEEN 16 AND 20 THEN '16-20'
        WHEN apd.age BETWEEN 21 AND 25 THEN '21-25'
        WHEN apd.age BETWEEN 26 AND 30 THEN '26-30'
        WHEN apd.age BETWEEN 31 AND 35 THEN '31-35'
        WHEN apd.age BETWEEN 36 AND 40 THEN '36-40'
        WHEN apd.age BETWEEN 41 AND 45 THEN '41-45'
        WHEN apd.age BETWEEN 46 AND 50 THEN '46-50'
        WHEN apd.age BETWEEN 51 AND 55 THEN '51-55'
        WHEN apd.age BETWEEN 56 AND 60 THEN '56-60'
        WHEN apd.age BETWEEN 61 AND 65 THEN '61-65'
        WHEN apd.age BETWEEN 66 AND 70 THEN '66-70'
        WHEN apd.age BETWEEN 71 AND 75 THEN '71-75'
        WHEN apd.age BETWEEN 76 AND 80 THEN '76-80'
        WHEN apd.age BETWEEN 81 AND 85 THEN '81-85'
        WHEN apd.age BETWEEN 86 AND 90 THEN '86-90'
        WHEN apd.age BETWEEN 91 AND 95 THEN '91-95'
        ELSE '>95'
    END AS age_frequency,
    case when apd.gender='F' then 'Female' --AP7521223
        when apd.gender='M' then 'Male'
    end as gender,
    acsd.surgery_code,
    asd.surgery_desc,
    acsd.dis_main_code,
    acf.case_hosp_code,
    ah.hosp_name,
    acf.case_patient_no,
    acf.case_status,
    acf.cs_preauth_dt,
    acf.cs_death_dt,
    acf.cs_apprv_rej_dt,
    acf.cs_surg_dt,
    acf.cs_dis_dt,
    acf.cs_cl_amount ,CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
FROM
    dwh.asri_case_ft acf
INNER JOIN
    dwh.asri_case_surgery_dm acsd ON acsd.case_id = acf.case_id
INNER JOIN
    dwh.asri_patient_dm apd ON apd.patient_id = acf.case_patient_no
INNER JOIN
    (SELECT hosp_id,hosp_name FROM dwh.asri_hospitals_dm) ah ON ah.hosp_id = acf.case_hosp_code
INNER JOIN
    (select surgery_id,surgery_desc 
from (select surgery_id,surgery_desc,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1) asd ON asd.surgery_id = acsd.surgery_code
WHERE
    acf.cs_dis_main_code = 'S13'
    AND acsd.surgery_code IN ('MR008A', 'MR008B', 'MR008C', 'MR009A', 'MR009B', 'MR009C', 'MR011A', 'S13.6.1')
    AND (acf.cs_apprv_rej_dt BETWEEN TO_DATE('2019-06-01', 'YYYY-MM-DD') AND TO_DATE('2024-03-15', 'YYYY-MM-DD')); 
   
   
   
 
   
   
drop materialized view dwh.asri_empnl_duty_doctors_hosp_dtls_mv;

create materialized view dwh.asri_empnl_duty_doctors_hosp_dtls_mv as
select 
ah.hosp_id, hosp_name as hospital_name, hosp_disp_code as hospital_display_code, hosp_empnl_ref_num as HSIN_Number, hosp_contact_no as authorized_person_number , status as hosp_status_code, cmb.cmb_dtl_name as hosp_status, hosp_bed_strength, hosp_email as hosp_email_id,
case when ah.pan_number is not null then ah.pan_number
    when ah.pan_number is null and hp_info.pannumber is not null then hp_info.pannumber
else ' ' end as pan_card_number,
case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hospital_type, govt_hosp_type,hosp_empnl_date,hosp_addr1 as hospital_address,mandal as mandal_code, md_loc.loc_name as mandal_name,md_loc.lgd_code as mandal_lgd_code,constituency_code, city_code, city_name as hospital_city, district_code, d_loc.loc_name as district_name,d_loc.lgd_code as district_lgd_code, 
hospital_state, req_no as duty_doctor_req_no, reg_num as duty_doctor_registered_num, duty_doctor_name,university, experience, contactno,ad.is_activeyn as duty_doctor_active_YN, apprv_status, cmb1.cmb_dtl_name as duty_doctor_approval_status, doctor_mapped_speciality_name,
case when ad.is_activeyn='Y' then 'Working'
when ad.is_activeyn='N' then  'Not Working' end as duty_doctor_working_status,
ad.crt_dt as duty_doctor_job_started_date, ad.lst_upd_dt as duty_doctor_job_ended_date, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(SELECT 
hosp_id,hosp_empnl_ref_num, hosp_contact_no, hosp_name, hosp_disp_code,hosp_email, hosp_type, govt_hosp_type, hosp_empnl_date, hosp_addr1, hosp_city,  tds_exemp_status, pan_number, isactive_ap, dist_id
FROM dwh.asri_hospitals_dm where isactive_ap='Y') ah 
LEFT JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, hosp_state,city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
left join (select distinct hosp_id,req_no, NVL(dctr_name,'') as duty_doctor_name, reg_num, university, experience, contactno,is_activeyn, apprv_status, lst_upd_dt, crt_dt from dwh.asri_duty_dctrs_dm ) ad on ad.hosp_id = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name,lgd_code from dwh.asri_locations_dm ) md_loc ON md_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select distinct city_id , city_name from dwh.asri_major_city_dm )amcd on amcd.city_id  = hp_info.city_code
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = hp_info.status
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb1 ON cmb1.cmb_dtl_id = ad.apprv_status
left join (select distinct reg_num as ds_regnum, spclty_code from dwh.asri_doctor_splty_dm where is_activeyn='Y') sds on sds.ds_regnum = ad.reg_num
LEFT JOIN ( SELECT dis_main_id, dis_main_name as doctor_mapped_speciality_name  FROM dwh.asri_disease_main_cd ) dm_p ON dm_p.dis_main_id = sds.spclty_code;







drop materialized view dwh.active_empnl_hosp_specialities_mv;

create materialized view dwh.active_empnl_hosp_specialities_mv as			 
select
*
from 
(SELECT
ah.hosp_id,hosp_name as hospital_name, hosp_disp_code as hospital_display_code, hosp_empnl_ref_num as HSIN_Number, hosp_contact_no as authorized_person_number , status as hosp_status_code, cmb_dtl_name as hosp_status, hosp_bed_strength, hosp_email as hosp_email_id, 
case when ah.pan_number is not null then ah.pan_number
    when ah.pan_number is null and hp_info.pannumber is not null then hp_info.pannumber
else ' ' end as pan_card_number, speciality_id as speciality_code, dis_main_name as speciality_name,
 case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hospital_type, govt_hosp_type, /* NABH Number*/hosp_empnl_date,hosp_addr1 as hospital_address,mandal as mandal_code, md_loc.loc_name as mandal_name,md_loc.lgd_code as mandal_lgd_code,constituency_code, /*contituency_name */
city_code, city_name as hospital_city,district_code, d_loc.loc_name as district_name,d_loc.lgd_code as district_lgd_code, 
hospital_state, /*stop payment */ tds_exemp_status as exemption_status, nh.nabh_no , nh.active_yn , nh.valid_from , nh.valid_to
 FROM 
(SELECT 
hosp_id,hosp_empnl_ref_num, hosp_contact_no, hosp_name, hosp_disp_code,hosp_email, hosp_type, govt_hosp_type, hosp_empnl_date, hosp_addr1, hosp_city,  tds_exemp_status, pan_number ,dist_id
FROM dwh.asri_hospitals_dm where isactive_ap='Y') ah 
LEFT JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
/*LEFT JOIN (SELECT  hosp_id, speciality_id   FROM dwh.asri_hosp_speciality_dm where renewal=9) hp_sp ON hp_sp.hosp_id = ah.hosp_id*/
LEFT JOIN (select distinct hosp_id, speciality_id from
		(SELECT  hosp_id, speciality_id, crt_dt, ROW_NUMBER() OVER(partition by hosp_id, speciality_id, crt_dt ) as rwn   FROM dwh.asri_hosp_speciality_dm where renewal=9 and is_active_flg='Y')
		where rwn=1
)hp_sp ON hp_sp.hosp_id = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name,lgd_code from dwh.asri_locations_dm ) md_loc ON md_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select city_id , city_name from asri_major_city_dm )amcd on amcd.city_id  = hp_info.city_code
LEFT JOIN (SELECT dis_main_id, dis_main_name FROM dwh.asri_disease_main_cd) adm ON hp_sp.speciality_id = adm.DIS_MAIN_ID
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = hp_info.status
left join (select distinct hosp_id , nabh_no , active_yn , valid_from , valid_to  from dwh.asri_hosp_nabh_dtls_dm where valid_to>= CURRENT_TIMESTAMP ) nh on nh.hosp_id=ah.hosp_id
);





drop materialized view dwh.asri_hosp_daily_feedback_mv;

create materialized view dwh.asri_hosp_daily_feedback_mv as 
select 
--count(hosp_feedback_id),
ahff.hosp_id, hosp_name,hosp_type,govt_hosp_type,dist_id as hospital_dist_id,hospital_district, hospital_state,
    a.range_value AS OUT_PAT_CONS_CASHLESS_MOD,
    b.range_value AS OUT_PAT_INVEST_CASHLESS_MODE,
    c.range_value AS CASH_LESS_TRMT_FOR_BENF,
    d.range_value AS WARD_CLEAN_FREQ,
    e.range_value AS SEPERATE_TOILET_MALE_FEMALE,
    f.range_value AS TOILET_CLEAN_FREQ,
    g.range_value AS BED_LINEN_CHANGE_FREQ,
    h.range_value AS BIOMED_WASTE_MGMNT,
    i.range_value AS CLEAN_DRINKING_WATER_AVAILABLE,
    j.range_value AS BREAKFAST_TIME,
    k.range_value AS LUNCH_TIME,
    l.range_value AS DINNER_TIME,
    m.range_value AS BREAKFAST_QUANTITY_QUALITY,
    n.range_value AS LUNCH_QUANTITY_QUALITY,
    o.range_value AS DINNER_QUANTITY_QUALITY,
    p.range_value AS HELPDESK_AVIALABILITY,
    q.range_value AS COMPUTER_FACILITY,
    r.range_value AS TELEPHONE_FACILITY,
    s.range_value AS DAILY_VISIT_SPLST_DCTRS,
    t.range_value AS DUTY_DCTRS_WRKNG_DAY,
    u.range_value AS TIMELY_SURG_WITHOUT_DELAY,
    v.range_value AS DISCH_COUNS_FOLL_UP_ADV_BY_DOC,
    w.range_value AS NO_OF_NURSING_WORKING_ON_DAY,
    x.range_value AS NURSE_TIME_MED_DISP_PAT,
    y.range_value AS NURSE_TIME_DISCH_PAT,
    z.range_value AS QUALITY_OF_MED_DISP_PAT,
    aa.range_value AS DISCH_MED_DISP_AT_10_DAYS,
    ab.range_value AS TRANSPORT_ALLOW_PAID_PAT,
    ac.range_value AS ASARA_LETTER_ACK_GIVEN_PAT,
aud.user_id, aud.new_emp_code , (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS MITHRA_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug as phno,
ahff.crt_dt as feedback_submitted_date,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ahff.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ahff.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ahff.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ahff.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ahff.crt_dt) + 1, 100), 'FM00') END) AS FY_feedback_submitted,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt     
from 
(select * from dwh.asri_hospital_feedback_form_dm) ahff 
left join (select hosp_id, hosp_name,dist_id,    case when hosp_type='C' then 'Corporate'
				when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type  from dwh.asri_hospitals_dm ) ah on ah.hosp_id=ahff.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) a on ahff.OUT_PAT_CONS_CASHLESS_MODE = a.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) b ON ahff.OUT_PAT_INVEST_CASHLESS_MODE = b.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) c  on ahff.CASH_LESS_TRMT_FOR_BENF = c.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) d ON ahff.WARD_CLEAN_FREQ = d.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) e ON ahff.SEPERATE_TOILET_MALE_FEMALE = e.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) f ON ahff.TOILET_CLEAN_FREQ = f.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) g ON ahff.BED_LINEN_CHANGE_FREQ = g.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) h on ahff.BIOMED_WASTE_MGMNT = h.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) i ON ahff.CLEAN_DRINKING_WATER_AVAILABLE = i.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) j on ahff.BREAKFAST_TIME = j.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) k ON ahff.LUNCH_TIME = k.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) l ON ahff.DINNER_TIME = l.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) m ON  ahff.BREAKFAST_QUANTITY_QUALITY = m.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) n ON ahff.LUNCH_QUANTITY_QUALITY = n.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) o ON  ahff.DINNER_QUANTITY_QUALITY = o.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) p ON ahff.HELPDESK_AVIALABILITY = p.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) q ON ahff.COMPUTER_FACILITY = q.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) r ON ahff.TELEPHONE_FACILITY = r.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) s  ON ahff.DAILY_VISIT_SPLST_DCTRS = s.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) t ON ahff.DUTY_DCTRS_WRKNG_DAY = t.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) u ON  ahff.TIMELY_SURG_WITHOUT_DELAY = u.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) v ON ahff.DISCH_COUNS_FOLL_UP_ADV_BY_DOC = v.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) w ON ahff.NO_OF_NURSING_WORKING_ON_DAY = w.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) x ON ahff.NURSE_TIME_MED_DISP_PAT = x.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) y  ON ahff.NURSE_TIME_DISCH_PAT = y.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) z ON ahff.QUALITY_OF_MED_DISP_PAT = z.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) aa ON ahff.DISCH_MED_DISP_AT_10_DAYS = aa.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) ab ON ahff.TRANSPORT_ALLOW_PAID_PAT = ab.range_id
left join (select range_id , range_value  from dwh.hosp_feedback_values_mst_dm) ac ON ahff.ASARA_LETTER_ACK_GIVEN_PAT = ac.range_id
left join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) aud on aud.user_id = ahff.crt_usr;




drop materialized view dwh.asri_empnl_paramedics_doctors_hosp_dtls_mv;

create materialized view dwh.asri_empnl_paramedics_doctors_hosp_dtls_mv as
select 
ah.hosp_id, hosp_name as hospital_name, hosp_disp_code as hospital_display_code, hosp_empnl_ref_num as HSIN_Number, hosp_contact_no as authorized_person_number , status as hosp_status_code, cmb.cmb_dtl_name as hosp_status, hosp_bed_strength, hosp_email as hosp_email_id,
case when ah.pan_number is not null then ah.pan_number
    when ah.pan_number is null and hp_info.pannumber is not null then hp_info.pannumber
else ' ' end as pan_card_number,
case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hospital_type, govt_hosp_type,hosp_empnl_date,hosp_addr1 as hospital_address,mandal as mandal_code, md_loc.loc_name as mandal_name,md_loc.lgd_code as mandal_lgd_code,constituency_code,  city_code, city_name as hospital_city,district_code, d_loc.loc_name as district_name,d_loc.lgd_code as district_lgd_code, 
hospital_state, req_no as paramedics_doctor_req_no, reg_num as paramedics_doctor_registered_num, paramedics_doctor_name,university, experience, contactno,pd.is_activeyn as paramedics_doctor_active_YN, apprv_status, cmb1.cmb_dtl_name as paramedics_doctor_approval_status, doctor_mapped_speciality_name,
case when pd.is_activeyn='Y' then 'Working'
when pd.is_activeyn='N' then  'Not Working' end as paramedics_doctor_working_status,
pd.crt_dt as paramedics_doctor_job_started_date, pd.lst_upd_dt as paramedics_doctor_job_ended_date
from 
(SELECT 
hosp_id,hosp_empnl_ref_num, hosp_contact_no, hosp_name, hosp_disp_code,hosp_email, hosp_type, govt_hosp_type, hosp_empnl_date, hosp_addr1, hosp_city,  tds_exemp_status, pan_number, isactive_ap, dist_id
FROM dwh.asri_hospitals_dm where isactive_ap='Y') ah 
INNER JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, hosp_state,city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
inner join (select distinct  hosp_id,req_no,prmdc_id, NVL(prmdc_name,'') as paramedics_doctor_name, reg_num, university, experience, contactno,is_activeyn, apprv_status, lst_upd_dt, crt_dt from dwh.asri_paramedics_dm  ) pd on pd.hosp_id = ah.hosp_id
INNER JOIN (SELECT loc_id, loc_name,lgd_code from dwh.asri_locations_dm ) md_loc ON md_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = hp_info.status
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb1 ON cmb1.cmb_dtl_id = pd.apprv_status
left join (select city_id , city_name from dwh.asri_major_city_dm )amcd on amcd.city_id  = hp_info.city_code
left join (select distinct reg_num as ds_regnum, spclty_code from dwh.asri_doctor_splty_dm where is_activeyn='Y') sds on sds.ds_regnum = pd.reg_num
LEFT JOIN ( SELECT dis_main_id, dis_main_name as doctor_mapped_speciality_name  FROM dwh.asri_disease_main_cd ) dm_p ON dm_p.dis_main_id = sds.spclty_code;






drop materialized view dwh.delivery_case_details_mv;

create materialized view dwh.delivery_case_details_mv as
select 
ac.case_id, ac.case_no, 
ac.cs_dis_main_code as speciality_code , dis_main_name as speciality_name,asd.surgery_id as procedure_code,asd.surgery_desc as procedure_name, 
case when asd.surgery_id in ('S4.1.6','S4.1.10','S4.1.11','S4.1.13','S4.1.14','S4.1.15','S4.1.17','S4.1.18','S4.1.6.1','S4.1.6.2','S4.1.9') then 'Ceserean'
	when asd.surgery_id in ( 'S4.1.5','S4.1.5.1','S4.1.5.2','S4.1.5.3','S4.1.5.6' ) then 'Normal Delivery' else null end as delivery_type,
NVL(asd.surgery_desc,'')||'_'||NVL(asd.surgery_amt,'0')||'_'||NVL(asd.postops_amt,'0') as proc_surg_aasra_amt_desc,
NVL(postops_amt,0) as postops_amt , asd.surgery_amt as procedure_defined_amount,
ac.case_status  as case_status_code,cmb.cmb_dtl_name as case_status, 
ac.case_regn_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.case_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) + 1, 100), 'FM00') END) AS fy_case_regn,
ac.cs_apprv_rej_dt,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.cs_apprv_rej_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS fy_preauth_aprv,
ac.cs_surg_dt as surgery_date ,cs_dis_dt as discharge_date,
ah.hosp_id, ah.hosp_name, ah.hosp_type,ah.govt_hosp_type,ah.hosp_empnl_ref_num , ah.hosp_empnl_date, hospital_district, hospital_state,hosp_md_ceo_name , hosp_md_mob_ph, hosp_md_email,hosp_active_status,aehd.status, cmb2.cmb_dtl_name as hospital_status,hosp_bed_strength,
ap.PATIENT_ID, ap.patient_name, ap.age, patient_gender, ap.date_of_birth, patient_ipop, ap.RATION_CARD_NO, FAMILY_CARD_NO, ap.uhidvalue ,PATIENT_ADDRESS,ap.contact_no as patient_phno, ap.reg_hosp_id ,  patient_mandal, patient_district, patient_state, ap.reg_hosp_date as patient_hosp_regn_date, patient_caste,
parity , gravida,gravida_para, abortion ,live_birth ,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select case_id , case_no, case_hosp_code , case_patient_no , case_status , case_regn_date , cs_surg_dt  ,cs_dis_dt , cs_dis_main_code , lst_upd_dt, cs_apprv_rej_dt from dwh.asri_case_ft ) ac
inner  join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm where surgery_code in ('S4.1.6','S4.1.10','S4.1.11','S4.1.13','S4.1.14','S4.1.15','S4.1.17','S4.1.18','S4.1.6.1','S4.1.6.2','S4.1.9','S4.1.5','S4.1.5.1','S4.1.5.2','S4.1.5.3','S4.1.5.6') ) acs on acs.case_id = ac.case_id
left join (select cmb_dtl_id, cmb_dtl_name from dwh.asri_combo_cd ) cmb on cmb.cmb_dtl_id = ac.case_status
left join (select hosp_id , hosp_name , hosp_contact_person , hosp_contact_no , cug_no , hosp_city , NVL(hosp_addr1,'')||','||NVL(hosp_addr2,'')||','||NVL(hosp_addr3,'') as hospital_address,case when isactive_ap='Y' then 'Active'else  'DeActive'  end as hosp_active_status,
	  			hosp_email , case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type, dist_id , hosp_empnl_ref_num , hosp_empnl_date
			from dwh.asri_hospitals_dm 
		 ) ah on ac.case_hosp_code = ah.hosp_id
left join (select hospinfo_id , hosp_md_ceo_name , hosp_md_mob_ph, hosp_md_email,status,hosp_bed_strength  from dwh.asri_empnl_hospinfo_dm) aehd on aehd.hospinfo_id=ah.hosp_empnl_ref_num
LEFT JOIN (SELECT PATIENT_ID,NVL(first_name,'')||' '||NVL(middle_name,'')||' '||NVL(last_name,'') as patient_name, age, case when gender='M' then 'Male' when gender='F' then 'Female' end  AS patient_gender,
			 date_of_birth, DISTRICT_CODE,MANDAL_CODE,RATION_CARD_NO, Village_code,patient_ipop, relation, contact_no,
		  CASE WHEN POSITION('/' IN RATION_CARD_NO) > 0  THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1) ELSE RATION_CARD_NO  end AS FAMILY_CARD_NO, uhidvalue , reg_hosp_id , reg_hosp_date, NVL(addr1,' ') || NVL(addr2,' ') || NVL(addr3,' ') AS PATIENT_ADDRESS, caste as patient_caste
          FROM dwh.asri_patient_dm
   		) ap on ap.patient_id = ac.case_patient_no	
left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = ac.cs_dis_main_code
left join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt,rest_days
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rest_days,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code
left join (select cmb_dtl_id , cmb_dtl_name  from dwh.asri_combo_cd) cmb2 on cmb2.cmb_dtl_id = aehd.status
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
LEFT JOIN ( SELECT loc_id, loc_name AS patient_mandal FROM dwh.asri_locations_dm) lp_m ON lp_m.loc_id = ap.mandal_code
LEFT JOIN (SELECT loc_id, loc_name as patient_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) pd_loc on pd_loc.loc_id = ap.district_code
LEFT JOIN (SELECT loc_id, loc_name as patient_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) ps_loc ON ps_loc.loc_id = pd_loc.loc_parnt_id
left join (select distinct case_id, parity , gravida,gravida||' _ '||parity as gravida_para, abortion ,live_birth
from 
(select *, row_number() OVER(partition by case_id order by  crt_dt desc ) as rwn   FROM rawdata.obstretric_history )
where rwn=1
) roh on roh.case_id=ac.case_id;





drop materialized view dwh.asri_ppd_cpd_performance_mv;

create materialized view dwh.asri_ppd_cpd_performance_mv as
select 
*, 
case when act_id in ('CD121','CD120','CD1181','CD1182','CD119','CD20053','CD1184') then 'CPD Actions'
when act_id in ('CD3017','CD304','CD3018','CD302','CD303') then 'PPD Actions' 
end as action_type
from 
(select 
au.login_name as panel_doctor_login_name,au.user_id as panel_doctor_user_id,FIRST_NAME,LAST_NAME, au.USER_NAME as panel_doctor_user_name,USER_ROLE, acm.cmb_dtl_name as user_role_name,aud.crt_dt as action_taken_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS action_taken_FYear,
       aud.act_id, acm2.CMB_DTL_NAME as action_taken ,ac.case_id, ac.cs_dis_main_code as case_speciality_code, dm.dis_main_name as case_speciality_name, ac.CASE_HOSP_CODE, ah.hosp_name as case_hospital_name, hospital_district , hospital_state ,
panel_doctor_mapped_hospitals,panel_doctor_mapped_specilaities, NVL(pd_d.loc_name,semd.district) as panel_doctor_district,
ROW_NUMBER() OVER(PARTITION BY au.user_id,ac.case_id, aud.act_id,aud.crt_dt ) as rwn,
case when ah.hosp_type='C' then 'Corporate'
when ah.hosp_type='G' then 'Government' end as hospital_type,
ah.govt_hosp_type
from
(select case_id , act_id , act_by , act_by_fk, crt_dt from dwh.asri_audit_ft where act_id in ('CD3017','CD304','CD3018','CD302','CD303','CD121','CD120','CD1181','CD1182','CD119','CD20053','CD1184') and  TRUNC(crt_dt) >= TO_DATE('2024-03-01','YYYY-MM-DD')) aud 
left  join (SELECT login_name, USER_ID,user_sk,FIRST_NAME,LAST_NAME,NVL(FIRST_NAME,' ')||' '||NVL(LAST_NAME,' ') as  USER_NAME,USER_ROLE,user_role_fk FROM dwh.asri_users_dm where active_yn='Y')  au on aud.act_by = au.user_id
left join (select case_id,case_hosp_code , case_patient_no, cs_dis_main_code from dwh.asri_case_ft) ac on ac.case_id = aud.case_id
LEFT JOIN (SELECT hosp_id, hosp_name, dist_id AS hosp_district_id, hosp_type, govt_hosp_type FROM dwh.asri_hospitals_dm) ah ON ac.CASE_HOSP_CODE= ah.hosp_id
LEFT JOIN (SELECT CMB_DTL_ID,cmb_dtl_id_sk,CMB_DTL_NAME FROM dwh.asri_combo_cd) acm ON au.user_role_fk = acm.cmb_dtl_id_sk
LEFT JOIN (select CMB_DTL_ID,cmb_dtl_id_sk,CMB_DTL_NAME FROM dwh.asri_combo_cd) acm2 ON acm2.cmb_dtl_id = aud.act_id
left join (select  user_id , LISTAGG(distinct hosp_id, ',') AS panel_doctor_mapped_hospitals from dwh.asri_insurance_hosp_mst_dm where active_yn='Y' group by user_id ) aph on aph.user_id = au.USER_ID
LEFT JOIN (
    SELECT distinct dis_main_id, dis_main_name  FROM dwh.asri_disease_main_cd
) dm ON dm.dis_main_id = ac.cs_dis_main_code
left join (select distinct ins_doc_id ,LISTAGG(distinct disease_id, ',') AS panel_doctor_mapped_specilaities  from dwh.asri_insurance_mst_dm where active='Y' group by ins_doc_id ) iu on iu.ins_doc_id = au.user_id
LEFT JOIN (
    SELECT loc_id, loc_parnt_id, loc_name AS hospital_district
    FROM dwh.asri_locations_dm
) lh_d ON lh_d.loc_id = ah.hosp_district_id
LEFT JOIN (
    SELECT loc_id, loc_parnt_id, loc_name AS hospital_state
    FROM dwh.asri_locations_dm
) lh_s on lh_s.loc_id = lh_d.loc_parnt_id
left join (select emp_id, district from
              (select emp_id, district, ROW_NUMBER() OVER(PARTITION BY emp_id ORDER BY crt_dt DESC) as rwn from dwh.sgvc_emp_mst_dm )
                 where rwn=1
          )semd on semd.emp_id = au.user_id
left join (select loc_id, loc_name from dwh.asri_locations_dm) pd_d  on pd_d.loc_id= semd.district
order by au.user_id
) ;






drop materialized view dwh.medco_to_nam_forwarded_delay_mv;

create materialized view dwh.medco_to_nam_forwarded_delay_mv as
select 
    ac.case_id,case_hosp_code,hosp_name,hospital_mandal,hospital_district,hospital_state,case when ah.hosp_type='C' then 'Corporate' when ah.hosp_type='G' then 'Government' end as hosp_type ,govt_hosp_type,ac.cs_dis_main_code as speciality_code,speciality_name,
     aud.user_id, aud.new_emp_code ,cmb_dtl_name as user_role, (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS NAM_NAME,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug,
    cs_preauth_dt as medco_forwarded_date,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_preauth_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_preauth_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_preauth_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_preauth_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_preauth_dt) + 1, 100), 'FM00') END) AS FY_medco_forwarded,
       CASE 
        WHEN EXTRACT(MONTH FROM cs_preauth_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM cs_preauth_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM cs_preauth_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS medco_forwarded_quarter,
       cs_dt_pre_auth as nam_forwarded_date,
          'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth) + 1, 100), 'FM00') END) AS FY_nam_forwarded,
     CASE 
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS nam_forwarded_quarter,
    DATEDIFF(hour,cs_preauth_dt,cs_dt_pre_auth) AS medco_forwarded_nam_forwarded_diff_hours,
    DATEDIFF(hour,cs_preauth_dt,cs_dt_pre_auth)/24.0 AS medco_forwarded_nam_forwarded_diff_days
FROM (select case_id,case_hosp_code,cs_dis_main_code,case_regn_date,cs_preauth_dt,cs_dt_pre_auth,CASE_PATIENT_NO from dwh.asri_case_ft
     where trunc(cs_dt_pre_auth)>=TO_DATE('2023-04-01', 'YYYY-MM-DD') and cs_preauth_dt is not null and cs_dt_pre_auth is not null) ac
left join (select case_id,act_by,crt_dt from dwh.asri_audit_ft where act_id in ('CD76')) aa on ac.case_id = aa.case_id
left join (select dis_main_id,dis_main_name as speciality_name from dwh.asri_disease_main_cd) adm on ac.cs_dis_main_code = adm.dis_main_id
left join (select hosp_id,hosp_name,hosp_type,govt_hosp_type,dist_id,hosp_empnl_ref_num from dwh.asri_hospitals_dm) ah on ac.case_hosp_code = ah.hosp_id
left join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) aud on aud.user_id = aa.act_by
LEFT JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
LEFT JOIN (SELECT loc_id, loc_name as hospital_mandal, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) m_loc ON m_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select cmb_dtl_id, cmb_dtl_name from dwh.asri_combo_cd) acc on acc.cmb_dtl_id = aud.user_role;




drop materialized view dwh.asri_nam_to_preauth_forwarding_delay_mv;

create materialized view dwh.asri_nam_to_preauth_forwarding_delay_mv as
select  
    acf.case_id,case_hosp_code,ah.hosp_name,case when ah.hosp_type='C' then 'Corporate' when ah.hosp_type='G' then 'Government' end as hosp_type ,ah.govt_hosp_type,hospital_mandal,hospital_district,hospital_state, acf.cs_dis_main_code as speciality_code,speciality_name,
    aud.user_id, aud.new_emp_code ,cmb_dtl_name as user_role_name, (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS MITHRA_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug,
    cs_dt_pre_auth as nam_forwarded_date,pex_verified_date,
         'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dt_pre_auth) + 1, 100), 'FM00') END) AS FY_nam_forwarded,
     CASE 
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS nam_forwarded_quarter,
         'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM pex_verified_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM pex_verified_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM pex_verified_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM pex_verified_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM pex_verified_date) + 1, 100), 'FM00') END) AS FY_PEX_Verified,
     CASE 
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM cs_dt_pre_auth) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS PEX_Verified_quarter,
    DATEDIFF(hour,cs_dt_pre_auth,pex_verified_date)/24.0 AS nam_forwarded_pex_verified_diff_days,
    ROUND(DATEDIFF('hour', cs_dt_pre_auth,pex_verified_date), 1) AS nam_forwarded_pex_verified_diff_hours
FROM (select case_id,case_hosp_code,cs_dis_main_code,case_regn_date,cs_preauth_dt,cs_dt_pre_auth,CASE_PATIENT_NO from dwh.asri_case_ft) acf
inner join (select case_id,crt_dt as pex_verified_date, act_by from dwh.asri_audit_ft where crt_dt BETWEEN TO_DATE('2023-04-01', 'YYYY-MM-DD') AND TO_DATE('2024-02-29', 'YYYY-MM-DD') and act_id = 'CD771') aa on acf.case_id = aa.case_id
left join (select dis_main_id,dis_main_name as speciality_name from dwh.asri_disease_main_cd) adm on acf.cs_dis_main_code = adm.dis_main_id
left join (select hosp_id,hosp_name,hosp_type,govt_hosp_type from dwh.asri_hospitals_dm) ah on acf.case_hosp_code = ah.hosp_id
left join (select patient_id,crt_usr, patient_ipop, crt_dt  from dwh.asri_patient_dm ) ap on acf.CASE_PATIENT_NO=ap.PATIENT_ID
left join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) aud on aud.user_id = ap.crt_usr
left join (select hosp_id , hosp_name ,  hosp_type ,  govt_hosp_type , dist_id , hosp_empnl_ref_num from asri_hospitals_dm  ) ahd on ahd.hosp_id  = acf.case_hosp_code
LEFT JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ahd.hosp_empnl_ref_num
LEFT JOIN (SELECT loc_id, loc_name as hospital_mandal, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) m_loc ON m_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ahd.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select cmb_dtl_id, cmb_dtl_name from dwh.asri_combo_cd) acc on acc.cmb_dtl_id = aud.user_role
where cs_dt_pre_auth is not null ;
          




drop materialized view  dwh.asri_onbed_pat_visit_fdbk_mv;

create materialized view dwh.asri_onbed_pat_visit_fdbk_mv as 
select 
nopv.case_id,  nopv.case_status , cmb_dtl_name as case_status_name, ac.case_patient_no, patient_name , capture_date ,
case when treatment_provided='YES' then 1 when treatment_provided='NO' then 0 end as  treatment_provided ,
case when food_provided='YES' then 1 when food_provided='NO' then 0 end as  food_provided ,
case when facilities='YES' then 1 when facilities='NO' then 0 end as  facilities ,
case when money_before_adm='YES' then 1 when money_before_adm='NO' then 0 end as  money_before_adm ,
case when money_at_adm='YES' then 1 when money_at_adm='NO' then 0 end as  money_at_adm ,
case when money_after_adm='YES' then 1 when money_after_adm='NO' then 0 end as  money_after_adm ,
patient_ration_card_no, ah.hosp_id, hosp_name, hospital_mandal, hospital_district, hospital_state,
case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hosp_type,
aud.user_id, aud.new_emp_code as NAM_LOGIN,(NVL(aud.first_name , '') || ' ' || NVL(aud.last_name, ''))AS MITHRA_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug as phno,
nopv.crt_dt as onbed_fdbck_date,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM nopv.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM nopv.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM nopv.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM nopv.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM nopv.crt_dt) + 1, 100), 'FM00') END) AS FY_onbed_fdbk,
       CASE 
        WHEN EXTRACT(MONTH FROM nopv.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM nopv.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM nopv.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS onbed_fdbk_quarter,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_id ,  case_status , patient_name , capture_date ,treatment_provided ,food_provided , facilities 
		,money_before_adm , money_at_adm , money_after_adm , crt_dt , crt_usr , onbed_image_date 
from dwh.nam_onbed_patient_visit_dm )nopv
inner join (select case_id , case_patient_no , case_hosp_code , case_regn_date  from dwh.asri_case_ft) ac on nopv.case_id = ac.case_id
left  JOIN (
    SELECT patient_id, ration_card_no AS patient_ration_card_no, sachivalayam_name,  age AS patient_age, gender AS patient_gender, mandal_code , district_code AS pat_district_code, uhidvalue
    FROM dwh.asri_patient_dm
) ap ON ap.patient_id = ac.case_patient_no
LEFT JOIN ( SELECT loc_id,  loc_name AS patient_district, loc_parnt_id FROM asri_locations_dm ) lp_d ON lp_d.loc_id = ap.pat_district_code
LEFT JOIN (  SELECT loc_id,  loc_name AS patient_state, loc_parnt_id FROM asri_locations_dm)lp_s on lp_s.loc_id=lp_d.loc_parnt_id
inner join(select hosp_id, hosp_name, dist_id, hosp_type , HOSP_EMPNL_REF_NUM  from dwh.asri_hospitals_dm where isactive_ap = 'Y') ah on ah.hosp_id = ac.case_hosp_code
left join (select HOSPINFO_ID, HOSP_BED_STRENGTH,district_code , mandal   from dwh.asri_empnl_hospinfo_dm) aeh on ah.HOSP_EMPNL_REF_NUM = aeh.HOSPINFO_ID
LEFT JOIN (SELECT loc_id, loc_name as hospital_mandal, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) m_loc ON m_loc.loc_id = aeh.mandal
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select distinct user_id ,new_emp_code  ,first_name, last_name, gender , cug, user_role, active_yn  from dwh.asri_users_dm  ) aud on nopv.CRT_USR = aud.user_id
left join (select cmb_dtl_id , cmb_dtl_name  from dwh.asri_combo_cd) cmb on cmb.cmb_dtl_id = nopv.case_status;



drop materialized view dwh.wt_empnl_hosp_active_doctors_dtls_mv;

create materialized view dwh.wt_empnl_hosp_active_doctors_dtls_mv as
select 
ah.hosp_id, hosp_name as hospital_name, hosp_disp_code as hospital_display_code, hosp_empnl_ref_num as HSIN_Number, hosp_contact_no as authorized_person_number , status as hosp_status_code, cmb.cmb_dtl_name as hosp_status, hosp_bed_strength, hosp_email as hosp_email_id,
case when ah.pan_number is not null then ah.pan_number
    when ah.pan_number is null and hp_info.pannumber is not null then hp_info.pannumber
else ' ' end as pan_card_number,
case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hospital_type, govt_hosp_type,hosp_empnl_date,hosp_addr1 as hospital_address,mandal as mandal_code, md_loc.loc_name as mandal_name,md_loc.lgd_code as mandal_lgd_code,constituency_code, city_code, city_name as hospital_city, district_code, d_loc.loc_name as district_name,d_loc.lgd_code as district_lgd_code, 
hospital_state, req_no, doctor_name, reg_num, university, experience, contactno,ad.is_activeyn as doctor_active_YN, apprv_status, cmb1.cmb_dtl_name as doctor_approval_status, 
case when ad.is_activeyn='Y' then 'Working'
when ad.is_activeyn='N' then  'Not Working' end as doctor_working_status,role,spclty_code as doctor_mapped_speciality_code, doctor_mapped_speciality_name,job_started_date, job_ended_date,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(SELECT 
hosp_id,hosp_empnl_ref_num, hosp_contact_no, hosp_name, hosp_disp_code,hosp_email, hosp_type, govt_hosp_type, hosp_empnl_date, hosp_addr1, hosp_city,  tds_exemp_status, pan_number, isactive_ap, dist_id
FROM dwh.asri_hospitals_dm where isactive_ap='Y') ah 
left JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, hosp_state, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
left join (select distinct * from
(select distinct hosp_id,req_no, NVL(dctr_name,'') as doctor_name, reg_num, university, experience, contactno,is_activeyn, apprv_status, 'Duty Doctor'  as role,lst_upd_dt as job_ended_date, crt_dt as job_started_date from dwh.asri_duty_dctrs_dm
union all
select distinct hosp_id,req_no,NVL(splst_name,'') as doctor_name, reg_num, university, experience, contactno,is_activeyn, apprv_status,
case when is_consultant='Y' then 'Consultant Specialist' when is_consultant='N' then 'In-House Specialist' else '' end as role, lst_upd_dt, crt_dt from dwh.asri_splst_dctrs_dm
union all
select distinct hosp_id,req_no,NVL(prmdc_name,'') as doctor_name, reg_num, university, experience, contactno,is_activeyn, apprv_status,'Paramedic' as role, lst_upd_dt as job_ended_date, crt_dt as job_started_date from dwh.asri_paramedics_dm
union all
select * from 
(select mu.hosp_id,null as req_no, doctor_name,regno as  reg_num, null as  university, null as experience, phone1 as  contactno,
case when eff_end_dt is null then 'Y'
else 'N' end as is_activeyn,null as apprv_status, 'Medco' as role, eff_end_dt as job_ended_date, eff_start_dt as job_started_date from 
(select distinct user_id, hosp_id, eff_start_dt,eff_end_dt from dwh.asri_nwh_users_dm ) mu
inner join (select distinct user_id, login_name ,(NVL(first_name,'') + ' ' +NVL(last_name,'')) as doctor_name,active_yn, user_role, crt_dt, lst_upd_dt, phone1,regno  from dwh.asri_users_dm where active_yn='Y'  and user_role='CD9') au on au.user_id = mu.user_id
)
)) ad on ad.hosp_id = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name,lgd_code from dwh.asri_locations_dm ) md_loc ON md_loc.loc_id = hp_info.mandal
LEFT JOIN (SELECT loc_id, loc_name, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select city_id , city_name from dwh.asri_major_city_dm )amcd on amcd.city_id  = hp_info.city_code
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = hp_info.status
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb1 ON cmb1.cmb_dtl_id = ad.apprv_status
left join (select distinct reg_num as ds_regnum, spclty_code from dwh.asri_doctor_splty_dm where is_activeyn='Y') sds on sds.ds_regnum = ad.reg_num
LEFT JOIN ( SELECT dis_main_id, dis_main_name as doctor_mapped_speciality_name  FROM dwh.asri_disease_main_cd ) dm_p ON dm_p.dis_main_id = sds.spclty_code;






drop materialized view dwh.wt_hospdist_vs_patientdist_mv;

create materialized view dwh.wt_hospdist_vs_patientdist_mv as 
SELECT
    ac.case_id,
    case_hosp_code,
    ah.hosp_name,
    case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hosp_type,
    govt_hosp_type as sub_hosp_type,
    hosp_dist,
    hosp_state,
    cs_dis_main_code AS case_disease_code,
    case_dis_name,
    cs_apprv_rej_dt AS case_preauth_appr_dt,
    surgery_code AS case_surg_code,
    surgery_desc,
    cs_surg_dt as surgery_date,
    cs_dis_dt as discharge_date,
    clm_sub_dt as claim_submitted_date,
    case_status,
    cmb_dtl_name AS status_name,
    case when case_status IN ('CD125','CD145','CD1253','CD1252','CD1251','CD1255') then 'APPROVED'
         when case_status IN ('CD118','CD1181','CD90','CDFD354','CD384','CD1195','CD15741','CD1190','CD121','CD1182','CD0489','CD1191','CD1186','CD1197','CD1194','CD1185','CDSC354','CD354','CD1192','CD1193','CDP1354','CD1451','CD316','CD15745','CD314','CD2027','CD2024') then 'In Process'
         when case_status IN ('CD1187','CD382','CD146','CD124','CD9021','CD74') then 'Rejected'
         when case_status IN ('CD145_1','CD384_1','CD0000','CD1254','CD1195','AP7741661','CD90CD90','CD15734','CD15735') then 'NULL'
         	else '' end as status_type,
    pck_appv_amt as package_amount,
    cs_cl_amount as claim_amount,
    --patient_name
    patient_ration_card_no,
    uhidvalue,
    sachivalayam_name,
    patient_mandal,
    patient_district,
   patient_state,
    patient_age,
    case when patient_age between 0 and 5 then '0-5 years'
			when patient_age between 6 and 10 then '6-10 years'
			when patient_age between 11 and 15 then '11-15 years'
			when patient_age between 16 and 20 then '16-20 years'
			when patient_age between 21 and 25 then '21-25 years'
			when patient_age between 26 and 30 then '26-30 years'
			when patient_age between 31 and 35 then '31-35 years'
			when patient_age between 36 and 40 then '36-40 years'
			when patient_age between 41 and 45 then '41-45 years'
			when patient_age between 46 and 50 then '46-50 years'
			when patient_age between 51 and 55 then '51-55 years'
			when patient_age between 56 and 60 then '56-60 years'
			when patient_age between 61 and 65 then '61-65 years'
			when patient_age between 66 and 70 then '66-70 years'
			when patient_age between 71 and 75 then '71-75 years'
			when patient_age between 76 and 80 then '76-80 years'
			when patient_age between 81 and 85 then '81-85 years'
			when patient_age between 86 and 90 then '86-90 years'
			when patient_age between 91 and 95 then '91-95 years'
			when patient_age between 96 and 100 then '96-100 years'
			when patient_age>100 then 'above 100 years'
			else '' end as patient_age_frequency,
    case when patient_gender in ('M','1') then 'Male'  when patient_gender in ('F','2') then 'Female' else patient_gender  end as patient_gender
FROM 
(
 SELECT case_id, case_hosp_code, cs_apprv_rej_dt, cs_surg_dt, cs_dis_dt, clm_sub_dt, case_status, pck_appv_amt, cs_cl_amount,case_patient_no,cs_dis_main_code 
  FROM dwh.asri_case_ft WHERE CS_APPRV_REJ_DT>TO_DATE('2023-04-01','YYYY-MM-DD')) ac
INNER JOIN (
    SELECT patient_id, ration_card_no AS patient_ration_card_no, sachivalayam_name,  age AS patient_age, gender AS patient_gender, mandal_code , district_code AS pat_district_code, uhidvalue
    FROM dwh.asri_patient_dm
) ap ON ap.patient_id = ac.case_patient_no
INNER JOIN (
    SELECT case_id, dis_main_code, surgery_code FROM dwh.asri_case_surgery_dm
) acs ON acs.case_id = ac.case_id
INNER JOIN (
    SELECT hosp_id, hosp_name, dist_id AS hosp_district_id, hosp_type, govt_hosp_type FROM asri_hospitals_dm
) ah ON ah.hosp_id = ac.case_hosp_code
INNER JOIN (
    SELECT STATUS_ID, GROUP_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID = 'CD17'
) acsg ON acsg.STATUS_ID = ac.CASE_STATUS
LEFT JOIN (
    SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd
) cmb ON cmb.cmb_dtl_id = ac.case_status
LEFT JOIN (
    SELECT loc_id, loc_name AS patient_mandal FROM asri_locations_dm
) lp_m ON lp_m.loc_id = ap.mandal_code
LEFT JOIN (
    SELECT loc_id,  loc_name AS patient_district, loc_parnt_id FROM asri_locations_dm
) lp_d ON lp_d.loc_id = ap.pat_district_code
LEFT JOIN (
    SELECT loc_id,  loc_name AS patient_state, loc_parnt_id FROM asri_locations_dm
)lp_s on lp_s.loc_id=lp_d.loc_parnt_id
LEFT JOIN (
    SELECT loc_id, loc_parnt_id, loc_name AS hosp_dist
    FROM asri_locations_dm
) lh_d ON lh_d.loc_id = ah.hosp_district_id
LEFT JOIN (
    SELECT loc_id, loc_parnt_id, loc_name  as hosp_state
    FROM asri_locations_dm
)lh_s on lh_s.loc_id = lh_d.loc_parnt_id
LEFT JOIN (
    SELECT dis_main_id, dis_main_name AS case_dis_name FROM asri_disease_main_cd
) dm ON dm.dis_main_id = ac.cs_dis_main_code
LEFT JOIN (
select surgery_id,surgery_desc 
from (select surgery_id,surgery_desc,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asg ON asg.surgery_id = acs.surgery_code;




drop materialized view dwh.foss_aasra_people_benefit_overview_mv;

create materialized view dwh.foss_aasra_people_benefit_overview_mv as 
SELECT DISTINCT
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_apprv_rej_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS fyYear,
 case when extract(MONTH FROM cs_apprv_rej_dt)<=9 then '0'||extract(MONTH FROM cs_apprv_rej_dt)|| ' ( '||TRIM (both '' from TO_CHAR(cs_apprv_rej_dt, 'Month'))||' )'
       when extract(MONTH FROM cs_apprv_rej_dt)>=10 then  extract(MONTH FROM cs_apprv_rej_dt)::text|| ' ( '||TRIM (both '' from TO_CHAR(cs_apprv_rej_dt, 'Month'))||' )' end as apprv_month, EXTRACT(YEAR FROM cs_apprv_rej_dt)::int as apprv_year,
        TO_CHAR(cs_apprv_rej_dt, 'Month YY') as fy_month,
      ac.case_id, ac.CASE_PATIENT_NO,cs_apprv_rej_dt, pck_appv_amt as utilized_amount,ac.cs_dis_main_code as speciality_code, dsm.dis_main_name as speciality_name, acs.surgery_code,su.surgery_desc as procedure_name, COALESCE(su.postops_amt,0) as postop_amount,
      ap.district_code, district_name as patient_district,ap.mandal_code,  mandal_name as patient_mandal, ap.village_code, village_name as patient_village,uhidvalue, FAMILY_CARD_NO, RATION_CARD_NO
FROM (SELECT CASE_ID,CASE_PATIENT_NO,cs_apprv_rej_dt, pck_appv_amt,cs_dis_main_code
	  FROM dwh.asri_case_ft
	  WHERE TRUNC(cs_apprv_rej_dt) >= TO_DATE('2023-04-01','YYYY-MM-DD')) ac 
inner join (select patient_id
			from dwh.asri_patac_audit_dm 
			where act_id = 'CD1002') pa on ac.CASE_PATIENT_NO = pa.patient_id
left join  (select case_id,surgery_code, dis_main_code 
		    from dwh.asri_case_surgery_dm) acs on ac.case_id = acs.case_id
LEFT JOIN (SELECT PATIENT_ID,DISTRICT_CODE,MANDAL_CODE,RATION_CARD_NO, Village_code,
		  CASE 
    	  WHEN POSITION('/' IN RATION_CARD_NO) > 0 
    	  THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
          ELSE RATION_CARD_NO
          end AS FAMILY_CARD_NO, uhidvalue
          FROM dwh.asri_patient_dm) ap ON ap.PATIENT_ID = ac.CASE_PATIENT_NO
left join (select surgery_id,postops_amt, surgery_desc	
		  from
		  (select surgery_id,surgery_desc,postops_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
		  from dwh.asri_surgery_dm) 
          where ranking = 1) su on su.surgery_id = acs.surgery_code
left join (select dis_main_id, dis_main_name from dwh.asri_disease_main_cd ) dsm on dsm.dis_main_id = ac.cs_dis_main_code
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS MANDAL_NAME
           FROM dwh.asri_locations_dm) al ON al.MANDAL_CODE = ap.MANDAL_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS DISTRICT_NAME
           FROM dwh.asri_locations_dm) alp ON alp.LOC_ID = ap.DISTRICT_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS VILLAGE_NAME	
           FROM dwh.asri_locations_dm) alv ON alv.LOC_ID = ap.village_code;
          
          

          

drop materialized view dwh.asri_aasra_paid_overlap_mv;

create materialized view dwh.asri_aasra_paid_overlap_mv as 
select   
*, discharged_date + rest_days  * INTERVAL '1 DAY' as aasra_end_date,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select    
case_id ,LEAD(case_id) over(partition by patient_ration_card_no order by discharged_date ) as next_case_id,speciality_name, LEAD(speciality_name) over(partition by patient_ration_card_no order by discharged_date ) as next_case_speciality_name, procedure_name,  LEAD(procedure_name) over(partition by patient_ration_card_no order by discharged_date ) as next_case_procedure_name,
preauth_approved_date, LEAD(preauth_approved_date) over(partition by patient_ration_card_no order by discharged_date ) as next_case_preauth_approved_date, 
patient_id,LEAD(patient_id) over(partition by patient_ration_card_no order by discharged_date ) as next_case_patient_id,  patient_name,patient_district, patient_state,hosp_name,LEAD(hosp_name) over(partition by patient_ration_card_no order by discharged_date ) as next_case_hosp_name,
hosp_type,LEAD(hosp_type) over(partition by patient_ration_card_no order by discharged_date ) as next_case_hosp_type, govt_hosp_type,  LEAD(govt_hosp_type) over(partition by patient_ration_card_no order by discharged_date ) as next_case_govt_hosp_type, hospital_district, LEAD(hospital_district) over(partition by patient_ration_card_no order by discharged_date ) as next_case_hospital_district, hospital_state,  LEAD(hospital_state) over(partition by patient_ration_card_no order by discharged_date ) as next_case_hospital_state,
patient_ration_card_no  , case_aasra_amount , LEAD(case_aasra_amount) over(partition by patient_ration_card_no order by discharged_date ) as next_case_case_aasra_amount,
aasra_paid_date ,FY_aasra_paid,
discharged_date, LEAD(discharged_date) over(partition by patient_ration_card_no order by discharged_date ) as next_case_discharged_date,
case when case_aasra_amount=5000 then 30  when case_aasra_amount=10000 then 60  when case_aasra_amount=15000 then 90  when case_aasra_amount=30000 then 180  when case_aasra_amount=60000 then 360 else ROUND((case_aasra_amount/225),0) end as rest_days,
LEAD(aasra_paid_date) over(partition by patient_ration_card_no order by discharged_date ) as next_aasra_paid_date
from 
(select  
*,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aasra_paid_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date) - 1,	 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date) + 1, 100), 'FM00') END) AS FY_aasra_paid,
case when rwn=1 then preauth_aprv_amount else 0 end as case_preauth_approved_amount,
case when rank_2=1 then postops_amt else 0 end as case_aasra_amount,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select 
ac.case_id,ac.cs_dis_main_code as speciality_code , dis_main_name as speciality_name,asd.surgery_id as procedure_code,asd.surgery_desc as procedure_name, asd.surgery_amt as procedure_defined_amount, postops_amt, ac.pck_appv_amt as preauth_aprv_amount,ap.patient_id,patient_name,  patient_ration_card_no,uhidvalue,district_code as patient_district_id,patient_district,patient_state,
hosp_id, hosp_name,hosp_type,govt_hosp_type,dist_id as hospital_dist_id,hospital_district, hospital_state, 
1 as is_preauth_approved,
case when is_perdm='Y'  and postops_amt is not null then 1 else 0 end as is_aasra_eligible,
case when  pat.act_id='CD1002' then 1 else 0 end as  is_aasra_paid,
case when  pat.act_id='CD1002' then pat.crt_dt else null end as  aasra_paid_date,
ac.case_regn_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) + 1, 100), 'FM00') END) AS FY_case_registered,
 ac.cs_apprv_rej_dt as preauth_approved_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_apprv_rej_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS FY_preauth_approved,
ac.cs_dis_dt as discharged_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_dis_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt) + 1, 100), 'FM00') END) AS FY_discharged,
ROW_NUMBER() OVER (PARTITION BY ac.case_id) as rwn,
ROW_NUMBER() OVER (PARTITION BY ac.case_id order by NVL(postops_amt,0) DESC ) as rank_2
from 
(SELECT CASE_ID,CASE_PATIENT_NO,case_hosp_code , cs_apprv_rej_dt,case_regn_date,case_status, pck_appv_amt,cs_dis_main_code,cs_dis_dt FROM dwh.asri_case_ft where DATE(case_regn_date)>='2022-04-01') ac 
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
left  JOIN (SELECT patient_id,NVL(first_name,'')||' '||NVL(middle_name,'')||' '||NVL(last_name,'') as patient_name, ration_card_no AS patient_ration_card_no, sachivalayam_name,  age AS patient_age, case when gender='M' then 'Male' when gender='F' then 'Female' end  AS patient_gender, mandal_code , district_code , uhidvalue
                FROM dwh.asri_patient_dm
) ap ON ap.patient_id = ac.case_patient_no
inner join (
	select distinct patient_id , act_id , act_by, crt_dt 
		from 
		(select patient_id , act_id , act_order,act_by, crt_dt , RANK() OVER(partition by patient_id,act_id order by crt_dt desc ) as ranking
			from dwh.asri_patac_audit_dm 
		)
		where ranking=1 and act_id='CD1002' 
)pat on pat.patient_id = ap.patient_id
left join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
left join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm, surgery_amt
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code
left join (select hosp_id, hosp_name,dist_id,    case when hosp_type='C' then 'Corporate'
				when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type  from dwh.asri_hospitals_dm ) ah on ac.case_hosp_code = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
LEFT JOIN (SELECT loc_id, loc_name as patient_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) pd_loc on pd_loc.loc_id = ap.district_code
LEFT JOIN (SELECT loc_id, loc_name as patient_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) ps_loc ON ps_loc.loc_id = pd_loc.loc_parnt_id
left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = ac.cs_dis_main_code
)
)
where case_aasra_amount<>0
)
where next_case_id is not null and aasra_end_date>=next_aasra_paid_date;





drop materialized view dwh.rep_hosp_dist_rejections_mv;

create materialized view dwh.rep_hosp_dist_rejections_mv as 
select distinct
ac.case_id, ah.hosp_name,hosp_disp_code as hospital_code, d_loc.loc_name as hosp_district ,s_loc.loc_name as hosp_state , case_status, cmb_dtl_name as status_name,status_date,claim_submitted_date,surgery_date, discharge_date
,ap.PATIENT_ID, ap.RATION_CARD_NO,ap.uhidvalue , fdk_case_id,
case when case_status = 'CDFD354' and dis_patient_district_final is not null then dis_patient_district_final
 else DISTRICT_NAME end as patient_district_name,
case when case_status = 'CDFD354' and dis_patient_district_final is not null then PATIENT_STATE_NAME_FINAL
else PATIENT_STATE_NAME end as patient_state, 
fm.householdcardno, FAMILY_CARD_NO, pm_jay
from 
(select case_id , case_no , case_hosp_code , case_patient_no , case_status , lst_upd_dt as status_date, clm_sub_dt as claim_submitted_date, cs_cl_amount as claim_amount, cs_dis_dt as discharge_date, cs_surg_dt as surgery_date from dwh.asri_case_ft 
where case_status in ('CDFD354','CDSC354','CD0489') ) ac 
inner join (SELECT hosp_id, hosp_name,dist_id, hosp_disp_code FROM dwh.asri_hospitals_dm) ah on ah.hosp_id = ac.case_hosp_code
left join (select case_id as fdk_case_id,dis_patient_district_final, dis_hosp_dist_name from dwh.wt_anm_feedback_mv) fdk on fdk.fdk_case_id = ac.case_id
LEFT JOIN (SELECT loc_id, loc_name,lgd_code, loc_parnt_id  from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name,lgd_code, loc_parnt_id  from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh.asri_combo_cd ) cmb ON cmb.cmb_dtl_id = ac.case_status
LEFT JOIN (SELECT PATIENT_ID,DISTRICT_CODE,MANDAL_CODE, village_code, RATION_CARD_NO,
		  CASE 
    	  WHEN POSITION('/' IN RATION_CARD_NO) > 0 
    	  THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
          ELSE RATION_CARD_NO
          end AS FAMILY_CARD_NO,
          uhidvalue 
          FROM dwh.asri_patient_dm  ) ap ON ap.PATIENT_ID = ac.CASE_PATIENT_NO
left join (select householdcardno, membername,uid_no,relation_name, mobile_no, pm_jay  from dwh.asri_family_cs_ap_dm) fm on fm.householdcardno = ap.FAMILY_CARD_NO
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS MANDAL_NAME
           FROM dwh.asri_locations_dm) al ON al.MANDAL_CODE = ap.MANDAL_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS DISTRICT_NAME, loc_parnt_id 
           FROM dwh.asri_locations_dm) alp ON alp.LOC_ID = ap.DISTRICT_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_STATE_NAME, loc_parnt_id 
           FROM dwh.asri_locations_dm) als on als.loc_id = alp.loc_parnt_id  
LEFT JOIN (SELECT LOC_ID,LOC_NAME , loc_parnt_id 
           FROM dwh.asri_locations_dm) ald_d on ald_d.loc_name = fdk.dis_patient_district_final
 LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_STATE_NAME_FINAL, loc_parnt_id 
           FROM dwh.asri_locations_dm) ald_s on ald_s.loc_id = ald_d.loc_parnt_id
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS VILLAGE_NAME	
           FROM dwh.asri_locations_dm) alv ON alv.LOC_ID = ap.village_code; 
          
          
          
drop materialized view dwh.ration_card_pat_proc_utilization_mv;

create materialized view dwh.ration_card_pat_proc_utilization_mv as
select  
out_all.*
from 
(select ac.case_id,case_patient_no,case_status,case_status_name, cs_dis_main_code as speciality_code,speciality_name, acs.surgery_code as procedure_code, surgery_desc as procedure_name,
		ration_card_no,family_card_no,age,gender,patient_district,patient_state,
		case_hosp_code,hosp_name,hosp_type,govt_hosp_type,hosp_district,hosp_state,
		preauth_approved_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM preauth_approved_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date) + 1, 100), 'FM00') END) AS fy_preauth_aprv
from 
(select case_id,case_hosp_code,cs_dis_main_code,case_status,case_patient_no,
      cs_apprv_rej_dt as preauth_approved_date
	  from dwh.asri_case_ft
	  where trunc(cs_apprv_rej_dt)>='2022-04-01') ac
inner join (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID = 'CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
inner  join (select patient_id,ration_card_no,district_code,mandal_code,age,gender,
		          case WHEN POSITION('/' IN RATION_CARD_NO) > 0 THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
                       ELSE RATION_CARD_NO
                       END AS FAMILY_CARD_NO
		   from dwh.asri_patient_dm ) ap on ac.case_patient_no = ap.patient_id
left join (select dis_main_id,dis_main_name as speciality_name
		   from dwh.asri_disease_main_cd) dm on ac.cs_dis_main_code = dm.dis_main_id
left join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
left  join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code
left join (select cmb_dtl_id,cmb_dtl_name as case_status_name
		   from dwh.asri_combo_cd) acb on ac.case_status = acb.cmb_dtl_id
left join (select hosp_id,hosp_name,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type,dist_id,hosp_vil_cd
		   from dwh.asri_hospitals_dm) ah on ac.case_hosp_code = ah.hosp_id
left join (select loc_id,loc_name as hosp_district,loc_parnt_id
		   from dwh.asri_locations_dm) al on ah.dist_id = al.loc_id
left join (select loc_id,loc_name as hosp_state,loc_parnt_id
		   from dwh.asri_locations_dm) alhs on al.loc_parnt_id = alhs.loc_id		   
left JOIN (SELECT loc_id, loc_name as patient_district,loc_parnt_id FROM dwh.asri_locations_dm ) ald ON ald.loc_id = ap.district_code
left JOIN (SELECT loc_id, loc_name as patient_state,loc_parnt_id FROM dwh.asri_locations_dm ) als on als.loc_id = ald.loc_parnt_id
) out_all 
inner join 
(select *
from 
(select 
ration_card_no, count(distinct surgery_code) as cnt
from 
(select case_id,case_hosp_code,cs_dis_main_code,case_status,case_patient_no,
      cs_apprv_rej_dt as preauth_approved_date
	  from dwh.asri_case_ft
	  where trunc(cs_apprv_rej_dt)>='2022-04-01') ac
inner join (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID = 'CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
inner  join (select patient_id,ration_card_no,district_code,mandal_code,age,gender,
		          case WHEN POSITION('/' IN RATION_CARD_NO) > 0 THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
                       ELSE RATION_CARD_NO
                       END AS FAMILY_CARD_NO
		   from dwh.asri_patient_dm ) ap on ac.case_patient_no = ap.patient_id
left join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
inner join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code
group by ration_card_no
)
where cnt>1
) cnt_r on cnt_r.ration_card_no = out_all.ration_card_no;





drop materialized view dwh.foss_dist_wise_anm_feedback_report_mv;

create materialized view dwh.foss_dist_wise_anm_feedback_report_mv as
SELECT *,
case when is_pat_tranferred='1' then trans_district_name
else dis_patient_district end as dis_patient_district_final,
case when is_pat_tranferred='1' then trans_sachivalaym_name
else dis_sachivalayam_name end as dis_patient_sachivalayam_final
FROM 
(SELECT 
ds.case_id, ds.patient_name, ds.mandal_name as dis_hosp_mandal_name, ds.hosp_dist_name as dis_hosp_dist_name, ds.hosp_id as dis_hosp_id, ds.hosp_name as dis_hosp_name,eh.hosp_type, eh.govt_hosp_type, ds.patient_id as discharged_patient_id,ds.patient_name as discharged_patient_name,
ds.resident_id as discharged_pat_resident_id, ds.status as discharge_status, ds.sachivalayam_name as dis_sachivalayam_name, ds.patient_mobile as dis_patient_mobile,
ds.gender as dis_patient_gender, ds.patient_district as dis_patient_district,
ds.aarogya_mithra_name as dis_aarogya_mithra_name, ds.cs_dis_dt as pat_dis_date,
 CASE WHEN TO_DATE(ds.cs_dis_dt, 'YYYY-MM-DD') <='2023-11-30' THEN 'Until Nov 23'
 ELSE TO_CHAR(ds.cs_dis_dt, 'Month YY') END as dis_month_year,
ds.data_sent_date as dis_data_sent_date,
ds.anm_push_date as dis_case_pushed_date, -- is_case_pushed
CASE 
        WHEN ds.anm_push_date IS NOT NULL AND ds.anm_push_date::timestamp IS NOT NULL THEN '1'
        ELSE '0'
    END AS is_case_pushed,
case when tr.case_id is null then '0'
     else '1' end  as is_pat_tranferred,
(SELECT loc_name from dwh.asri_locations_dm  where loc_id = tr.district_id) as trans_district_name, 
(SELECT loc_name from dwh.asri_locations_dm  where loc_id = tr.mandal_id) as trans_mandal_name,
tr.sachivalayam_id as trans_sachivalayam_id, al.loc_name as trans_sachivalaym_name, tr.called_the_patient , tr.call_anm_secretariat_confrmd,
case when fd.case_id is null then '0'
     else '1' end  as is_feedback_submitted,
free_services as fdbk_free_services,hospital_service_satisfaction as fdbk_hospital_service_satisfaction,mithra_service_satisfaction as fdbk_mithra_service_satisfaction,
communication_amt_coll as fdbk_communication_amt_coll,lab_diagnostics_amt_coll as fdbk_lab_diagnostics_amt_coll,medicine_amt_coll as fdbk_medicine_amt_coll,
operations_amt_coll as fdbk_operations_amt_coll,aarogyamithra_amt_coll as fdbk_aarogyamithra_amt_coll,other_hosp_staff_amt_coll as fdbk_other_hosp_staff_amt_coll,
total_amt_coll as fdbk_total_amt_coll, doctor_services as fdbk_doctor_services, nurse_services as fdbk_nurse_services, time_food_services as fdbk_time_food_services,
fd.crt_dt as feedback_submitted_date, 
CASE when ds.anm_push_date is null then  10 
	else ABS(EXTRACT(EPOCH FROM (ds.anm_push_date - ds.cs_dis_dt)) / (3600 * 24))  END AS diff_dis_push
FROM dwh.asri_discharge_case_data_dm as ds 
LEFT JOIN dwh.anm_transfer_case_dm as tr ON tr.case_id = ds.case_id 
LEFT JOIN dwh.asri_patient_dis_feedback_ref_dm as fd ON fd.case_id = ds.case_id
LEFT JOIN (SELECT loc_id, loc_name FROM dwh.asri_locations_dm ) al ON al.loc_id = tr.sachivalayam_id
left join (select hosp_id,hosp_type, govt_hosp_type  from dwh.asri_hospitals_dm )eh on eh.hosp_id = ds.hosp_id);




drop materialized view dwh.asri_mithra_service_fdbk_mv;

create materialized view dwh.asri_mithra_service_fdbk_mv as 
select 
ac.case_id,patient_id, patient_ration_card_no,uhidvalue,patient_gender,patient_age, district_code as patient_district_id,patient_district,patient_state,ac.case_regn_date, ac.cs_dis_dt as discharged_date,apdf.crt_dt as feedback_submitted_date,  hosp_id, hosp_name,hosp_type,govt_hosp_type,dist_id as hospital_dist_id,hospital_district, hospital_state,
aud.user_id, aud.new_emp_code , (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS MITHRA_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug as phno,mithra_service_satisfaction,
case when mithra_service_satisfaction = 'Y' then  1 when mithra_service_satisfaction = 'N' then 0 end as is_mithra_satisfied,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FY_case_registered,
      'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_dis_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt) + 1, 100), 'FM00') END) AS FY_patient_discharged,
       'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM apdf.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM apdf.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM apdf.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM apdf.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM apdf.crt_dt) + 1, 100), 'FM00') END) AS FY_feedback_submitted
from    
(select case_id , case_patient_no , case_hosp_code , cs_dis_dt, case_regn_date, crt_usr  from dwh.asri_case_ft) ac
inner join (select distinct case_id ,mithra_service_satisfaction , crt_usr, crt_dt  from dwh.asri_patient_dis_feedback_ref_dm) apdf on apdf.case_id = ac.case_id
left join (select hosp_id, hosp_name,dist_id,    case when hosp_type='C' then 'Corporate'
when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type  from dwh.asri_hospitals_dm ) ah on ac.case_hosp_code = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) aud on aud.user_id = ac.crt_usr
left  JOIN (SELECT patient_id, ration_card_no AS patient_ration_card_no, sachivalayam_name,  age AS patient_age, case when gender='M' then 'Male' when gender='F' then 'Female' end  AS patient_gender, mandal_code , district_code , uhidvalue
                FROM dwh.asri_patient_dm
) ap ON ap.patient_id = ac.case_patient_no
LEFT JOIN (SELECT loc_id, loc_name as patient_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) pd_loc on pd_loc.loc_id = ap.district_code
LEFT JOIN (SELECT loc_id, loc_name as patient_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) ps_loc ON ps_loc.loc_id = pd_loc.loc_parnt_id;





drop materialized view dwh.asri_flagged_cases_mv;

create materialized view dwh.asri_flagged_cases_mv as
select
	afdd.flg_id ,afdd.case_id , acf.case_hosp_code , ahd.Old_Dist_name, ahd.hosp_name , ahd.hosp_type, 	ahd.govt_hosp_type , afdd.case_status , acc.cmb_dtl_name as case_status_Name , 	afdd.flg_type , acc2.cmb_dtl_name as flg_name,  afdd.flg_status , acc3.cmb_dtl_name as flg_status_Name,
	afdd.crt_usr , aud.login_name as Crt_login_Name, aud.first_name as crt_User_Name, aud.user_role as crt_user_role_id, acc4.cmb_dtl_name as crt_user_role, afdd.crt_dt , afdd.lst_upd_usr , aud1.login_name as lst_upd_user_login_Name, aud1.first_name as lst_upd_user_Name, aud1.user_role as lst_user_role_id,
	acc5.cmb_dtl_name as lst_upd_usr_role, afdd.lst_upd_dt , afdd.active_yn, agsd.amount_of_moneycollected,  agsd.final_collected_amt 
from
	asri_flgging_dtls_dm afdd
left join asri_combo_cd acc on
	acc.cmb_dtl_id = afdd.case_status
left join asri_combo_cd acc2 on
	acc2.cmb_dtl_id = afdd.flg_type
left join asri_combo_cd acc3 on
	acc3.cmb_dtl_id = afdd.flg_status
left join asri_grievance_services_dm agsd on 
	agsd.case_id = afdd.case_id 
left join asri_users_dm aud on
	aud.user_id = afdd.crt_usr 
left join asri_users_dm aud1 on
	aud1.user_id = afdd.lst_upd_usr 
left join asri_combo_cd acc4 on 
acc4.cmb_dtl_id = aud.user_role
left join asri_combo_cd acc5 on 
acc5.cmb_dtl_id = aud1.user_role
left join asri_case_ft acf  on afdd.case_id = acf.case_id 
left join (
	select
		hosp_id , hosp_name , hosp_type, govt_hosp_type , application_type , hosp_empnl_date , 	hosp_estab_yr, dist_id , loc_name as Old_Dist_name from asri_hospitals_dm
left join (
		select
			loc_id ,loc_name
		from asri_locations_dm ald where loc_hdr_id = 'LH6' ) ald on
		ald.loc_id = dist_id
	where
		isactive_ap = 'Y')ahd on
	ahd.hosp_id = acf.case_hosp_code ;





drop materialized view dwh.asri_chemo_days_diff_mv;

create materialized view dwh.asri_chemo_days_diff_mv as 
select *
from 
(select 
case_id, LAG(case_id) over(partition by patient_card_no order by preauth_approved_date ) as prev_case_id,
patient_card_no,surgery_code,surgery_desc,postops_amt as aasra_amount,rest_days,preauth_approved_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM preauth_approved_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date) + 1, 100), 'FM00') END) AS fy_preauth_aprv,
LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ) as prev_preauth_date, ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),0) as days_diff ,
 CASE
        WHEN ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) <= 5 THEN '0-5 days'
        WHEN ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) BETWEEN 6 AND 10 THEN '06-10 days'
        WHEN ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) BETWEEN 11 AND 15 THEN '11-15 days'
        WHEN ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) BETWEEN 16 AND 20 THEN '16-20 days'
        WHEN ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) BETWEEN 21 AND 25 THEN '21-25 days'
        WHEN ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) BETWEEN 26 AND 30 THEN '26-30 days'
        when ROUND(ABS(DATEDIFF(hour,LAG(preauth_approved_date) over(partition by patient_card_no order by preauth_approved_date ),preauth_approved_date)/24.0),1) is null then NULL
   		else '>30 days' end as days_diff_frequency,
is_aasra_paid,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select  
ac.case_id,ap.RATION_CARD_NO as patient_card_no, acs.surgery_code, surgery_desc, postops_amt, rest_days, ac.cs_apprv_rej_dt as preauth_approved_date
,case when pat.patient_id is not null then 1 else 0 end as is_aasra_paid,surgery_sk,
RANK() over(partition by ac.case_id order by NVL(postops_amt,0) desc,surgery_sk  desc ) as ranking
from 
(select case_id , case_no, case_hosp_code , case_patient_no , case_status , case_regn_date, cs_apprv_rej_dt 
from dwh.asri_case_ft where DATE(cs_apprv_rej_dt) >='2022-04-01' ) ac
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
LEFT JOIN (SELECT PATIENT_ID,NVL(first_name,'')||' '||NVL(middle_name,'')||' '||NVL(last_name,'') as patient_name, age, RATION_CARD_NO,
		  CASE WHEN POSITION('/' IN RATION_CARD_NO) > 0  THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1) ELSE RATION_CARD_NO  end AS FAMILY_CARD_NO, uhidvalue , reg_hosp_id , reg_hosp_date, NVL(addr1,' ') || NVL(addr2,' ') || NVL(addr3,' ') AS PATIENT_ADDRESS
          FROM dwh.asri_patient_dm
   		) ap on ap.patient_id = ac.case_patient_no	
left join (select distinct case_id, surgery_code, crt_dt  from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
inner join ( select distinct  surgery_id,surgery_desc ,surgery_sk , postops_amt,is_perdm, surgery_amt,rest_days
from (select surgery_id,surgery_desc ,surgery_sk, postops_amt,is_perdm,surgery_amt,rest_days, rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm where surgery_id in ('S12.16.2.1','S12.17.1','S12.17.3','S12.18.1','S12.19.1','S12.20.1','S12.21.1','S12.22.1','S12.23.1','S12.24.1','S12.25.1','S12.26.1','S12.26.2','S12.26.3','S12.27.1.1','S12.27.1.2','S12.27.1.3','S12.28.1','S12.30.1','S12.31.1',
			'S12.32.1','S12.32.2','S12.1.1','S12.1.2','S12.1.3','S12.1.4','S12.1.5','S12.1.6','S12.1.7','S12.2.1','S12.3.1','S12.4.1','S12.5.1','S12.6.1','S12.7.1.2','S12.7.2.1','S12.8.1','S12.10.2','S12.11.1','S12.12.1','S12.13.1','S12.14.2','S12.15.1','S12.16.1.1','S12.7.1.1',
			'S12.10.1','S12.17.2','S12.14.1','S12.11.5','S12.1.8','S12.11.2','S12.11.3','S12.11.4','S12.13.2','S12.13.3','S12.13.4','S12.13.5','S12.13.6','S12.14.3','S12.14.4','S12.14.5','S12.16.2.2','S12.19.2','S12.21.2','S12.31.2','S12.31.3','S12.31.4','S12.33.1','S12.33.2',
			'S12.33.3','S12.33.4','S12.33.5','S12.34.1','S12.35.1','S12.35.2','S12.35.3','S12.35.4','S12.27.1.5','S12.35.14','S12.35.5','S12.35.6','S12.35.7','S12.1.9','S12.1.10','S12.19.3','S12.27.1.1.1','S12.27.1.1.2','S12.27.1.2.1','S12.27.1.2.2','S12.27.1.4.1','S12.27.1.4.2',
			'S12.27.1.4.3','S12.35.72','S12.13.7','MO053A','S12.9.3','SO069A','MO061C') )
where ranking = 1 
) asd on asd.surgery_id = acs.surgery_code
inner join (
	select distinct patient_id , act_id , act_by, crt_dt 
		from 
		(select patient_id , act_id , act_order,act_by, crt_dt , RANK() OVER(partition by patient_id,act_id order by crt_dt desc ) as ranking
			from dwh.asri_patac_audit_dm 
		)
		where ranking=1 and act_id='CD1002' 
)pat on pat.patient_id = ap.patient_id
) where ranking=1
)where prev_case_id is not null ;





drop materialized view dwh.ration_card_pat_spec_utilization_mv;

create materialized view dwh.ration_card_pat_spec_utilization_mv as
select  
out_all.*
from 
(select ac.case_id,case_patient_no,case_status,case_status_name, cs_dis_main_code as speciality_code,speciality_name, acs.surgery_code as procedure_code, surgery_desc as procedure_name,
		ration_card_no,family_card_no,age,gender,patient_district,patient_state,
		case_hosp_code,hosp_name,hosp_type,govt_hosp_type,hosp_district,hosp_state,
		preauth_approved_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM preauth_approved_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM preauth_approved_date) + 1, 100), 'FM00') END) AS fy_preauth_aprv
from 
(select case_id,case_hosp_code,cs_dis_main_code,case_status,case_patient_no,
      cs_apprv_rej_dt as preauth_approved_date
	  from dwh.asri_case_ft
	  where trunc(cs_apprv_rej_dt)>='2022-04-01') ac
inner join (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID = 'CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
inner  join (select patient_id,ration_card_no,district_code,mandal_code,age,gender,
		          case WHEN POSITION('/' IN RATION_CARD_NO) > 0 THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
                       ELSE RATION_CARD_NO
                       END AS FAMILY_CARD_NO
		   from dwh.asri_patient_dm ) ap on ac.case_patient_no = ap.patient_id
left join (select dis_main_id,dis_main_name as speciality_name
		   from dwh.asri_disease_main_cd) dm on ac.cs_dis_main_code = dm.dis_main_id
left join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
left  join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code
left join (select cmb_dtl_id,cmb_dtl_name as case_status_name
		   from dwh.asri_combo_cd) acb on ac.case_status = acb.cmb_dtl_id
left join (select hosp_id,hosp_name,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type,dist_id,hosp_vil_cd
		   from dwh.asri_hospitals_dm) ah on ac.case_hosp_code = ah.hosp_id
left join (select loc_id,loc_name as hosp_district,loc_parnt_id
		   from dwh.asri_locations_dm) al on ah.dist_id = al.loc_id
left join (select loc_id,loc_name as hosp_state,loc_parnt_id
		   from dwh.asri_locations_dm) alhs on al.loc_parnt_id = alhs.loc_id		   
left JOIN (SELECT loc_id, loc_name as patient_district,loc_parnt_id FROM dwh.asri_locations_dm ) ald ON ald.loc_id = ap.district_code
left JOIN (SELECT loc_id, loc_name as patient_state,loc_parnt_id FROM dwh.asri_locations_dm ) als on als.loc_id = ald.loc_parnt_id
) out_all 
inner join 
(select *
from 
(select 
ration_card_no, count(distinct speciality_name) as cnt
from 
(select case_id,case_hosp_code,cs_dis_main_code,case_status,case_patient_no,
      cs_apprv_rej_dt as preauth_approved_date
	  from dwh.asri_case_ft
	  where trunc(cs_apprv_rej_dt)>='2022-04-01') ac
inner join (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID = 'CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
inner  join (select patient_id,ration_card_no,district_code,mandal_code,age,gender,
		          case WHEN POSITION('/' IN RATION_CARD_NO) > 0 THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
                       ELSE RATION_CARD_NO
                       END AS FAMILY_CARD_NO
		   from dwh.asri_patient_dm ) ap on ac.case_patient_no = ap.patient_id
left join (select dis_main_id,dis_main_name as speciality_name
		   from dwh.asri_disease_main_cd) dm on ac.cs_dis_main_code = dm.dis_main_id
group by ration_card_no
)
where cnt>1
) cnt_r on cnt_r.ration_card_no = out_all.ration_card_no;





drop materialized view dwh.asri_discharge_facilitation_mv;

create materialized view dwh.asri_discharge_facilitation_mv as
select 
adcd.case_id,  ah.hosp_id, ah.hosp_name,ah.dist_id,hospital_district, hospital_state, au.user_id, au.new_emp_code as mithra_login , (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS MITHRA_NAME ,case when au.gender='M' then 'Male' when au.gender='F' then 'Female' end as gender  , au.cug as phno,
case when pat_dis_ltr_yn='Y' then 1 else 0 end  as is_discharge_letter_upload, case when acec.dis_photo_avail_yn='Y' then 1 else 0 end as is_cex_verified ,
cs_dis_dt as discharged_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_dis_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_dis_dt) + 1, 100), 'FM00') END) AS FY_discharged_date,  
 CASE 
        WHEN EXTRACT(MONTH FROM cs_dis_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM cs_dis_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM cs_dis_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END as discharged_date_quarter,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select case_id, pat_dis_ltr_yn  from dwh.asri_case_ft) ac
inner join(select  case_id, hosp_id , cs_dis_dt  from dwh.asri_discharge_case_data_dm ) adcd on adcd.case_id = ac.case_id
inner join(select distinct 
case_id , act_id , act_by  from 
(select distinct 
case_id , act_id , act_by , crt_dt , act_order,
RANK() OVER(PARTITION by case_id, act_id order by crt_dt desc) as ranking
from dwh.asri_audit_ft )
where ranking=1 and act_id = 'CD86') aud on aud.case_id = ac.case_id
left  join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) au on au.user_id=aud.act_by
left join (select hosp_id, hosp_name,dist_id  from dwh.asri_hospitals_dm ) ah on  ah.hosp_id = adcd.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select distinct case_id , dis_photo_avail_yn  from dwh.asri_claim_exec_chklst_dm where dis_photo_avail_yn is not null) acec on adcd.case_id = acec.case_id;





drop materialized view dwh.asri_grievances_mv;

create materialized view dwh.asri_grievances_mv as
select
	agsd.gr_seq_id , agsd.gr_source , agmd1.gr_desc , agsd.type_of_gr , agmd.gr_desc as griev_type_desc , agsd.hosp_id , ahd.hosp_name , ahd.hosp_type, ahd.govt_hosp_type , ahd.hosp_empnl_date, agsd.call_type , acc.cmb_dtl_name as call_type_name , agsd.gr_on_role , acc2.cmb_dtl_name as gr_on_role_name , agsd.gr_on_usr , aud.login_name as gr_on_usr_name , agsd.template_id , agsd.info_rcvd , agsd.info_provd , agsd.additional_remarks , agsd.gr_status , acc3.cmb_dtl_name as gr_status_name , agsd.crt_usr ,
	aud1.login_name as crt_user_name , agsd.crt_dt , agsd.lst_upd_usr , aud2.login_name as lst_updated_user_name, aud2.first_name , aud2.user_role , agsd.gr_nature , agmd2.gr_desc as gr_nature_name , agsd.gr_source_desc , agsd.gr_nature_desc , agsd.gr_type_desc , agsd.gr_ptdist_name , agsd.gr_hspdst_name , agsd.gr_hspname_desc , agsd.gr_role_status 
from
	asri_grievance_services_dm agsd
left join asri_grievance_master_dm agmd on
	agmd.gr_seq_id = agsd.type_of_gr
left join asri_grievance_master_dm agmd1 on
	agmd1.gr_seq_id = agsd.gr_source
left join (
	select
		hosp_id , hosp_name , hosp_type, govt_hosp_type , application_type , hosp_empnl_date , hosp_estab_yr, dist_id , loc_name as Old_Dist_name
	from
		asri_hospitals_dm
left join (
		select
			loc_id ,
			loc_name
		from
			asri_locations_dm ald
		where
			loc_hdr_id = 'LH6' ) ald on
		ald.loc_id = dist_id
	where
		isactive_ap = 'Y')ahd on
	ahd.hosp_id = agsd.hosp_id
left join asri_combo_cd acc on
	acc.cmb_dtl_id = agsd.call_type
left join asri_combo_cd acc2 on
	acc2.cmb_dtl_id = agsd.gr_on_role
left join asri_combo_cd acc3 on
	acc3.cmb_dtl_id = agsd.gr_status
left join asri_users_dm aud on
	aud.user_id = agsd.gr_on_usr
left join asri_users_dm aud1 on
	aud1.user_id = agsd.crt_usr
left join asri_users_dm aud2 on
	aud2.user_id = agsd.lst_upd_usr
left join asri_grievance_master_dm agmd2 on
	agmd2.gr_seq_id = agsd.gr_nature
where
	agsd.crt_dt > TO_DATE('2020-12-15',
	'YYYY-MM-DD');





DROP MATERIALIZED VIEW wt_anm_feedback_mv CASCADE;

create materialized view dwh.wt_anm_feedback_mv as
SELECT distinct  *,
case when is_pat_tranferred='1' then trans_district_name
else dis_patient_district end as dis_patient_district_final,
case when is_pat_tranferred='1' then trans_sachivalaym_name
else dis_sachivalayam_name end as dis_patient_sachivalayam_final,
case when tr_phc is not null then tr_phc 
else ds_phc end as dis_patient_phc_final,
case when diff<=8 then 'Within SLA'
	else 'Beyond SLA' end as feedback_status
FROM 
(SELECT 
ds.case_id, ds.mandal_name as dis_hosp_mandal_name, ds.hosp_dist_name as dis_hosp_dist_name, ds.hosp_id as dis_hosp_id, ds.hosp_name as dis_hosp_name,eh.hosp_type, eh.govt_hosp_type, ds.patient_id as discharged_patient_id,ds.patient_name as discharged_patient_name,
ds.resident_id as discharged_pat_resident_id, ds.status as discharge_status, ds.sachivalayam_name as dis_sachivalayam_name, ds.patient_mobile as dis_patient_mobile, uhidvalue as dis_pat_uhid,
ds.gender as dis_patient_gender, ds.patient_district as dis_patient_district, al_p1.loc_name as ds_phc, al_p2.loc_name as tr_phc,
ds.aarogya_mithra_name as dis_aarogya_mithra_name, ds.cs_dis_dt as pat_dis_date,
 CASE WHEN TO_DATE(ds.cs_dis_dt, 'YYYY-MM-DD') <='2023-11-30' THEN 'Until Nov 23'
 ELSE TO_CHAR(ds.cs_dis_dt, 'Month') END as dis_month_year,
ds.data_sent_date as dis_data_sent_date,
ds.anm_push_date as dis_case_pushed_date, -- is_case_pushed
CASE 
        WHEN ds.cs_dis_dt IS NOT NULL AND  DATEDIFF(hour,cs_dis_dt,sysdate)/24.0 >=3 then 1 
        ELSE '0'
    END AS is_case_pushed,
case when tr.case_id is null then '0'
     else '1' end  as is_pat_tranferred,
(SELECT loc_name from dwh.asri_locations_dm  where loc_id = tr.district_id) as trans_district_name, 
(SELECT loc_name from dwh.asri_locations_dm  where loc_id = tr.mandal_id) as trans_mandal_name,
tr.sachivalayam_id as trans_sachivalayam_id, al.loc_name as trans_sachivalaym_name, tr.called_the_patient , tr.call_anm_secretariat_confrmd,
case when fd.case_id is null then '0'
     else '1' end  as is_feedback_submitted,
free_services as fdbk_free_services,hospital_service_satisfaction as fdbk_hospital_service_satisfaction,mithra_service_satisfaction as fdbk_mithra_service_satisfaction,
communication_amt_coll as fdbk_communication_amt_coll,lab_diagnostics_amt_coll as fdbk_lab_diagnostics_amt_coll,medicine_amt_coll as fdbk_medicine_amt_coll,
operations_amt_coll as fdbk_operations_amt_coll,aarogyamithra_amt_coll as fdbk_aarogyamithra_amt_coll,other_hosp_staff_amt_coll as fdbk_other_hosp_staff_amt_coll,
total_amt_coll as fdbk_total_amt_coll, doctor_services as fdbk_doctor_services, nurse_services as fdbk_nurse_services, time_food_services as fdbk_time_food_services,
fd.crt_dt as feedback_submitted_date, 
CASE when fd.crt_dt is null then  ABS(EXTRACT(EPOCH FROM (SYSDATE - ds.cs_dis_dt)) / (3600 * 24)) 
	else ABS(EXTRACT(EPOCH FROM (fd.crt_dt - ds.cs_dis_dt)) / (3600 * 24))  END AS diff
FROM dwh.asri_discharge_case_data_dm as ds 
LEFT JOIN dwh.anm_transfer_case_dm as tr ON tr.case_id = ds.case_id 
LEFT JOIN dwh.asri_patient_dis_feedback_ref_dm as fd ON fd.case_id = ds.case_id
LEFT JOIN (SELECT loc_id, loc_name, old_loc_parnt_id FROM dwh.asri_locations_dm ) al_s ON al_s.loc_id = ds.sachivalayam_code
LEFT JOIN (SELECT loc_id, loc_name, old_loc_parnt_id FROM dwh.asri_locations_dm ) al_p1 ON al_p1.loc_id = al_s.old_loc_parnt_id
LEFT JOIN (SELECT loc_id, loc_name, old_loc_parnt_id FROM dwh.asri_locations_dm ) al ON al.loc_id = tr.sachivalayam_id
LEFT JOIN (SELECT loc_id, loc_name, old_loc_parnt_id FROM dwh.asri_locations_dm ) al_p2 ON al_p2.loc_id = al.old_loc_parnt_id
left join (select hosp_id,hosp_type, govt_hosp_type  from dwh.asri_hospitals_dm )eh on eh.hosp_id = ds.hosp_id
left join (select patient_id, uhidvalue from dwh.asri_patient_dm ) ap on ap.patient_id = ds.patient_id);





drop materialized view dwh.asri_ip_op_registration_mv_1;

create materialized view dwh.asri_ip_op_registration_mv_1 as 
select 
ap.patient_id , ap.ration_card_no , ap.crt_dt as NAM_crt_dt, aud.new_emp_code as NAM_LOGIN, ap.patient_ipop, ac.case_id, ac.case_regn_date, ah.hosp_id, ah.hosp_name, m_loc.hospital_mandal, d_loc.hospital_district, s_loc .hospital_state, 
case
	when ah.hosp_type = 'C' then 'Corporate'
	when ah.hosp_type = 'G' then 'Government'
end as hospital_type,
aeh.HOSP_BED_STRENGTH, aud.user_id, (NVL(aud.first_name , '') || ' ' || NVL(aud.last_name, ''))as USER_NAME , acc.cmb_dtl_name as USER_ROLE_NAME,
case
when aud.gender = 'M' then 'Male'
when aud.gender = 'F' then 'Female'
end as gender ,
aud.cug
from 
asri_patient_dm ap 
left join (select case_id , case_patient_no , case_hosp_code , case_regn_date  from dwh.asri_case_ft) ac  on ac.case_patient_no = ap.patient_id
inner join(select hosp_id, hosp_name, dist_id, HOSP_EMPNL_REF_NUM,hosp_type   from dwh.asri_hospitals_dm where isactive_ap = 'Y') ah on ah.hosp_id = ap.reg_hosp_id
left join (select HOSPINFO_ID, HOSP_BED_STRENGTH,district_code , mandal   from dwh.asri_empnl_hospinfo_dm) aeh on ah.HOSP_EMPNL_REF_NUM = aeh.HOSPINFO_ID
left join (select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug, user_role, active_yn  from dwh.asri_users_dm  ) aud on ap.CRT_USR = aud.user_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_mandal, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) m_loc ON m_loc.loc_id = aeh.mandal
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc ON d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select cmb_dtl_id, cmb_dtl_name from dwh.asri_combo_cd) acc on acc.cmb_dtl_id = aud.user_role;





drop materialized view dwh.asri_card_uiltization_mv;

create materialized view asri_card_uiltization_mv as
select 
householdcardno, card_type,membername, relation_name, NVL(ld.loc_name,district) as member_district, NVL(lm.loc_name,mandal) as member_mandal,
case when gender in ('1','M') then 'Male' when gender in ('2','F') then 'Female' else gender end as member_gender,age, SPLIT_PART(dateofbirth, ' ', 1) as member_dob
from 
(select householdcardno,LEFT(householdcardno, 3) as card_type,membername, relation_name,district,  mandal,  gender ,age, dateofbirth from dwh.asri_family_cs_ap_dm 
union 
select householdcardno, LEFT(householdcardno, 2) as card_type,membername, relation_name, district, mandal ,gender, age,dateofbirth from dwh.asri_family_cs_amaravati_dm
union 
select temp_card_num, LEFT(temp_card_num, 3) as card_type, membername, relation_name,NULL as district, NULL as mandal,gender, age, dateofbirth from dwh.asri_tap_family_ap_dm
union 
select temp_card_num,LEFT(temp_card_num, 3) as card_type, membername, relation_name,district_id, NULL as  mandal,gender , age, dateofbirth from asri_janmabhoomi_family_dm
) tf 
left join (SELECT loc_id, loc_name from dwh.asri_locations_dm  ) ld on ld.loc_id=tf.district
left join (SELECT loc_id, loc_name from dwh.asri_locations_dm  ) lm on lm.loc_id=tf.mandal;






drop materialized view dwh.asri_case_stages_mv;

create materialized view dwh.asri_case_stages_mv as 
select 
case_id,case_no,case_status,case_status_name, case_regn_date,fy_case_regn,is_preauth_initiated,preauth_inititated_date,preauth_initiated_amount,fy_preauth_init,is_preauth_approved,preauth_approved_date,preauth_approved_amount,fy_preauth_aprv,is_claim_submitted,claim_submitted_date,claim_submitted_amount,fy_claim_submit,
is_claim_approved,claim_approved_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM claim_approved_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM claim_approved_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM claim_approved_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM claim_approved_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM claim_approved_date) + 1, 100), 'FM00') END) AS fy_claim_aprv,
claim_approved_amount,
is_claim_paid,claim_paid_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM claim_paid_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM claim_paid_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM claim_paid_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM claim_paid_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM claim_paid_date) + 1, 100), 'FM00') END) AS fy_claim_paid,
claim_paid_amount
from
(select 
ac.case_id , ac.case_no, ac.case_status, case_status_name, 
ac.case_regn_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.case_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) + 1, 100), 'FM00') END) AS fy_case_regn,
case when NVL(ac.cs_preauth_dt,pre_init_dt) is not null then 1 else 0 end as is_preauth_initiated,NVL(ac.cs_preauth_dt,pre_init_dt) as preauth_inititated_date,pck_appv_amt as preauth_initiated_amount, 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM NVL(ac.cs_preauth_dt,pre_init_dt)) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(ac.cs_preauth_dt,pre_init_dt)) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(ac.cs_preauth_dt,pre_init_dt)), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(ac.cs_preauth_dt,pre_init_dt)), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(ac.cs_preauth_dt,pre_init_dt)) + 1, 100), 'FM00') END) AS fy_preauth_init,
case when pa.CASE_ID is not null then 1 else 0 end as is_preauth_approved, ac.cs_apprv_rej_dt as preauth_approved_date, pck_appv_amt as preauth_approved_amount,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.cs_apprv_rej_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS fy_preauth_aprv,
case when ac.clm_sub_dt is not null then 1 else 0 end as is_claim_submitted, ac.clm_sub_dt as claim_submitted_date,ac.cs_clm_bill_amt as claim_submitted_amount,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.clm_sub_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.clm_sub_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.clm_sub_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.clm_sub_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.clm_sub_dt) + 1, 100), 'FM00') END) AS fy_claim_submit,
case when NVL(aud.clm_aprv_dt,ac.ceo_aprv_dt) is not null then 1 when ac.case_status='CD145' then 1 else 0 end as is_claim_approved, 
case when NVL(aud.clm_aprv_dt,ac.ceo_aprv_dt) is not null then NVL(aud.clm_aprv_dt,ac.ceo_aprv_dt) when ac.case_status='CD145' then  ac.lst_upd_dt else null end as claim_approved_date,
case when NVL(aud.clm_aprv_dt,ac.ceo_aprv_dt) is not null then ac.cs_cl_amount when ac.case_status='CD145' then  ac.cs_cl_amount  else 0 end as claim_approved_amount,
case when NVL(clm_paid_date_1,clm_paid_date_2,ac.cs_sbh_dt) is not null then 1 when ac.case_status='CD125' then 1 else 0 end as is_claim_paid, 
case when NVL(clm_paid_date_1,clm_paid_date_2,ac.cs_sbh_dt) is not null then NVL(clm_paid_date_1,clm_paid_date_2,ac.cs_sbh_dt) when ac.case_status='CD125' then ac.lst_upd_dt else null end as claim_paid_date,
case when NVL(clm_paid_date_1,clm_paid_date_2,ac.cs_sbh_dt) is not null then ac.cs_cl_amount when ac.case_status='CD125' then ac.cs_cl_amount else 0 end as claim_paid_amount
--select count(ac.case_id)
from 
(select case_id , case_no, case_hosp_code , case_patient_no , case_status , case_regn_date , cs_surg_dt  ,cs_dis_dt , cs_dis_main_code ,
		cs_apprv_rej_dt ,cs_preauth_dt, cs_dt_pre_auth , clm_sub_dt , actual_clm_sub_dt ,ceo_aprv_dt, cs_sbh_dt, lst_upd_dt,cs_death_dt, cs_cl_amount, cs_clm_bill_amt,pck_appv_amt 
from dwh.asri_case_ft /*where DATE(case_regn_date) >='2019-04-01'*/   ) ac
LEFT JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
left join (
		select 
	case_id, MAX(pre_init_dt) pre_init_dt, MAX(clm_aprv_dt) clm_aprv_dt, MAX(clm_paid_date_1) clm_paid_date_1, MAX(clm_paid_date_2) clm_paid_date_2
	from 
	(select 
	aud.case_id,
	case when act_id='CD75' then crt_dt else null end as pre_init_dt,
	case when act_id='CD145' then crt_dt else null end as clm_aprv_dt, case when act_id = 'CD1251' then crt_dt else null end as clm_paid_date_1,case when act_id = 'CD125' then crt_dt else null end as clm_paid_date_2 
	from 
	(select distinct case_id , act_id , crt_dt from 
		(select case_id , act_id , crt_dt , act_by,act_order, RANK() OVER(partition by case_id , act_id order by crt_dt desc ) as ranking from dwh.asri_audit_ft where act_id in ('CD145','CD1251','CD125','CD75') )where ranking=1
	) aud
	left join (select cmb_dtl_id, cmb_dtl_name from dwh.asri_combo_cd ) cmb on cmb.cmb_dtl_id = aud.act_id
	)
	group by case_id
) aud on aud.case_id=ac.case_id
left join (select cmb_dtl_id,cmb_dtl_name as case_status_name
		   from dwh.asri_combo_cd) acb on ac.case_status = acb.cmb_dtl_id
);




drop materialized view dwh.foss_asri_people_benefit_overview_mv;

create materialized view dwh.foss_asri_people_benefit_overview_mv as
select  
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_apprv_rej_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS FYear,
       case when extract(MONTH FROM cs_apprv_rej_dt)<=9 then '0'||extract(MONTH FROM cs_apprv_rej_dt)|| ' ( '||TRIM (both '' from TO_CHAR(cs_apprv_rej_dt, 'Month'))||' )'
       when extract(MONTH FROM cs_apprv_rej_dt)>=10 then  extract(MONTH FROM cs_apprv_rej_dt)::text|| ' ( '||TRIM (both '' from TO_CHAR(cs_apprv_rej_dt, 'Month'))||' )' end as apprv_month, EXTRACT(YEAR FROM cs_apprv_rej_dt)::int as apprv_year,
 TO_CHAR(cs_apprv_rej_dt, 'Month YY') as fy_month, RATION_CARD_NO as patient_ration_card,
ac.CASE_ID,CASE_PATIENT_NO,uhidvalue, case_status,cmb.cmb_dtl_name as status_name, lst_upd_dt as status_date,cs_apprv_rej_dt,cs_surg_dt as surgery_date,cs_dis_dt as discharge_date,case_regn_date as case_registered_date, alps.loc_name as patient_state,    
ap.district_code, district_name as patient_district ,ap.mandal_code,  mandal_name as patient_mandal, ap.village_code, village_name as patient_village, pck_appv_amt,  case_trust_aprv_amt,
case when claim_case_id is not null and case_trust_aprv_amt is not null then case_trust_aprv_amt
when claim_case_id is not null and case_trust_aprv_amt is null then pck_appv_amt
else pck_appv_amt end as utilized_amount,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(SELECT CASE_ID,CASE_PATIENT_NO, cs_apprv_rej_dt,cs_surg_dt,cs_dis_dt,case_regn_date,case_status,lst_upd_dt, pck_appv_amt FROM dwh.asri_case_ft WHERE TRUNC(cs_apprv_rej_dt) >= TO_DATE('2023-04-01','YYYY-MM-DD')) ac 
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
LEFT JOIN (SELECT PATIENT_ID,DISTRICT_CODE,MANDAL_CODE, village_code, RATION_CARD_NO,
		  CASE 
    	  WHEN POSITION('/' IN RATION_CARD_NO) > 0 
    	  THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
          ELSE RATION_CARD_NO
          end AS FAMILY_CARD_NO,
          uhidvalue 
          FROM dwh.asri_patient_dm  ) ap ON ap.PATIENT_ID = ac.CASE_PATIENT_NO
left join(select distinct case_id as claim_case_id, case_trust_aprv_amt  from  dwh.asri_case_claim_dm) acc on acc.claim_case_id = ac.case_id
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS MANDAL_NAME
           FROM dwh.asri_locations_dm) al ON al.MANDAL_CODE = ap.MANDAL_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS DISTRICT_NAME, loc_parnt_id as district_parnt_loc
           FROM dwh.asri_locations_dm) alp ON alp.LOC_ID = ap.DISTRICT_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME
           FROM dwh.asri_locations_dm) alps ON alps.LOC_ID = alp.district_parnt_loc
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS VILLAGE_NAME	
           FROM dwh.asri_locations_dm) alv ON alv.LOC_ID = ap.village_code
left join (select cmb_dtl_id, cmb_dtl_name from dwh.asri_combo_cd ) cmb on cmb.cmb_dtl_id = ac.case_status;



drop materialized view dwh.asri_dist_wise_proc_total_amount_mv;

create materialized view dwh.asri_dist_wise_proc_total_amount_mv AS
select 
fy_preauth_aprv,state, district ,surgery_code, surgery_desc, spec_surg_aasra_amt_desc ,count(case_id) as total_cases,
SUM(case_trust_aprv_amt) AS total_approved_amount, 
        ROW_NUMBER() OVER (PARTITION BY fy_preauth_aprv,state, district ORDER BY SUM(case_trust_aprv_amt) DESC) AS ranking,
 CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(SELECT
	acf.case_id ,ald.loc_name as district,als.loc_name as state,  acsd.surgery_code , asd.surgery_desc, accd.case_trust_aprv_amt,
--NVL(acf.cs_dis_main_code,'')||'-'||NVL(dm.dis_main_name,'')||'_'||NVL(acsd.surgery_code,'')||'-'||NVL(asd.surgery_desc,'')||'_'||NVL(asd.surgery_amt,'0')||'_'||NVL(asd.postops_amt,'0'),
NVL(acf.cs_dis_main_code,'')||'-'||NVL(dm.dis_main_name,'')||'_'||NVL(asd.surgery_amt,'0')||'_'||NVL(asd.postops_amt,'0') as spec_surg_aasra_amt_desc,cs_apprv_rej_dt,case_regn_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM acf.cs_apprv_rej_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS fy_preauth_aprv
	--((acf.cs_dis_main_code) || ' - ' || (dm.dis_main_name))||_(( acsd.surgery_code) || ' - ' || (asd.surgery_desc))
    FROM
       (select case_id ,cs_apprv_rej_dt ,case_patient_no,case_regn_date,cs_dis_main_code  from dwh.asri_case_ft) acf
        INNER JOIN (select case_id , surgery_code  from dwh.asri_case_surgery_dm) acsd ON acsd.case_id = acf.case_id
        left join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt
			from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  				from dwh.asri_surgery_dm)
			where ranking = 1
		) asd on asd.surgery_id = acsd.surgery_code
		left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = acf.cs_dis_main_code
        INNER JOIN(select patient_id , district_code  from dwh.asri_patient_dm) apd ON apd.patient_id = acf.case_patient_no
        INNER JOIN (SELECT loc_id, loc_name,loc_parnt_id FROM dwh.asri_locations_dm WHERE loc_hdr_id = 'LH6') ald ON ald.loc_id = apd.district_code
        INNER JOIN (SELECT loc_id, loc_name,loc_parnt_id FROM dwh.asri_locations_dm ) als on als.loc_id = ald.loc_parnt_id
        INNER JOIN (select  case_id , case_trust_aprv_amt  from dwh.asri_case_claim_dm) accd ON accd.case_id = acf.case_id
        INNER JOIN (
            SELECT DISTINCT CASE_ID
            FROM (
                SELECT CASE_ID, ACT_ID
                FROM dwh.asri_audit_ft
            ) aa
            INNER JOIN (
                SELECT STATUS_ID
                FROM dwh.asrim_case_status_group
                WHERE GROUP_ID = 'CD17'
            ) csg ON aa.ACT_ID = csg.STATUS_ID
        ) pa ON acf.CASE_ID = pa.CASE_ID
        )
 group by fy_preauth_aprv, state, district ,surgery_code, surgery_desc, spec_surg_aasra_amt_desc ; 






drop  materialized view dwh.asri_dist_family_utilization_mv;

create materialized view dwh.asri_dist_family_utilization_mv as 
select distinct
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_apprv_rej_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS fyYear,
ac.case_id,patient_id,ap.district_code, district_name,ap.mandal_code,  mandal_name, ap.village_code, village_name,uhidvalue as uhid, FAMILY_CARD_NO, RATION_CARD_NO , pck_appv_amt as utilized_amount,cs_apprv_rej_dt as approved_date
from 
(SELECT CASE_ID,CASE_PATIENT_NO, cs_apprv_rej_dt, pck_appv_amt FROM dwh.asri_case_ft WHERE TRUNC(cs_apprv_rej_dt) >= TO_DATE('2019-04-01','YYYY-MM-DD')) ac 
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
LEFT JOIN (SELECT PATIENT_ID,DISTRICT_CODE,MANDAL_CODE, village_code, RATION_CARD_NO,
		  CASE 
    	  WHEN POSITION('/' IN RATION_CARD_NO) > 0 
    	  THEN SUBSTRING(RATION_CARD_NO, 1, POSITION('/' IN RATION_CARD_NO) - 1)
          ELSE RATION_CARD_NO
          end AS FAMILY_CARD_NO,
          uhidvalue 
          FROM dwh.asri_patient_dm  ) ap ON ap.PATIENT_ID = ac.CASE_PATIENT_NO
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS MANDAL_NAME
           FROM dwh.asri_locations_dm) al ON al.MANDAL_CODE = ap.MANDAL_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS DISTRICT_NAME
           FROM dwh.asri_locations_dm) alp ON alp.LOC_ID = ap.DISTRICT_CODE
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS VILLAGE_NAME	
           FROM dwh.asri_locations_dm) alv ON alv.LOC_ID = ap.village_code
WHERE FAMILY_CARD_NO NOT IN ('CMCO','ARG','NOARG');






drop materialized view dwh.asri_case_wise_preauth_apprv_aasra_paid_mv;

create materialized view dwh.asri_case_wise_preauth_apprv_aasra_paid_mv as
select  
*,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aasra_paid_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aasra_paid_date) + 1, 100), 'FM00') END) AS FY_aasra_paid_date,
case when rwn=1 then preauth_aprv_amount else 0 end as case_preauth_approved_amount,
case when rank_2=1 then postops_amt else 0 end as case_aasra_amount,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select 
ac.case_id,ac.cs_dis_main_code as speciality_code , dis_main_name as speciality_name,asd.surgery_id as procedure_code,asd.surgery_desc as procedure_name, asd.surgery_amt as procedure_defined_amount, postops_amt, ac.pck_appv_amt as preauth_aprv_amount,ap.patient_id,patient_name,  patient_ration_card_no,uhidvalue,district_code as patient_district_id,patient_district,patient_state,
hosp_id, hosp_name,hosp_type,govt_hosp_type,dist_id as hospital_dist_id,hospital_district, hospital_state, 
1 as is_preauth_approved,
case when is_perdm='Y'  and postops_amt is not null then 1 else 0 end as is_aasra_eligible,
case when  pat.act_id='CD1002' then 1 else 0 end as  is_aasra_paid,
case when  pat.act_id='CD1002' then pat.crt_dt else null end as  aasra_paid_date,
ac.case_regn_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) + 1, 100), 'FM00') END) AS FY_case_registered,
 ac.cs_apprv_rej_dt as preauth_approved_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM cs_apprv_rej_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS FY_preauth_approved,
ROW_NUMBER() OVER (PARTITION BY ac.case_id) as rwn,
ROW_NUMBER() OVER (PARTITION BY ac.case_id order by NVL(postops_amt,0) DESC ) as rank_2
from 
(SELECT CASE_ID,CASE_PATIENT_NO,case_hosp_code , cs_apprv_rej_dt,case_regn_date,case_status, pck_appv_amt,cs_dis_main_code FROM dwh.asri_case_ft where DATE(case_regn_date)>='2019-04-01') ac 
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID) pa ON ac.CASE_ID = pa.CASE_ID
left join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
left join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm, surgery_amt
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code
left join (select hosp_id, hosp_name,dist_id,    case when hosp_type='C' then 'Corporate'
				when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type  from dwh.asri_hospitals_dm ) ah on ac.case_hosp_code = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left  JOIN (SELECT patient_id,NVL(first_name,'')||' '||NVL(middle_name,'')||' '||NVL(last_name,'') as patient_name, ration_card_no AS patient_ration_card_no, sachivalayam_name,  age AS patient_age, case when gender='M' then 'Male' when gender='F' then 'Female' end  AS patient_gender, mandal_code , district_code , uhidvalue
                FROM dwh.asri_patient_dm
) ap ON ap.patient_id = ac.case_patient_no
LEFT JOIN (SELECT loc_id, loc_name as patient_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) pd_loc on pd_loc.loc_id = ap.district_code
LEFT JOIN (SELECT loc_id, loc_name as patient_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) ps_loc ON ps_loc.loc_id = pd_loc.loc_parnt_id
left join (
	select distinct patient_id , act_id , act_by, crt_dt 
		from 
		(select patient_id , act_id , act_order,act_by, crt_dt , RANK() OVER(partition by patient_id,act_id order by crt_dt desc ) as ranking
			from dwh.asri_patac_audit_dm 
		)
		where ranking=1 and act_id='CD1002' 
)pat on pat.patient_id = ap.patient_id
left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = ac.cs_dis_main_code
);



drop materialized view dwh.asri_onbed_check_mv;

create materialized view dwh.asri_onbed_check_mv as 
select  
ac.case_id,cs_dis_main_code as speciality_code,dis_main_name as speciality_name,asd.surgery_id as procedure_code,surgery_desc as procedure_name,case_patient_no, act_id, cmb_dtl_name as act_status,aud.crt_dt as action_taken_date, 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS fy_action_taken,
hosp_id , hosp_name,hosp_type,govt_hosp_type, NVL(hosp_bed_strength,'0') as hosp_bed_strength,hospital_district, hospital_state,
ac.case_regn_date, 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ac.case_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ac.case_regn_date) + 1, 100), 'FM00') END) AS fy_case_regn,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select case_id , case_hosp_code , case_patient_no , case_regn_date , cs_apprv_rej_dt,cs_dis_main_code  from dwh.asri_case_ft where DATE(case_regn_date) >='2022-04-01') ac 
inner join (select distinct case_id , act_id , crt_dt from 
		(select case_id , act_id , crt_dt , act_by,act_order, RANK() OVER(partition by case_id , act_id order by crt_dt desc ) as ranking from dwh.asri_audit_ft where act_id in ('CD73','CD76','CD329','CD732','CD75', 'CD771', 'CD3017', 'CD85','CD426', 'CD430', 'CD1800',
			'CD1801', 'CD1802', 'CD1803', 'CD50', 'CD64', 'CD3025', 'CD3026', ' CD3027', 'CD3028','CD1994', 'Cd427') )where ranking=1
) aud on aud.case_id=ac.case_id
left join (select hosp_id , hosp_name , hosp_contact_person , hosp_contact_no , cug_no , hosp_city , NVL(hosp_addr1,'')||','||NVL(hosp_addr2,'')||','||NVL(hosp_addr3,'') as hospital_address,case when isactive_ap='Y' then 'Active'else  'DeActive'  end as hosp_active_status,
	  			hosp_email , case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hosp_type,govt_hosp_type, dist_id , hosp_empnl_ref_num , hosp_empnl_date
			from dwh.asri_hospitals_dm 
) ah on ac.case_hosp_code = ah.hosp_id 
LEFT JOIN(SELECT hospinfo_id,status,hosp_bed_strength,pannumber, panholdername, mandal, constituency_code,district_code, city_code  FROM  dwh.asri_empnl_hospinfo_dm) hp_info ON hp_info.hospinfo_id=ah.hosp_empnl_ref_num
left join (select cmb_dtl_id , cmb_dtl_name  from dwh.asri_combo_cd) cmb2 on cmb2.cmb_dtl_id = aud.act_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = ac.cs_dis_main_code
left join (select distinct case_id, surgery_code from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
left join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt
from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  from dwh.asri_surgery_dm)
where ranking = 1
) asd on asd.surgery_id = acs.surgery_code;





drop materialized view dwh.case_wise_aasra_eligible_mv;

create materialized view dwh.case_wise_aasra_eligible_mv as
select   
asel.case_id,asel.surgery_id,asel.surgery_desc, asel.cs_dis_main_code,asel.dis_main_name as procedure_name,asel.postops_amt,asel.hosp_id,asel.hosp_name,asel.dist_id,asel.hospital_district,asel.hospital_state,asel.patient_id,asel.user_id,asel.new_emp_code,asel.mithra_name,asel.gender,asel.phno,
1 as is_eligible_for_aasara,
case when mas.case_id is not null then 1 else 0 end  as is_Mithra_account_submitted, asel.case_regn_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM asel.case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM asel.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM asel.case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM asel.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM asel.case_regn_date) + 1, 100), 'FM00') END) AS FY_case_regn_date,
asel.cs_apprv_rej_dt,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM asel.cs_apprv_rej_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM asel.cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM asel.cs_apprv_rej_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM asel.cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM asel.cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS FY_preauth_approved_date,
mas.nam_submitted_date,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM mas.nam_submitted_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM mas.nam_submitted_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM mas.nam_submitted_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM mas.nam_submitted_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM mas.nam_submitted_date) + 1, 100), 'FM00') END) AS FY_nam_submitted_date,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select 
ac.case_id,
asd.surgery_id,asd.surgery_desc,cs_dis_main_code,dm.dis_main_name,asd.postops_amt, ah.hosp_id, ah.hosp_name,ah.dist_id,hospital_district,hospital_state, ap.patient_id
,aud.user_id, aud.new_emp_code , (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS MITHRA_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug as phno,
CS_APPRV_REJ_DT,case_regn_date
from 
(select case_id , case_patient_no , case_hosp_code, case_status,CS_APPRV_REJ_DT,case_regn_date,cs_dis_main_code  from dwh.asri_case_ft where DATE(case_regn_date)>='2019-12-01') ac
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID
) pa ON ac.CASE_ID = pa.CASE_ID
left join (select case_id, surgery_code,DIS_MAIN_CODE from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
inner join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,STATE_FLAG
			from (select surgery_id,surgery_desc , postops_amt,is_perdm,STATE_FLAG,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  			from dwh.asri_surgery_dm)
		where ranking = 1 and  is_perdm = 'Y' and STATE_FLAG IN('AP','N','BOTH')) asd on asd.surgery_id = acs.surgery_code 
inner join (select patient_id , crt_usr,acc_lst_upd_usr ,acc_crt_usr from dwh.asri_patient_dm where ACC_STATUS<>'CD4044' or acc_status is not null) ap on ac.case_patient_no = ap.patient_id
left join (select hosp_id, hosp_name,dist_id  from dwh.asri_hospitals_dm ) ah on ac.case_hosp_code = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left  join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) aud on aud.user_id = ap.acc_crt_usr
left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = ac.cs_dis_main_code
) asel
left join (
select  
ac.case_id,
asd.surgery_id,asd.surgery_desc, cs_dis_main_code,dm.dis_main_name,asd.postops_amt, ah.hosp_id, ah.hosp_name,ah.dist_id,hospital_district,hospital_state, ap.patient_id
,aud.user_id, aud.new_emp_code , (NVL(first_name , '') || ' ' || NVL(last_name, ''))AS MITHRA_NAME ,case when aud.gender='M' then 'Male' when aud.gender='F' then 'Female' end as gender  , aud.cug as phno,CS_APPRV_REJ_DT,case_regn_date,
apa.crt_dt as nam_submitted_date
from 
(select case_id , case_patient_no , case_hosp_code, case_status,CS_APPRV_REJ_DT,case_regn_date,cs_dis_main_code  from dwh.asri_case_ft where DATE(case_regn_date)>='2019-12-01' ) ac
INNER JOIN (SELECT DISTINCT CASE_ID
			FROM (SELECT CASE_ID, ACT_ID FROM dwh.asri_audit_ft) aa 
			INNER JOIN (SELECT STATUS_ID FROM dwh.asrim_case_status_group WHERE GROUP_ID='CD17') csg ON aa.ACT_ID = csg.STATUS_ID
) pa ON ac.CASE_ID = pa.CASE_ID
left join (select case_id, surgery_code,DIS_MAIN_CODE from dwh.asri_case_surgery_dm) acs on acs.case_id = ac.case_id
inner join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,STATE_FLAG
			from (select surgery_id,surgery_desc , postops_amt,is_perdm,STATE_FLAG,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  			from dwh.asri_surgery_dm)
		where ranking = 1 and  is_perdm = 'Y' and STATE_FLAG IN('AP','N','BOTH')) asd on asd.surgery_id = acs.surgery_code 
inner join (select patient_id , crt_usr,acc_lst_upd_usr ,acc_crt_usr from dwh.asri_patient_dm where ACC_STATUS<>'CD4044' or acc_status is null) ap on ac.case_patient_no = ap.patient_id
left join (select hosp_id, hosp_name,dist_id  from dwh.asri_hospitals_dm ) ah on ac.case_hosp_code = ah.hosp_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_district, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) d_loc on d_loc.loc_id = ah.dist_id
LEFT JOIN (SELECT loc_id, loc_name as hospital_state, lgd_code, loc_parnt_id from dwh.asri_locations_dm ) s_loc ON s_loc.loc_id = d_loc.loc_parnt_id
left join( select distinct user_id ,new_emp_code ,first_name, last_name, gender , cug,  active_yn, user_role  from dwh.asri_users_dm ) aud on aud.user_id = ap.acc_crt_usr
inner join (	select distinct patient_id , act_id , act_by, crt_dt 
		from 
		(select patient_id , act_id , act_order,act_by, crt_dt , RANK() OVER(partition by patient_id,act_id order by crt_dt desc ) as ranking
			from dwh.asri_patac_audit_dm 
		)
		where ranking=1 and ACT_ID='CD4033'
) apa on  apa.PATIENT_ID=ap.PATIENT_ID
left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = ac.cs_dis_main_code
)mas on mas.case_id = asel.case_id and mas.user_id= asel.user_id and asel.surgery_id = mas.surgery_id;






drop materialized view dwh.asri_dist_wise_proc_total_cases_mv;

create materialized view dwh.asri_dist_wise_proc_total_cases_mv as 
select 
fy_preauth_aprv,state, district ,surgery_code, surgery_desc, spec_surg_aasra_amt_desc ,count(case_id) as total_cases,
SUM(case_trust_aprv_amt) AS total_approved_amount, 
        ROW_NUMBER() OVER (PARTITION BY fy_preauth_aprv,state, district ORDER BY COUNT(case_id) DESC) AS ranking,
 CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(SELECT
	acf.case_id ,ald.loc_name as district,als.loc_name as state,  acsd.surgery_code , asd.surgery_desc, accd.case_trust_aprv_amt,
--NVL(acf.cs_dis_main_code,'')||'-'||NVL(dm.dis_main_name,'')||'_'||NVL(acsd.surgery_code,'')||'-'||NVL(asd.surgery_desc,'')||'_'||NVL(asd.surgery_amt,'0')||'_'||NVL(asd.postops_amt,'0'),
NVL(acf.cs_dis_main_code,'')||'-'||NVL(dm.dis_main_name,'')||'_'||NVL(asd.surgery_amt,'0')||'_'||NVL(asd.postops_amt,'0') as spec_surg_aasra_amt_desc,cs_apprv_rej_dt,case_regn_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM acf.cs_apprv_rej_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM acf.cs_apprv_rej_dt) + 1, 100), 'FM00') END) AS fy_preauth_aprv
	--((acf.cs_dis_main_code) || ' - ' || (dm.dis_main_name))||_(( acsd.surgery_code) || ' - ' || (asd.surgery_desc))
    FROM
       (select case_id ,cs_apprv_rej_dt ,case_patient_no,case_regn_date,cs_dis_main_code  from dwh.asri_case_ft) acf
        INNER JOIN (select case_id , surgery_code  from dwh.asri_case_surgery_dm) acsd ON acsd.case_id = acf.case_id
        left join ( select distinct  surgery_id,surgery_desc  , postops_amt,is_perdm,surgery_amt
			from (select surgery_id,surgery_desc , postops_amt,is_perdm,surgery_amt,rank() OVER(partition by surgery_id order by surgery_sk desc) as ranking
	  				from dwh.asri_surgery_dm)
			where ranking = 1
		) asd on asd.surgery_id = acsd.surgery_code
		left join (select distinct dis_main_id , dis_main_name  from dwh.asri_disease_main_cd ) dm on dm.dis_main_id = acf.cs_dis_main_code
        INNER JOIN(select patient_id , district_code  from dwh.asri_patient_dm) apd ON apd.patient_id = acf.case_patient_no
        INNER JOIN (SELECT loc_id, loc_name,loc_parnt_id FROM dwh.asri_locations_dm WHERE loc_hdr_id = 'LH6') ald ON ald.loc_id = apd.district_code
        INNER JOIN (SELECT loc_id, loc_name,loc_parnt_id FROM dwh.asri_locations_dm ) als on als.loc_id = ald.loc_parnt_id
        INNER JOIN (select  case_id , case_trust_aprv_amt  from dwh.asri_case_claim_dm) accd ON accd.case_id = acf.case_id
        INNER JOIN (
            SELECT DISTINCT CASE_ID
            FROM (
                SELECT CASE_ID, ACT_ID
                FROM dwh.asri_audit_ft
            ) aa
            INNER JOIN (
                SELECT STATUS_ID
                FROM dwh.asrim_case_status_group
                WHERE GROUP_ID = 'CD17'
            ) csg ON aa.ACT_ID = csg.STATUS_ID
        ) pa ON acf.CASE_ID = pa.CASE_ID
        )
 group by fy_preauth_aprv, state, district ,surgery_code, surgery_desc, spec_surg_aasra_amt_desc ; 

































































