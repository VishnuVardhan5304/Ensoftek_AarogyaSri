create materialized view dwh_ehs.op_exec_perf_rep_mv as
select  
eu.user_id as Executive_Id,  user_name as Excecutive_Name, login_name,aud.act_id, aud.crt_dt as action_taken_date, aud.case_id,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
CASE WHEN act_id in ('CD204', 'CD2041') then 1 else 0 end as is_pex_action,
case WHEN act_id in ('CD420','CD360','CD1326','CD41' ) then 1 else 0 end as is_cex_action,  0 as is_fex_action,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select case_id,patient_scheme  from dwh_ehs.ehf_case_ft) ec
inner join (select distinct case_id, act_id, act_by, crt_dt FROM dwh_ehs.ehf_audit_ft where act_id in ('CD204', 'CD2041','CD420','CD360','CD1326','CD41') ) aud on aud.case_id = ec.case_id   -- Taken inner join beacuse some may be op cases or many had just registered
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name,login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = aud.act_by
union all
select  
eu.user_id as Executive_Id,  user_name as Excecutive_Name, login_name,faud.act_id, faud.crt_dt as action_taken_date, faud.case_id,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme, 0 as is_pex_action, 0 as is_cex_action,  1 as is_fex_action,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM faud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM faud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM faud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM faud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM faud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM faud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM faud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM faud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select case_id, patient_scheme from dwh_ehs.ehf_case_ft ) ec
inner join (select distinct case_followup_id,SPLIT_PART(case_followup_id, '/', 1) AS case_id, act_order, act_id, act_by, crt_dt from dwh_ehs.ehf_followup_audit_dm where act_id='CD64' ) faud on faud.case_id = ec.case_id   -- Taken inner join beacuse some may be op cases or many had just registered
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name,login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = faud.act_by;





create materialized view dwh_ehs.ehs_preauth_pending_mv as 
select *,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select pd.pending_by as waiting_preauths, COALESCE(AT_TRUST,0) AS AT_TRUST, COALESCE(PEX,0) AS PEX, COALESCE(PPD,0) AS PPD, COALESCE(PTD,0) AS PTD, COALESCE(EO,0) AS EO, 
 COALESCE(AT_HOSPITAL,0) AS AT_HOSPITAL, COALESCE(MITHRA,0) AS MITHRA, COALESCE(MEDCO,0) AS MEDCO
from 
(select '1' as sno,'<12 hours' as pending_by
union all
select '2' as sno,'12 to 24 hrs' as pending_by
union all
select '3' as sno,'>24 hrs' as pending_by
) pd
LEFT JOIN (
SELECT waiting_preauths_hours_bucket AS waiting_preauths_hours_bucket,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('PEX', 'PPD', 'PTD', 'EO') THEN 1
             END) AS "AT_TRUST",
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('PEX') THEN 1
             END) AS PEX,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('PPD') THEN 1
             END) AS PPD,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('PTD') THEN 1 
             END) AS PTD,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ( 'EO') THEN 1
             END) AS EO,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('Mithra','Medco') THEN 1
             END) AS AT_HOSPITAL,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('Mithra') THEN 1
             END) AS MITHRA,
       COUNT(CASE
                 WHEN PREAUTH_PENDING_BY IN ('Medco') THEN 1
             END) AS MEDCO
FROM
  (select   * from 
(select   case_status,
             CASE
                 WHEN (CASE_STATUS = 'CD2') THEN 'Medco'
                 WHEN (CASE_STATUS in ('CD6','CD651','CD652')) THEN 'Mithra'
                 WHEN (CASE_STATUS in ('CD7','CD12001','CD1301')) THEN 'PEX'
                 WHEN (CASE_STATUS in ('CD204','CD2041','CD210')) THEN 'PPD'
                 WHEN (CASE_STATUS in ('CD205','CD2058','CD20581','CD20591','CD206','CD10')) THEN 'PTD'
                 WHEN (CASE_STATUS in ('CD801','CD217','CD899','CD954','CD897')) THEN 'EO'
                 when preauth_total_package_amt>=200000 and case_status='CD8' then 'EO' 
             END PREAUTH_PENDING_BY,
         CASE
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)<12 THEN '<12 hrs'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)>=12
                            AND ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)<=24 THEN '12 to 24 hrs'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)>24 THEN '>24 hrs'
                       ELSE ''
         END AS waiting_preauths_hours_bucket
from
( select case_id ,CASE_HOSP_CODE,case_patient_no, case_regn_date , cs_preauth_dt, preauth_fwd_dt, case_status , lst_upd_dt,lst_upd_usr,patient_scheme,preauth_total_package_amt  from dwh_ehs.ehf_case_ft
    WHERE CASE_STATUS IN (	'CD2','CD6','CD651','CD652','CD7','CD12001','CD1301','CD204','CD2041','CD210','CD205','CD2058','CD20581','CD20591','CD206','CD10','CD801','CD217','CD899','CD954','CD897','CD8')
     and patient_scheme='CD501'
    
 ) ec 
)
where PREAUTH_PENDING_BY is not null) AS virtual_table
GROUP BY waiting_preauths_hours_bucket
ORDER BY "AT_TRUST" DESC
)ecp on ecp.waiting_preauths_hours_bucket = pd.pending_by
ORDER BY sno
);










create materialized view  dwh_ehs.ehs_claims_pending_mv as
select *,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select pd.waiting_claim_in_days as waiting_claims, COALESCE(AT_TRUST,0) AS AT_TRUST, COALESCE(CEX,0) AS CEX, COALESCE(CPD,0) AS CPD, COALESCE(CTD,0) AS CTD, COALESCE(JEO,0) AS JEO, 
COALESCE(EO,0) AS EO, COALESCE(EO_comm,0) AS EO_comm, COALESCE(CEO,0) AS CEO, COALESCE(AT_HOSPITAL,0) AS AT_HOSPITAL, COALESCE(MEDCO,0) AS MEDCO
from 
(select '1' as sno, '<= 7 days' as waiting_claim_in_days
union all
select '2' as sno, '> 7 days' as waiting_claim_in_days
) pd
LEFT JOIN (
SELECT waiting_claims_days_bucket ,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('EO-comm', 'CEX', 'CPD', 'CTD', 'JEO', 'EO', 'CEO') THEN 1
             END) AS AT_TRUST,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('CEX') THEN 1
             END) AS CEX,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('CPD') THEN 1
             END) AS CPD,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('CTD') THEN 1
             END) AS CTD,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('JEO') THEN 1
             END) AS JEO,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('EO') THEN 1
             END) AS EO,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('EO-comm') THEN 1
             END) AS EO_comm,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('CEO') THEN 1
             END) AS CEO,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('MEDCO') THEN 1
             END) AS AT_HOSPITAL,
       COUNT(CASE
                 WHEN CLAIM_PENDING_BY IN ('MEDCO') THEN 1
             END) AS MEDCO
FROM
  (select   * from 
(select  
             CASE
                 WHEN (CASE_STATUS = 'CD2') THEN 'MEDCO'
                 WHEN (CASE_STATUS = 'CD40') THEN 'CEX'
                 WHEN (CASE_STATUS in ('CD41','CD44','CD1111','CD20052')) THEN 'CPD'
                 WHEN (CASE_STATUS in ('CD42','CD43','CD47','CD1112')) THEN 'CTD'
                 WHEN (CASE_STATUS in ('CD45','CD46')) THEN 'JEO'
                 WHEN (CASE_STATUS in ('CD483','CD485','CD486')) THEN 'EO'
                 WHEN (CASE_STATUS in ('CD481','CD482','CD484','CD489')) THEN 'EO-comm'
                 WHEN (CASE_STATUS = 'CD487') THEN 'CEO'
                 ELSE ''
             END AS CLAIM_PENDING_BY,
                   CASE
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)<=7 then '<= 7 days'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)>7 then '> 7 days'
                       ELSE ''
                   END AS waiting_claims_days_bucket
from
( select case_id ,CASE_HOSP_CODE,case_patient_no, case_regn_date , cs_preauth_dt, preauth_fwd_dt, case_status , lst_upd_dt,lst_upd_usr,patient_scheme,preauth_total_package_amt  from dwh_ehs.ehf_case_ft
    WHERE CASE_STATUS IN ('CD2','CD40','CD41','CD44','CD1111','CD20052','CD42','CD43','CD47','CD1112','CD45','CD46','CD483','CD485','CD486','CD481','CD482','CD484','CD489','CD487') 
        and patient_scheme='CD501'
 ) ec 
)
where CLAIM_PENDING_BY is not null) AS virtual_table
GROUP BY waiting_claims_days_bucket
ORDER BY "AT_TRUST" DESC
)ecp on ecp.waiting_claims_days_bucket = pd.waiting_claim_in_days
ORDER BY sno
);









create materialized view dwh_ehs.ehs_active_hospitals_dtls_mv as
select  
eh.hosp_id as hosp_code,hosp_name as hospital_name,ehe.status as hospital_empnl_status_code, cmb.cmb_dtl_name as hospital_empnl_status_name, hosp_email,hosp_contact_person,hosp_contact_no, hosp_dist as hospital_district_code,hospital_district,hospital_type,govt_hosp_type,state_code as hospital_state_code,hospital_state,hosp_city,
hospital_address,  case when hosp_active_yn='Y' then 'Active' when hosp_active_yn='N' then 'In-Active' when hosp_active_yn='E' then 'De-Empanelled' when hosp_active_yn='D' then 'De-Listed' when hosp_active_yn='S' then 'Suspended' when hosp_active_yn='P' then 'Stop Payments' when hosp_active_yn='CB' then 'Claim Block' else null end as hospital_status,
hosp_empnl_ref_num, hosp_empnl_date,nabh_flg,bed_strength, NVL(eh.pancard, ehe.pannumber) as pancard_number, bankname, bankbranch as bank_branch_name,ifsccode,bankacntnumbr as bank_account_number,
es.icd_cat_code as speciality_code, esp.dis_main_name  as speciality_name, 
((es.icd_cat_code) || ' - ' || (esp.dis_main_name)) as speciality_desc,
case when icd_cat_code='S18' then 1 else 0 end as is_dental_surgery,
hosp_md_ceo_name , hosp_md_email ,hosp_md_tel_ph,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM hosp_empnl_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date) + 1, 100), 'FM00') END) AS FY_hosp_empanelled
,CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select hosp_id,hosp_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,hosp_city,
(isnull(house_no,'')|| ' ' || isnull(street,'')) as hospital_address, hosp_active_yn, hosp_empnl_date,nabh_flg,bed_strength,hosp_empnl_ref_num, pancard
from  dwh_ehs.ehfm_hospitals_dm  ) eh 
left join (select distinct hosp_id,icd_cat_code  from dwh_ehs.ehfm_hosp_speciality_dm where is_active_flg='Y' ) es on es.hosp_id = eh.hosp_id
LEFT JOIN (select distinct  dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = es.icd_cat_code
left join (SELECT LOC_ID,LOC_NAME AS hospital_district	, loc_parnt_id   FROM dwh_ehs.ehfm_locations_dm) elhd on elhd.loc_id = eh.hosp_dist
left join (select loc_id,loc_name as hospital_state from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = eh.state_code
left join (select distinct  hospinfo_id, hosp_md_ceo_name , hosp_md_email,hosp_md_tel_ph , status, pannumber, bankname, bankbranch,ifsccode, bankacntnumbr  from dwh_ehs.ehf_empnl_hospinfo_dm)  ehe on eh.hosp_empnl_ref_num = ehe.hospinfo_id
left join (select cmb_dtl_id , cmb_dtl_name  from dwh_ehs.asrim_combo_ehs ) cmb on cmb.cmb_dtl_id = ehe.status;




 



create materialized view dwh_ehs.ppd_perf_rep_mv as
select  
	aud.case_id,user_id as Panel_Doctor_Id , user_name as Panel_Doctor_Name, login_name, aud.act_id,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
cmb_dtl_name as  Panel_Doctor_Action, crt_dt AS Action_Taken_Date,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_id,patient_scheme  from dwh_ehs.ehf_case_ft) ec
inner join (select distinct
		case_id , act_id , act_by, crt_dt
	from
	(select 
		case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft  WHERE act_id IN ('CD210', 'CD2058', 'CD205', 'CD2059')
        )
	where ranking=1) aud on  aud.case_id = ec.case_id
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name, login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = aud.act_by
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = aud.act_id;






create materialized view dwh_ehs.ptd_perf_rep_mv as
select   
	aud.case_id, user_id as Trust_Doctor_Id,user_name as Trust_Doctor_Name, login_name, aud.act_id,
cmb_dtl_name AS Panel_Trust_Doctor_Action, crt_dt AS Action_Taken_Date,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_id,patient_scheme  from dwh_ehs.ehf_case_ft) ec
inner join (select distinct
		case_id , act_id , act_by, crt_dt
	from
	(select 
		case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft  WHERE act_id IN ('CD10', 'CD8', 'CD801', 'CD9')
        )
	where ranking=1) aud on  aud.case_id = ec.case_id
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name, login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = aud.act_by
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = aud.act_id;








create materialized view dwh_ehs.cpd_perf_rep_mv as
select   
	aud.case_id,user_id AS Claim_Panel_Doctor_Id, user_name as Claim_Panel_Doctor_Name, login_name, aud.act_id, 
cmb_dtl_name as Claim_Panel_Doctor_Action, crt_dt AS Action_Taken_Date,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_id,patient_scheme  from dwh_ehs.ehf_case_ft) ec
inner join (select distinct
		case_id , act_id , act_by, crt_dt
	from
	(select 
		case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft  WHERE act_id IN ('CD1213' , 'CD1212', 'CD44' , 'CD42','CD43' ,'CD482' )
        )
	where ranking=1) aud on  aud.case_id = ec.case_id
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name, login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = aud.act_by
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = aud.act_id;





create materialized view dwh_ehs.ctd_perf_rep_mv as
SELECT  
	aud.case_id,user_id AS Trust_Doctor_Id, user_name as Trust_Doctor_Name, login_name, aud.act_id, 
 cmb_dtl_name as Claim_Trust_Doctor_Action, crt_dt AS Action_Taken_Date,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_id,patient_scheme  from dwh_ehs.ehf_case_ft) ec
inner join (select distinct
		case_id , act_id , act_by, crt_dt
	from
	(select 
		case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft  WHERE act_id IN ('CD119', 'CD13', 'CD14', 'CD149', 'CD15', 'CD45', 'CD46', 'CD47', 'CD471', 'CD480', 'CD481', 'CD483', 'CD484', 'CD53', 'CD54', 'CD55')
        )
	where ranking=1) aud on  aud.case_id = ec.case_id
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name, login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = aud.act_by
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = aud.act_id;




create materialized view dwh_ehs.ftd_perf_rep_mv as
select  
case_followup_id,user_id as FollowUp_Trust_Doctor_Id,user_name as FollowUp_Trust_Doctor_Name, login_name, aud.act_id,
cmb_dtl_name as FollowUp_Trust_Doctor_Action, crt_dt AS Action_Taken_Date,
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
 'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM aud.crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM aud.crt_dt) + 1, 100), 'FM00') END) AS FY_action_taken,
       CASE 
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS action_taken_quarter, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_id,patient_scheme  from dwh_ehs.ehf_case_ft) ec
inner join (select distinct
		case_followup_id, case_id , act_id , act_by , crt_dt
	from
	(select  case_followup_id,SPLIT_PART(case_followup_id, '/', 1) AS case_id, act_order, act_id, act_by, crt_dt, 
		 RANK() OVER(PARTITION by case_followup_id, act_id  order by crt_dt desc) as ranking
 	   from dwh_ehs.ehf_followup_audit_dm where act_id in ('CD65','CD66','CD67')
        )
	where ranking=1) aud on  aud.case_id = ec.case_id
LEFT JOIN ( SELECT user_id, dsgn_id, (isnull(first_name,'') + ' '+ isnull(last_name,'')) as user_name, login_name FROM dwh_ehs.ehfm_users_dm ) eu ON eu.user_id = aud.act_by
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = aud.act_id;






create materialized view dwh_ehs.ehs_preauth_pending_details_mv as 
select  distinct   * from 
(select  
ec.case_id,preauth_total_package_amt as preauth_inititated_amount,
dis_main_id as specilaity_code,dis_main_name as speciality_name,emt.icd_proc_code as procedure_code , emt.proc_name,
case_regn_date , cs_preauth_dt as PREAUTH_INITIATE_DATE, preauth_fwd_dt as PREAUTH_FORWARDED_TRUST_DATE, case_status,cmb_dtl_name as last_act_detail , 
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,eu.user_id as user_id,  (NVL(first_name , '') || ' ' || NVL(last_name, ''))as USER_NAME, login_name as user_login_name, 
lst_upd_dt as Pending_Date,ep.patient_id, patient_ipop,
 eh.hosp_id as hosp_code,hosp_name as hospital_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist as hospital_district_code,hospital_district,hospital_type,govt_hosp_type,state_code as hospital_state_code,hospital_state,hosp_city,
hospital_address,   hosp_active_yn,hosp_empnl_ref_num, hosp_empnl_date,nabh_flg,bed_strength,
             CASE
                 WHEN (CASE_STATUS = 'CD2') THEN 'MEDCO'
                 WHEN (CASE_STATUS in ('CD6','CD651','CD652')) THEN 'MITHRA'
                 WHEN (CASE_STATUS in ('CD7','CD12001','CD1301')) THEN 'PEX'
                 WHEN (CASE_STATUS in ('CD204','CD2041','CD210')) THEN 'PPD'
                 WHEN (CASE_STATUS in ('CD205','CD2058','CD20581','CD20591','CD206','CD10')) THEN 'PTD'
                 WHEN (CASE_STATUS in ('CD801','CD217','CD899','CD954','CD897')) THEN 'EO'
                 when preauth_total_package_amt>=200000 and case_status='CD8' then 'EO' 
             END PREAUTH_PENDING_BY,
ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1) waiting_preauth_in_hours,
                   CASE
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)<12 THEN '<12 hrs'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)>=12
                            AND ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)<=24 THEN '12 to 24 hrs'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)>24 THEN '>24 hrs'
                       ELSE ''
                   END AS waiting_preauths_hours_bucket,
ROUND((ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE()), 1)/24.0),0) as waiting_preauth_in_days,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FY_case_regn,
      CASE 
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS case_regn_quarter,
            'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM lst_upd_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt) + 1, 100), 'FM00') END) AS FY_pending,
        CASE 
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS pending_quarter,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
( select case_id ,CASE_HOSP_CODE,case_patient_no, case_regn_date , cs_preauth_dt, preauth_fwd_dt, case_status , lst_upd_dt,lst_upd_usr,patient_scheme,preauth_total_package_amt  from dwh_ehs.ehf_case_ft
    WHERE CASE_STATUS IN (	'CD2','CD6','CD651','CD652','CD7','CD12001','CD1301','CD204','CD2041','CD210','CD205','CD2058','CD20581','CD20591','CD206','CD10','CD801','CD217','CD899','CD954','CD897','CD8')
 ) ec 
left JOIN (SELECT distinct case_id,asri_cat_code,icd_proc_code FROM dwh_ehs.ehf_case_therapy_dm  where activeyn='Y' ) ect ON ect.case_id = ec.case_id
LEFT JOIN(SELECT distinct asri_code, icd_proc_code, proc_name FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201') emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code
LEFT JOIN (SELECT distinct dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
INNER join (select hosp_id,hosp_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,hosp_city,
			(isnull(house_no,'')|| ' ' || isnull(street,'')) as hospital_address, hosp_active_yn, hosp_empnl_date,nabh_flg,bed_strength,hosp_empnl_ref_num
			from  dwh_ehs.ehfm_hospitals_dm   where  hosp_active_yn='Y'
		 ) eh  on eh.hosp_id = ec.CASE_HOSP_CODE
left join (SELECT LOC_ID,LOC_NAME AS hospital_district	, loc_parnt_id   FROM dwh_ehs.ehfm_locations_dm) elhd on elhd.loc_id = eh.hosp_dist
left join (select loc_id,loc_name as hospital_state from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = eh.state_code	
LEFT JOIN(SELECT cmb_dtl_id,cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = ec.case_status
left join (select patient_id, patient_ipop  from dwh_ehs.ehf_patient_dm ) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN(SELECT USER_ID,FIRST_NAME,LAST_NAME , dsgn_id, login_name FROM dwh_ehs.ehfm_users_dm) eu ON ec.lst_upd_usr= eu.user_id
left join (select dsgn_id,dsgn_name from dwh_ehs.ehfm_designation_dm) ed on ed.dsgn_id = eu.dsgn_id
)
where PREAUTH_PENDING_BY is not null ;










   

create materialized view  dwh_ehs.ehs_claims_pending_details_mv as
select  
ec.case_id,
dis_main_id as specilaity_code,dis_main_name as speciality_name,emt.icd_proc_code as procedure_code , emt.proc_name,
case_regn_date , cs_preauth_dt as PREAUTH_INITIATE_DATE, preauth_fwd_dt as PREAUTH_FORWARDED_TRUST_DATE, case_status,cmb_dtl_name as last_act_detail , 
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,eu.user_id as user_id,  (NVL(first_name , '') || ' ' || NVL(last_name, ''))as USER_NAME, login_name as user_login_name,
lst_upd_dt as Pending_Date,ep.patient_id, patient_ipop,
 eh.hosp_id as hosp_code,hosp_name as hospital_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist as hospital_district_code,hospital_district,hospital_type,govt_hosp_type,state_code as hospital_state_code,hospital_state,hosp_city,
hospital_address,   hosp_active_yn,hosp_empnl_ref_num, hosp_empnl_date,nabh_flg,bed_strength,
             CASE
                 WHEN (CASE_STATUS = 'CD2') THEN 'MEDCO'
                 WHEN (CASE_STATUS = 'CD40') THEN 'CEX'
                 WHEN (CASE_STATUS in ('CD41','CD44','CD1111','CD20052')) THEN 'CPD'
                 WHEN (CASE_STATUS in ('CD42','CD43','CD47','CD1112')) THEN 'CTD'
                 WHEN (CASE_STATUS in ('CD45','CD46')) THEN 'JEO'
                 WHEN (CASE_STATUS in ('CD483','CD485','CD486')) THEN 'EO'
                 WHEN (CASE_STATUS in ('CD481','CD482','CD484','CD489')) THEN 'EO-comm'
                 WHEN (CASE_STATUS = 'CD487') THEN 'CEO'
                 ELSE ''
             END AS CLAIM_PENDING_BY,
ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0) waiting_claim_in_days,
                   CASE
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)<=7 then '<= 7 days'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)>7 then '> 7 days'
                       ELSE ''
                   END AS waiting_claims_days_bucket,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FY_case_regn,
      CASE 
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS case_regn_quarter,
            'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM lst_upd_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt) + 1, 100), 'FM00') END) AS FY_pending,
        CASE 
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS pending_quarter,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
( select case_id ,CASE_HOSP_CODE,case_patient_no, case_regn_date , cs_preauth_dt, preauth_fwd_dt, case_status , lst_upd_dt,lst_upd_usr,patient_scheme,preauth_total_package_amt  from dwh_ehs.ehf_case_ft
    WHERE CASE_STATUS IN ('CD2','CD40','CD41','CD44','CD1111','CD20052','CD42','CD43','CD47','CD1112','CD45','CD46','CD483','CD485','CD486','CD481','CD482','CD484','CD489','CD487')
 ) ec 
left JOIN (SELECT distinct case_id,asri_cat_code,icd_proc_code FROM dwh_ehs.ehf_case_therapy_dm where activeyn='Y' ) ect ON ect.case_id = ec.case_id
LEFT JOIN(SELECT distinct asri_code, icd_proc_code, proc_name FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201') emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code
LEFT JOIN (SELECT distinct dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
INNER join (select hosp_id,hosp_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,hosp_city,
			(isnull(house_no,'')|| ' ' || isnull(street,'')) as hospital_address, hosp_active_yn, hosp_empnl_date,nabh_flg,bed_strength,hosp_empnl_ref_num
			from  dwh_ehs.ehfm_hospitals_dm  where  hosp_active_yn='Y'
		 ) eh  on eh.hosp_id = ec.CASE_HOSP_CODE
left join (SELECT LOC_ID,LOC_NAME AS hospital_district	, loc_parnt_id   FROM dwh_ehs.ehfm_locations_dm) elhd on elhd.loc_id = eh.hosp_dist
left join (select loc_id,loc_name as hospital_state from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = eh.state_code	
LEFT JOIN(SELECT cmb_dtl_id,cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = ec.case_status
left join (select patient_id, patient_ipop  from dwh_ehs.ehf_patient_dm ) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN(SELECT USER_ID,FIRST_NAME,LAST_NAME , dsgn_id, login_name FROM dwh_ehs.ehfm_users_dm) eu ON ec.lst_upd_usr= eu.user_id
left join (select dsgn_id,dsgn_name from dwh_ehs.ehfm_designation_dm) ed on ed.dsgn_id = eu.dsgn_id;











create materialized view dwh_ehs.ehs_followup_pending_mv as
select *,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select pd.waiting_followup_claim_in_days as waiting_followup_claims, COALESCE(AT_TRUST,0) AS AT_TRUST, COALESCE(FCX,0) AS FCX, COALESCE(FTD,0) AS FTD, COALESCE(CH,0) AS CH, COALESCE(CEO,0) AS CEO, 
COALESCE(AT_HOSPITAL,0) AS AT_HOSPITAL, COALESCE(MEDCO,0) AS MEDCO, COALESCE(MITHRA,0) AS MITHRA
from 
(select '1' as sno, '<= 7 days' as waiting_followup_claim_in_days
union all
select '2' as sno, '> 7 days' as waiting_followup_claim_in_days
) pd
LEFT JOIN (
SELECT waiting_followup_claims_days_bucket ,
       COUNT(CASE
                 WHEN followup_pending_by IN ('FCX', 'FTD', 'CH', 'CEO') THEN 1
             END) AS AT_TRUST,
       COUNT(CASE
                 WHEN followup_pending_by IN ('FCX') THEN 1
             END) AS FCX,
       COUNT(CASE
                 WHEN followup_pending_by IN ('FTD') THEN 1
             END) AS FTD,
       COUNT(CASE
                 WHEN followup_pending_by IN ('CH') THEN 1
             END) AS CH,
       COUNT(CASE
                 WHEN followup_pending_by IN ('CEO') THEN 1
             END) AS CEO,
       COUNT(CASE
                 WHEN followup_pending_by IN ('MITHRA','MEDCO') THEN 1
             END) AS AT_HOSPITAL,
       COUNT(CASE
                 WHEN followup_pending_by IN ('MEDCO') THEN 1
             END) AS MEDCO,
       COUNT(CASE
                 WHEN followup_pending_by IN ('MITHRA') THEN 1
             END) AS MITHRA
FROM
  (select   * from 
(select  
 case when followup_status in ('CD67','CD70') then 'MEDCO'
	when followup_status = 'CD62' then 'MITHRA'
	when followup_status = 'CD63' then 'FCX'
	when followup_status in  ('CD64','CD132') then 'FTD'
	when followup_status in ('CD65','CD131') then 'CH'
	when followup_status in ('CD68') then 'CEO'
	else '' end as followup_pending_by, 
                   CASE
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)<=7 then '<= 7 days'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)>7 then '> 7 days'
                       ELSE ''
                   END AS waiting_followup_claims_days_bucket
from
( select case_id ,CASE_HOSP_CODE,case_patient_no, case_regn_date , cs_preauth_dt, preauth_fwd_dt, case_status ,lst_upd_usr,patient_scheme,preauth_total_package_amt  from dwh_ehs.ehf_case_ft where patient_scheme='CD501' ) ec 
inner join (select case_followup_id ,SPLIT_PART(case_followup_id, '/', 1) AS case_id,followup_status,lst_upd_dt  from dwh_ehs.ehf_case_followup_claim_dm 
			 where  followup_status in ('CD62','CD63','CD64','CD132','CD67','CD65','CD68','CD131','CD70')) ef  on ef.case_id = ec.case_id
)
where followup_pending_by is not null) AS virtual_table
GROUP BY waiting_followup_claims_days_bucket
ORDER BY AT_TRUST DESC
)ecp on ecp. waiting_followup_claims_days_bucket = pd.waiting_followup_claim_in_days
ORDER BY sno
);




create materialized view dwh_ehs.ehs_chronic_op_claims_pending_mv as
select *,
 CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select pd.waiting_chronic_op_in_days as waiting_chronic_op_claims, COALESCE(AT_TRUST,0) AS AT_TRUST, COALESCE(COEX,0) AS COEX, COALESCE(COTD,0) AS COTD, COALESCE(COCH,0) AS COCH, COALESCE(CEO,0) AS CEO, 
COALESCE(AT_HOSPITAL,0) AS AT_HOSPITAL,COALESCE(MEDCO,0) AS MEDCO, COALESCE(MITHRA,0) AS MITHRA
from 
(select '1' as sno, '<= 7 days' as waiting_chronic_op_in_days
union all
select '2' as sno, '> 7 days' as waiting_chronic_op_in_days
) pd
LEFT JOIN (
SELECT waiting_chronic_op_days_bucket ,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('COEX', 'COTD', 'COCH', 'CEO') THEN 1
             END) AS AT_TRUST,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('COEX') THEN 1
             END) AS COEX,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('COTD') THEN 1
             END) AS COTD,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('COCH') THEN 1
             END) AS COCH,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('CEO') THEN 1
             END) AS CEO,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('Mithra','Medco') THEN 1
             END) AS AT_HOSPITAL,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('Medco') THEN 1
             END) AS MEDCO,
       COUNT(CASE
                 WHEN chronic_op_pending_by IN ('Mithra') THEN 1
             END) AS MITHRA
FROM
(SELECT DISTINCT *,
CASE
	  WHEN waiting_chronic_op_in_days<=7 then '<= 7 days'
      WHEN waiting_chronic_op_in_days>7 then '> 7 days'
      ELSE ''
      END AS waiting_chronic_op_days_bucket
from
(select distinct
chronic_id ,chronic_status,cmb_dtl_name,
case when chronic_status in ('CD407','CD410') then 'Medco'
	when chronic_status in ('CD402','CD403','CD12000') then 'Mithra'
	when chronic_status ='CD404' then 'COEX'
	when chronic_status = 'CD405' then 'COTD'
	when chronic_status in ('CD406','CD408') then 'COCH'
	when chronic_status in ('CD409') then 'CEO'
	else '' end as chronic_op_pending_by, lst_upd_dt as latest_updated_date,
ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 1) waiting_chronic_op_in_days
from 
(select chronic_id , chronic_status, lst_upd_dt from dwh_ehs.ehf_chronic_case_dtls_dm 
where chronic_status in ('CD402','CD403','CD12000','CD404','CD405','CD406','CD407','CD408','CD409','CD410')) cd 
left join ( SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = cd.chronic_status)) AS virtual_table
GROUP BY waiting_chronic_op_days_bucket
ORDER BY AT_TRUST DESC
)ecp on ecp. waiting_chronic_op_days_bucket = pd.waiting_chronic_op_in_days
ORDER BY sno
);


   
create materialized view dwh_ehs.chronic_op_report_mv as
select  * from 
(SELECT 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM chronic_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM chronic_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM chronic_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM chronic_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM chronic_regn_date) + 1, 100), 'FM00') END) AS FYear,
     CASE 
        WHEN EXTRACT(MONTH FROM chronic_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM chronic_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM chronic_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM chronic_regn_date) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null
    END AS chronic_regn_date_quarter,
 CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt,
ech.chronic_id, hosp_code,hosp_name as hospital_name,hosp_dist as hospital_district_code, h_d.loc_name as hospital_district,h_d.loc_parnt_id as hospital_state_name, h_s.loc_name as hospital_state, hosp_active_yn, nabh_flg,hospital_type,
chronic_status, cmb_dtl_name as chronic_status_name, chronic_regn_date, pck_appv_amt, tot_pckg_amt ,claim_amount, clm_sub_dt, consultation_amt,
card_no as patient_card_no, FAMILY_CARD_NO as PATIENT_FAMILY_CARD_NO,ecp.name as patient_name,relation,
case when relation=0 then 0
else '1' end as is_dependent, employee_no,
case when card_type='P' then 'Pensioner'
      when card_type='E' then 'Employee' 
end as patient_type,rl.relation_name, occupation_cd, age ,gender,ecp.post_dist as ddo_dist_code,ddo_patient_district,ddo_state_name as ddo_patient_state_name, district_code as patient_district_code, p_d.loc_name as patient_district,p_d.loc_parnt_id as patient_state_code, p_s.loc_name as patient_state,
chronic_regn_date as chronice_case_registration_date,clm_sub_dt as claim_submitted_date,
case when card_type='P' then p_d.loc_name
	when card_type='E' then ddo_patient_district
end as patinet_district_final
FROM 
(SELECT  chronic_id, hosp_code, chronic_status, chronic_regn_date, pck_appv_amt, tot_pckg_amt,claim_amount, clm_sub_dt, consultation_amt  FROM dwh_ehs.ehf_chronic_case_dtls_dm) ech
LEFT JOIN (SELECT hosp_id, hosp_name, hosp_city, hosp_dist,nabh_flg,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type, hosp_active_yn FROM dwh_ehs.ehfm_hospitals_dm ) eh on eh.hosp_id = ech.hosp_code 
LEFT JOIN (SELECT chronic_id, card_no,
case when (POSITION('/' IN CARD_NO) - 1) != '-1' then  SUBSTRING(CARD_NO, 1, POSITION('/' IN CARD_NO) - 1)
			else CARD_NO
		end
		 AS FAMILY_CARD_NO , 
		 employee_no, name, card_type, occupation_cd, age,case when gender='M' then 'Male'
			    when gender='F' then 'Female'
			    else '' end as gender, family_head, child_yn, district_code,
          mandal_code, village_code, state, relation, post_dist
From dwh_ehs.ehf_chronic_patient_dtls_dm 
 ) ecp on ecp.chronic_id = ech.chronic_id
LEFT JOIN (SELECT loc_id, loc_name, loc_parnt_id FROM dwh_ehs.ehfm_locations_dm ) h_d ON h_d.loc_id  = eh.hosp_dist
LEFT JOIN (SELECT loc_id, loc_name, loc_parnt_id FROM dwh_ehs.ehfm_locations_dm ) h_s ON h_s.loc_id  = h_d.loc_parnt_id
LEFT JOIN (SELECT loc_id, loc_name, loc_parnt_id FROM dwh_ehs.ehfm_locations_dm ) p_d ON p_d.loc_id  = ecp.district_code
LEFT JOIN (SELECT loc_id, loc_name, loc_parnt_id FROM dwh_ehs.ehfm_locations_dm ) p_s ON p_s.loc_id  = p_d.loc_parnt_id
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = ech.chronic_status
left join (select loc_id,loc_name as ddo_patient_district, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_l on ddo_l.loc_id = ecp.post_dist
left join (select loc_id,loc_name AS ddo_state_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_ls on ddo_ls.loc_id = ddo_l.loc_parnt_id
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = ecp.relation
);




create materialized view dwh_ehs.bifurcation_emp_pen_case_details_mv as
select * ,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select    
card_no as patient_card_no,
case when card_type = 'P' then 'Pensioner'
	when card_type='E' then 'Employee'
	else ' 'end as patient_card_type,
case when patient_ipop = 'OP' then 'Out Patient'
	 when patient_ipop = 'IP' then 'In Patient'
	 when patient_ipop = 'RG' then 'Registered Patient'
	 when patient_ipop = 'REF' then 'Referred Patient'
else ' ' end as patient_type, 
reg_hosp_date as patient_registered_date,  ddo_l.loc_name as ddo_district_name, ddo_state_name,
case when DATE(reg_hosp_date)>='2013-12-05' and DATE(reg_hosp_date)<='2014-06-01' then 'Before Bifurcation'
	when DATE(reg_hosp_date)>='2014-06-02' then 'After Bifurcation' end as Bifurcation_status, 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM reg_hosp_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date) + 1, 100), 'FM00') END) AS FYear,
case_id, case_no,case_hosp_code, case_patient_no, case_status as case_status_code,cmb.cmb_dtl_name as case_status, case_regn_date as case_registered_date, lst_upd_dt as case_latest_update_date,cs_dis_dt as case_discharged_date,cs_surg_dt as case_surgery_date
from 
(select case_id,case_no,case_hosp_code, case_patient_no, case_status,case_regn_date, lst_upd_dt,claim_no, patient_scheme,pck_appv_amt, cs_cl_amount, cs_dis_dt, cs_dis_upd_dt,clm_sub_dt, cs_preauth_dt,preauth_aprv_dt,cs_surg_dt,cs_surg_upd_dt  from dwh_ehs.ehf_case_ft where patient_scheme='CD501') ec 
inner join (select patient_id, card_no,card_type,occupation_cd,age,gender,relation,((house_no)+ ' , '+(street)) as patient_address,district_code,mandal_code,village_code,state,patient_ipop,patient_scheme, designation, post_dist, contact_no, employee_no, reg_hosp_date  from dwh_ehs.ehf_patient_dm where card_type in ('P','E')) ep on ep.patient_id = ec.case_patient_no
left join (select loc_id,loc_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_l on ddo_l.loc_id = ep.post_dist
left join (select loc_id,loc_name AS ddo_state_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_ls on ddo_ls.loc_id = ddo_l.loc_parnt_id
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb ON cmb.cmb_dtl_id = ec.case_status
);





create materialized view dwh_ehs.ehs_followup_pending_details_mv as
select   
ec.case_id,case_followup_id,
dis_main_id as specilaity_code,dis_main_name as speciality_name,emt.icd_proc_code as procedure_code , emt.proc_name,
case_regn_date , cs_preauth_dt as PREAUTH_INITIATE_DATE, preauth_fwd_dt as PREAUTH_FORWARDED_TRUST_DATE, case_status,cmb_dtl_name as last_act_detail , 
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
lst_upd_dt as Pending_Date,ep.patient_id, patient_ipop,
 eh.hosp_id as hosp_code,hosp_name as hospital_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist as hospital_district_code,hospital_district,hospital_type,govt_hosp_type,state_code as hospital_state_code,hospital_state,hosp_city,
hospital_address,   hosp_active_yn,hosp_empnl_ref_num, hosp_empnl_date,nabh_flg,bed_strength,
case when followup_status in ('CD67','CD70') then 'MEDCO'
	when followup_status = 'CD62' then 'MITHRA'
	when followup_status = 'CD63' then 'FCX'
	when followup_status in  ('CD64','CD132') then 'FTD'
	when followup_status in ('CD65','CD131') then 'CH'
	when followup_status in ('CD68') then 'CEO'
	else '' end as followup_pending_by, 
ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0) waiting_claim_in_days,
                   CASE
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)<=7 then '<= 7 days'
                       WHEN ROUND(DATEDIFF('hour', LST_UPD_DT, GETDATE())/24.0, 0)>7 then '> 7 days'
                       ELSE ''
                   END AS waiting_followup_claims_days_bucket,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FY_case_regn,
      CASE 
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS case_regn_quarter,
            'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM lst_upd_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM lst_upd_dt) + 1, 100), 'FM00') END) AS FY_pending,
        CASE 
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM lst_upd_dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS pending_quarter,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
( select case_id ,CASE_HOSP_CODE,case_patient_no, case_regn_date , cs_preauth_dt, preauth_fwd_dt, case_status ,lst_upd_usr,patient_scheme,preauth_total_package_amt  from dwh_ehs.ehf_case_ft ) ec 
inner join (select case_followup_id ,SPLIT_PART(case_followup_id, '/', 1) AS case_id,followup_status,lst_upd_dt  from dwh_ehs.ehf_case_followup_claim_dm 
			 where  followup_status in ('CD62','CD63','CD64','CD132','CD67','CD65','CD68','CD131','CD70')) ef  on ef.case_id = ec.case_id
left JOIN (SELECT distinct case_id,asri_cat_code,icd_proc_code FROM dwh_ehs.ehf_case_therapy_dm where activeyn='Y' ) ect ON ect.case_id = ec.case_id
LEFT JOIN(SELECT distinct asri_code, icd_proc_code, proc_name FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201') emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code
LEFT JOIN (SELECT distinct dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
INNER join (select hosp_id,hosp_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,hosp_city,
			(isnull(house_no,'')|| ' ' || isnull(street,'')) as hospital_address, hosp_active_yn, hosp_empnl_date,nabh_flg,bed_strength,hosp_empnl_ref_num
			from  dwh_ehs.ehfm_hospitals_dm  where  hosp_active_yn='Y'
		 ) eh  on eh.hosp_id = ec.CASE_HOSP_CODE
left join (SELECT LOC_ID,LOC_NAME AS hospital_district	, loc_parnt_id   FROM dwh_ehs.ehfm_locations_dm) elhd on elhd.loc_id = eh.hosp_dist
left join (select loc_id,loc_name as hospital_state from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = eh.state_code	
LEFT JOIN(SELECT cmb_dtl_id,cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = ec.case_status
left join (select patient_id, patient_ipop  from dwh_ehs.ehf_patient_dm ) ep on ep.patient_id = ec.case_patient_no;





create materialized view dwh_ehs.ehs_followup_case_details_mv as
select  
case_followup_id, ec.case_id, 
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
case_no,claim_no, card_no as patient_card_no, name as patient_name,patient_id,
case when card_type = 'P' then 'Pensioner'
	when card_type='E' then 'Employee'
	else ' 'end as patient_card_type,
case when patient_ipop = 'OP' then 'Out Patient'
	 when patient_ipop = 'IP' then 'In Patient'
	 when patient_ipop = 'RG' then 'Registered Patient'
	 when patient_ipop = 'REF' then 'Referred Patient'
else ' ' end as patient_ipop_type, emp_code,
emp_hphone as patient_contact_no, ep.occupation_cd as patient_occupation,age as patient_age,patient_gender,relation_name as patient_relation,emp_caste as patient_caste, dsg.dept_designation as patient_designation,ep.reg_hosp_date as patient_reg_hosp_date,ep.post_dist as ddo_patient_district_code, ddo_l.loc_name as ddo_sto_dist,ddo_state_name, patient_address, pl_v.patient_village_name as patient_village,
case when pl_m.patient_mandal_name is not null then pl_m.patient_mandal_name
	when pl_m.patient_mandal_name is null and pl_v.patient_village_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_v.loc_parnt_id)
	end as patient_mandal,	
case when pl_d.patient_district_name is not null then pl_d.patient_district_name
	when pl_d.patient_district_name is null and pl_m.patient_mandal_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_m.loc_parnt_id)
	end as patient_district,
case when pl_s.patient_state_name is not null then pl_s.patient_state_name
	when pl_s.patient_state_name is null and pl_d.patient_district_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_d.loc_parnt_id)
	end as patient_state,nabh_flg,
ec.case_hosp_code as hospital_code, ehd.hosp_name as hospital_name,hospital_type,govt_hosp_type,ehd.hosp_dist as hospital_district_code, hl_d.hospital_district_name as hospital_district,ehd.state_code as hosital_state_code, hl_s.hospital_state_name as hospital_state, hosp_active_yn,
ec.case_status,cmb.cmb_dtl_name as case_current_status_name,ec.lst_upd_dt as case_current_status_date,claim_paid as claim_paid_amount,
emt.asri_code as Speciality_Code, esp.dis_main_name as Name_of_the_Speciality, emt.icd_proc_code as  Procedure_Code, emt.proc_name as Name_of_the_Procedure,therapy_raised_date,cs_preauth_dt as therapy_initiated_date,preauth_aprv_dt as therapy_approved_date, icd_amt as therapy_defined_amount,
ec.cs_surg_dt as surgery_date, ec.cs_surg_upd_dt as surgery_updated_date, cs_dis_dt as discharge_date, cs_dis_upd_dt as discharge_updated_date,clm_sub_dt as claim_sumbmitted_date,followup_taken_date ,actual_package as followup_package_amount, followup_status as follow_status_code, fcmb.cmb_dtl_name as followup_status,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FY_case_regn,
      CASE 
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
         WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null
    END AS case_regn_quarter,
 CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_followup_id,case_id as f_case_id, claim_paid,followup_status, lst_upd_dt, actual_package, crt_dt as followup_taken_date from dwh_ehs.ehf_case_followup_claim_dm) fc 
inner join (select case_id,case_no,case_hosp_code, case_patient_no, case_status,case_regn_date, lst_upd_dt,claim_no, patient_scheme,pck_appv_amt, cs_cl_amount, cs_dis_dt, cs_dis_upd_dt,clm_sub_dt, cs_preauth_dt,preauth_aprv_dt,cs_surg_dt,cs_surg_upd_dt  from dwh_ehs.ehf_case_ft )ec on ec.case_id = fc.f_case_id
left join (select hosp_id,hosp_name,hosp_email,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,nabh_flg,hosp_active_yn from  dwh_ehs.ehfm_hospitals_dm )ehd on ehd.hosp_id = ec.case_hosp_code 
left join (select patient_id, name,card_no,card_type,occupation_cd,age,case when gender='M' then 'Male'
			    when gender='F' then 'Female'
			    else '' end as patient_gender,relation,((house_no)+ ' , '+(street)) as patient_address,district_code,mandal_code,village_code,state,patient_ipop,designation, post_dist, contact_no, employee_no,caste,reg_hosp_date  from dwh_ehs.ehf_patient_dm) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN (SELECT distinct  case_id,asri_cat_code,icd_proc_code,crt_dt as therapy_raised_date, lst_upd_dt FROM dwh_ehs.ehf_case_therapy_dm where activeyn='Y' ) ect ON ect.case_id = ec.case_id 
LEFT JOIN (SELECT distinct  asri_code, icd_proc_code, proc_name,icd_amt,common_cat_amt,hosp_stay_amt FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201' ) emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code 
LEFT JOIN (SELECT distinct  dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
left join ( SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) fcmb ON fcmb.cmb_dtl_id = fc.followup_status
left join ( SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = ec.case_status
left join (select distinct emp_code,emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod from 
			(select emp_code,emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod, rank() over(partition by emp_code order by crt_dt desc) as ranking from dwh_ehs.ehf_enrollment_dm )
				where ranking=1
) en on ep.employee_no = en.emp_code
left join (select loc_id,loc_name as hospital_district_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) hl_d on hl_d.loc_id = ehd.hosp_dist
left join (select loc_id,loc_name as hospital_state_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = ehd.state_code
left join (select loc_id,loc_name as patient_village_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_v on pl_v.loc_id = ep.village_code
left join (select loc_id,loc_name as patient_mandal_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_m on pl_m.loc_id = ep.mandal_code
left join (select loc_id,loc_name as patient_district_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_d on pl_d.loc_id = ep.district_code
left join (select loc_id,loc_name as patient_state_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_s on pl_s.loc_id = pl_d.loc_parnt_id
left join (select loc_id,loc_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_l on ddo_l.loc_id = ep.post_dist
left join (select loc_id,loc_name AS ddo_state_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_ls on ddo_ls.loc_id = ddo_l.loc_parnt_id
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = ep.relation
left join ( select distinct  dsgn_id, dept_designation,hod  from 
			(select *, rank() over(partition by dsgn_id order by crt_dt desc) as ranking from dwh_ehs.ehf_designation_mst_dm )
			where ranking=1
) dsg on dsg.dsgn_id = en.dept_designation and dsg.hod = en.dept_hod  ;



create materialized view dwh_ehs.mis_followup_availed_case_details as
select  
case_followup_id, ec.case_id, case_no,claim_no, card_no as patient_card_no, name as patient_name,patient_id,
case when card_type = 'P' then 'Pensioner'
	when card_type='E' then 'Employee'
	else ' 'end as patient_card_type,
case when patient_ipop = 'OP' then 'Out Patient'
	 when patient_ipop = 'IP' then 'In Patient'
	 when patient_ipop = 'RG' then 'Registered Patient'
	 when patient_ipop = 'REF' then 'Referred Patient'
else ' ' end as patient_ipop_type,
case when ec.patient_scheme='CD501' then 'EHS'
	when  ec.patient_scheme='CD502' then 'WJHS'
	else '' end as patient_scheme_type, emp_code,
emp_hphone as patient_contact_no, ep.occupation_cd as patient_occupation,age as patient_age,patient_gender,relation_name as patient_relation,emp_caste as patient_caste, en.dept as patient_department, dsg.dept_designation as patient_designation,ep.reg_hosp_date as patient_reg_hosp_date,ep.post_dist as ddo_patient_district_code, ddo_l.loc_name as ddo_sto_dist,ddo_state_name, patient_address, pl_v.patient_village_name as patient_village,
case when pl_m.patient_mandal_name is not null then pl_m.patient_mandal_name
	when pl_m.patient_mandal_name is null and pl_v.patient_village_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_v.loc_parnt_id)
	end as patient_mandal,	
case when pl_d.patient_district_name is not null then pl_d.patient_district_name
	when pl_d.patient_district_name is null and pl_m.patient_mandal_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_m.loc_parnt_id)
	end as patient_district,
case when pl_s.patient_state_name is not null then pl_s.patient_state_name
	when pl_s.patient_state_name is null and pl_d.patient_district_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_d.loc_parnt_id)
	end as patient_state,nabh_flg,
ec.case_hosp_code as hospital_code, ehd.hosp_name as hospital_name,hospital_type,govt_hosp_type,ehd.hosp_dist as hospital_district_code, hl_d.hospital_district_name as hospital_district,ehd.state_code as hosital_state_code, hl_s.hospital_state_name as hospital_state,
ec.case_status,cmb.cmb_dtl_name as case_current_status_name,ec.lst_upd_dt as case_current_status_date,claim_paid as claim_paid_amount,
emt.asri_code as Speciality_Code, esp.dis_main_name as Name_of_the_Speciality, ((emt.asri_code) || ' - ' || (esp.dis_main_name)) as speciality_desc, emt.icd_proc_code as  Procedure_Code, emt.proc_name as Name_of_the_Procedure,therapy_raised_date,cs_preauth_dt as therapy_initiated_date,preauth_aprv_dt as therapy_approved_date, icd_amt as therapy_approved_amount,
ec.cs_surg_dt as surgery_date, ec.cs_surg_upd_dt as surgery_updated_date, cs_dis_dt as discharge_date, cs_dis_upd_dt as discharge_updated_date,clm_sub_dt as claim_sumbmitted_date,followup_taken_date ,actual_package as followup_package_amount, followup_status as follow_status_code, fcmb.cmb_dtl_name as followup_status,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select case_followup_id,case_id as f_case_id, claim_paid,followup_status, lst_upd_dt, actual_package, crt_dt as followup_taken_date from dwh_ehs.ehf_case_followup_claim_dm  ) fc 
inner join (select case_id,case_no,case_hosp_code, case_patient_no, case_status,case_regn_date, lst_upd_dt,claim_no, patient_scheme,pck_appv_amt, cs_cl_amount, cs_dis_dt, cs_dis_upd_dt,clm_sub_dt, cs_preauth_dt,preauth_aprv_dt,cs_surg_dt,cs_surg_upd_dt  from dwh_ehs.ehf_case_ft  )ec on ec.case_id = fc.f_case_id
left join (select hosp_id,hosp_name,hosp_email,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,nabh_flg from  dwh_ehs.ehfm_hospitals_dm )ehd on ehd.hosp_id = ec.case_hosp_code 
left join (select patient_id, name,card_no,card_type,occupation_cd,age,case when gender='M' then 'Male'
			    when gender='F' then 'Female'
			    else '' end as patient_gender,relation,((house_no)+ ' , '+(street)) as patient_address,district_code,mandal_code,village_code,state,patient_ipop,designation, post_dist, contact_no, employee_no,caste,reg_hosp_date  from dwh_ehs.ehf_patient_dm) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN (SELECT case_id,asri_cat_code,icd_proc_code,crt_dt as therapy_raised_date,icd_amt, lst_upd_dt FROM dwh_ehs.ehf_case_therapy_dm where activeyn='Y') ect ON ect.case_id = ec.case_id 
LEFT JOIN (SELECT  asri_code, icd_proc_code, proc_name FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201' ) emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code 
LEFT JOIN (SELECT dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
left join ( SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) fcmb ON fcmb.cmb_dtl_id = fc.followup_status
left join ( SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = ec.case_status
left join (select distinct emp_code,emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod, dept from 
			(select emp_code,emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod,dept, rank() over(partition by emp_code order by crt_dt desc) as ranking from dwh_ehs.ehf_enrollment_dm )
				where ranking=1
) en on ep.employee_no = en.emp_code
left join (select loc_id,loc_name as hospital_district_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) hl_d on hl_d.loc_id = ehd.hosp_dist
left join (select loc_id,loc_name as hospital_state_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = ehd.state_code
left join (select loc_id,loc_name as patient_village_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_v on pl_v.loc_id = ep.village_code
left join (select loc_id,loc_name as patient_mandal_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_m on pl_m.loc_id = ep.mandal_code
left join (select loc_id,loc_name as patient_district_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_d on pl_d.loc_id = ep.district_code
left join (select loc_id,loc_name as patient_state_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_s on pl_s.loc_id = ep.state
left join (select loc_id,loc_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_l on ddo_l.loc_id = ep.post_dist
left join (select loc_id,loc_name AS ddo_state_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_ls on ddo_ls.loc_id = ddo_l.loc_parnt_id
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = ep.relation
left join ( select distinct  dsgn_id, dept_designation,hod  from 
			(select *, rank() over(partition by dsgn_id order by crt_dt desc) as ranking from dwh_ehs.ehf_designation_mst_dm )
			where ranking=1
) dsg on dsg.dsgn_id = en.dept_designation and en.dept_hod = dsg.hod ;





create materialized view dwh_ehs.ehs_unique_patient_details_mv as
select *,CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
 from
(select 
up.FYear,up.total_cases, up.total_unique_patients, rs.total_readmitted_patients
from 
(select FYear,  count(case_id) as total_cases, count(distinct patient_card_no)  as total_unique_patients from
(select  
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FYear, case_regn_date,
case when  ec.patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
FAMILY_CARD_NO as patient_family_card_no,ec.case_id, case_no, case_hosp_code, reg_hosp_date,
ec.case_patient_no, card_no as patient_card_no, patient_card_type,PATIENT_VILLAGE_NAME, PATIENT_MANDAL_NAME, PATIENT_DISTRICT_NAME, PATIENT_STATE_NAME , patient_gender 
from 
(select case_id, case_no, case_hosp_code, case_patient_no, case_regn_date,cs_dis_dt,case_status as case_status_code, lst_upd_dt,patient_scheme from dwh_ehs.ehf_case_ft ) ec 
left join (select patient_id, card_no, 
			CASE 
    	  WHEN POSITION('/' IN card_no) > 0 
    	  THEN SUBSTRING(card_no, 1, POSITION('/' IN card_no) - 1)
          ELSE card_no
          end AS FAMILY_CARD_NO,
          case when card_type = 'P' then 'Pensioner'
			when card_type='E' then 'Employee'
			else ' 'end as patient_card_type,occupation_cd,age,
			case when gender='M' then 'Male'
			    when gender='F' then 'Female'
			    else '' end as patient_gender,relation,((house_no)+ ' , '+(street)) as patient_address,district_code as patient_district_code,mandal_code as patient_mandal_code,village_code as patient_village_code,state as patient_state_code,patient_ipop,patient_scheme, designation, post_dist, contact_no, employee_no, reg_hosp_date  from dwh_ehs.ehf_patient_dm 
) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS PATIENT_MANDAL_NAME
           FROM dwh_ehs.ehfm_locations_dm) al ON al.MANDAL_CODE = patient_mandal_code
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_DISTRICT_NAME, loc_parnt_id 
           FROM dwh_ehs.ehfm_locations_dm) alp ON alp.LOC_ID = ep.patient_district_code
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_STATE_NAME 
           FROM dwh_ehs.ehfm_locations_dm) als on als.loc_id = alp.loc_parnt_id
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_VILLAGE_NAME	
           FROM dwh_ehs.ehfm_locations_dm) alv ON alv.LOC_ID = ep.patient_village_code
)
group by FYear
order by 1
) up
full outer join 
(select FYear, count(distinct patient_card_no)  as total_readmitted_patients from
(select 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FYear, case_regn_date,
case when  ec.patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
FAMILY_CARD_NO as patient_family_card_no,ec.case_id, case_no, case_hosp_code, reg_hosp_date,
ec.case_patient_no, card_no as patient_card_no, patient_card_type,PATIENT_VILLAGE_NAME, PATIENT_MANDAL_NAME, PATIENT_DISTRICT_NAME, PATIENT_STATE_NAME , patient_gender, 
ROW_NUMBER() over(partition by card_no) as cnt
from 
(select case_id, case_no, case_hosp_code, case_patient_no, case_regn_date,cs_dis_dt,case_status as case_status_code, lst_upd_dt,patient_scheme from dwh_ehs.ehf_case_ft ) ec 
left join (select patient_id, card_no, 
			CASE 
    	  WHEN POSITION('/' IN card_no) > 0 
    	  THEN SUBSTRING(card_no, 1, POSITION('/' IN card_no) - 1)
          ELSE card_no
          end AS FAMILY_CARD_NO,
          case when card_type = 'P' then 'Pensioner'
			when card_type='E' then 'Employee'
			else ' 'end as patient_card_type,occupation_cd,age,
			case when gender='M' then 'Male'
			    when gender='F' then 'Female'
			    else '' end as patient_gender,relation,((house_no)+ ' , '+(street)) as patient_address,district_code as patient_district_code,mandal_code as patient_mandal_code,village_code as patient_village_code,state as patient_state_code,patient_ipop,patient_scheme, designation, post_dist, contact_no, employee_no, reg_hosp_date  from dwh_ehs.ehf_patient_dm
			    ) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS PATIENT_MANDAL_NAME
           FROM dwh_ehs.ehfm_locations_dm) al ON al.MANDAL_CODE = patient_mandal_code
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_DISTRICT_NAME, loc_parnt_id 
           FROM dwh_ehs.ehfm_locations_dm) alp ON alp.LOC_ID = ep.patient_district_code
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_STATE_NAME 
           FROM dwh_ehs.ehfm_locations_dm) als on als.loc_id = alp.loc_parnt_id
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_VILLAGE_NAME	
           FROM dwh_ehs.ehfm_locations_dm) alv ON alv.LOC_ID = ep.patient_village_code
)
where cnt>1
group by FYear
order by 1
) rs on rs.FYear = up.FYear
order by up.FYear
)
where FYear is not null;









create materialized view dwh_ehs.ehs_followup_tracking_mv as
select  
ec.case_id, case_no,claim_no, 
case when  patient_scheme='CD501' then 'EHS' else 'WJHS' end as patient_scheme,
card_no as patient_card_no, name as patient_name,patient_id,
case when card_type = 'P' then 'Pensioner'
	when card_type='E' then 'Employee'
	else ' 'end as patient_card_type,
case when patient_ipop = 'OP' then 'Out Patient'
	 when patient_ipop = 'IP' then 'In Patient'
	 when patient_ipop = 'RG' then 'Registered Patient'
	 when patient_ipop = 'REF' then 'Referred Patient'
else ' ' end as patient_ipop_type, emp_code,
emp_hphone as patient_contact_no, ep.occupation_cd as patient_occupation,age as patient_age,patient_gender,relation_name as patient_relation,emp_caste as patient_caste, dsg.dept_designation as patient_designation,ep.reg_hosp_date as patient_reg_hosp_date,ep.post_dist as ddo_patient_district_code, ddo_l.loc_name as ddo_sto_dist,ddo_state_name, patient_address, pl_v.patient_village_name as patient_village,
case when pl_m.patient_mandal_name is not null then pl_m.patient_mandal_name
	when pl_m.patient_mandal_name is null and pl_v.patient_village_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_v.loc_parnt_id)
	end as patient_mandal,	
case when pl_d.patient_district_name is not null then pl_d.patient_district_name
	when pl_d.patient_district_name is null and pl_m.patient_mandal_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_m.loc_parnt_id)
	end as patient_district,
case when pl_s.patient_state_name is not null then pl_s.patient_state_name
	when pl_s.patient_state_name is null and pl_d.patient_district_name is not null then (select loc_name from dwh_ehs.ehfm_locations_dm where loc_id=pl_d.loc_parnt_id)
	end as patient_state,nabh_flg,
ec.case_hosp_code as hospital_code, ehd.hosp_name as hospital_name,hospital_type,govt_hosp_type,ehd.hosp_dist as hospital_district_code, hl_d.hospital_district_name as hospital_district,ehd.state_code as hosital_state_code, hl_s.hospital_state_name as hospital_state,
ec.case_status,cmb.cmb_dtl_name as case_current_status_name,ec.lst_upd_dt as case_current_status_date,
emt.asri_code as Speciality_Code, esp.dis_main_name as Name_of_the_Speciality, emt.icd_proc_code as  Procedure_Code, emt.proc_name as Name_of_the_Procedure,therapy_raised_date,cs_preauth_dt as therapy_initiated_date,preauth_aprv_dt as therapy_approved_date, icd_amt as therapy_approved_amount,
ec.cs_surg_dt as surgery_date, ec.cs_surg_upd_dt as surgery_updated_date, cs_dis_dt as discharge_date, cs_dis_upd_dt as discharge_updated_date,clm_sub_dt as claim_sumbmitted_date,
first_followup_yn,second_followup_yn,third_followup_yn,fourth_followup_yn,
     'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM case_regn_date) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM case_regn_date) + 1, 100), 'FM00') END) AS FY_case_regn,
      CASE 
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
          WHEN EXTRACT(MONTH FROM case_regn_date) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null
    END AS case_regn_quarter,
 CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select fc.f_case_id,
	   SUM(case when followup_number = 1 then 1 
	   		else 0
	   		end) first_followup_yn,
	   SUM(case when followup_number = 2 then 1 
	   		else 0
	   		end) second_followup_yn,	
	   SUM(case when followup_number = 3 then 1 
	   		else 0
	   		end) third_followup_yn,	
	   SUM(case when followup_number = 4 then 1 
	   		else 0
	   		end) fourth_followup_yn
from
(select distinct  case_followup_id,split_part(case_followup_id,'/',2) as followup_number, case_id as f_case_id, claim_paid,followup_status, lst_upd_dt, actual_package, crt_dt as followup_taken_date from dwh_ehs.ehf_case_followup_claim_dm) fc 
group by f_case_id) fc 
inner join (select case_id,case_no,case_hosp_code, case_patient_no, case_status,case_regn_date, lst_upd_dt,claim_no, patient_scheme,pck_appv_amt, cs_cl_amount, cs_dis_dt, cs_dis_upd_dt,clm_sub_dt, cs_preauth_dt,preauth_aprv_dt,cs_surg_dt,cs_surg_upd_dt  from dwh_ehs.ehf_case_ft )ec on ec.case_id = fc.f_case_id
left join (select hosp_id,hosp_name,hosp_email,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,nabh_flg,hosp_active_yn from  dwh_ehs.ehfm_hospitals_dm )ehd on ehd.hosp_id = ec.case_hosp_code 
left join (select patient_id, name,card_no,card_type,occupation_cd,age,case when gender='M' then 'Male'
			    when gender='F' then 'Female'
			    else '' end as patient_gender,relation,((house_no)+ ' , '+(street)) as patient_address,district_code,mandal_code,village_code,state,patient_ipop,designation, post_dist, contact_no, employee_no,caste,reg_hosp_date  from dwh_ehs.ehf_patient_dm) ep on ep.patient_id = ec.case_patient_no
LEFT JOIN (SELECT distinct  case_id,asri_cat_code,icd_proc_code,crt_dt as therapy_raised_date, lst_upd_dt FROM dwh_ehs.ehf_case_therapy_dm where activeyn='Y' ) ect ON ect.case_id = ec.case_id 
LEFT JOIN (SELECT distinct  asri_code, icd_proc_code, proc_name,icd_amt,common_cat_amt,hosp_stay_amt FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201' ) emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code 
LEFT JOIN (SELECT distinct  dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
left join ( SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = ec.case_status
left join (select distinct emp_code,emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod from 
			(select emp_code,emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod, rank() over(partition by emp_code order by crt_dt desc) as ranking from dwh_ehs.ehf_enrollment_dm )
				where ranking=1
) en on ep.employee_no = en.emp_code
left join (select loc_id,loc_name as hospital_district_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) hl_d on hl_d.loc_id = ehd.hosp_dist
left join (select loc_id,loc_name as hospital_state_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = ehd.state_code
left join (select loc_id,loc_name as patient_village_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_v on pl_v.loc_id = ep.village_code
left join (select loc_id,loc_name as patient_mandal_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_m on pl_m.loc_id = ep.mandal_code
left join (select loc_id,loc_name as patient_district_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_d on pl_d.loc_id = ep.district_code
left join (select loc_id,loc_name as patient_state_name,loc_parnt_id from dwh_ehs.ehfm_locations_dm ) pl_s on pl_s.loc_id = pl_d.loc_parnt_id
left join (select loc_id,loc_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_l on ddo_l.loc_id = ep.post_dist
left join (select loc_id,loc_name AS ddo_state_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_ls on ddo_ls.loc_id = ddo_l.loc_parnt_id
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = ep.relation
left join ( select distinct  dsgn_id, dept_designation,hod  from 
			(select *, rank() over(partition by dsgn_id order by crt_dt desc) as ranking from dwh_ehs.ehf_designation_mst_dm )
			where ranking=1
) dsg on dsg.dsgn_id = en.dept_designation  and dsg.hod = en.dept_hod  ;






create materialized view dwh_ehs.ehs_empanelled_hosp_list_mv as 
select  
eh.hosp_id,hosp_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist as hospital_district_code,HOSPITAL_DISTRICT_NAME,hospital_type,govt_hosp_type,state_code,HOSPITAL_STATE_NAME,hosp_city,
hospital_address, hosp_active_yn, hosp_empnl_ref_num, hosp_empnl_date,nabh_flg,bed_strength, es.icd_cat_code as speciality_code, esp.dis_main_name  as speciality_name, 
((es.icd_cat_code) || ' - ' || (esp.dis_main_name)) as speciality_desc,
case when icd_cat_code='S18' then 1 else 0 end as is_dental_surgery,
hosp_md_ceo_name , hosp_md_email , hosp_md_tel_ph,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM hosp_empnl_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM hosp_empnl_date) + 1, 100), 'FM00') END) AS FY_hosp_empanelled,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select hosp_id,hosp_name,hosp_email,hosp_contact_person,hosp_contact_no,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,hosp_city,
(isnull(house_no,'')|| ' ' || isnull(street,'')) as hospital_address, hosp_active_yn, hosp_empnl_date,nabh_flg,bed_strength,hosp_empnl_ref_num
from  dwh_ehs.ehfm_hospitals_dm where hosp_active_yn='Y' ) eh 
left join (select distinct hosp_id,icd_cat_code  from dwh_ehs.ehfm_hosp_speciality_dm where is_active_flg='Y' ) es on es.hosp_id = eh.hosp_id
LEFT JOIN (select distinct  dis_main_id,dis_main_name FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = es.icd_cat_code
left join (SELECT LOC_ID,LOC_NAME AS hospital_district_name	, loc_parnt_id 
           FROM dwh_ehs.ehfm_locations_dm) elhd on elhd.loc_id = eh.hosp_dist
left join (select loc_id,loc_name as hospital_state_name from dwh_ehs.ehfm_locations_dm ) hl_s on hl_s.loc_id = eh.state_code
left join (select distinct  hospinfo_id, hosp_md_ceo_name , hosp_md_email,hosp_md_tel_ph   from dwh_ehs.ehf_empnl_hospinfo_dm)  ehe on eh.hosp_empnl_ref_num = ehe.hospinfo_id;




create materialized view dwh_ehs.MR_AUDIT_mv as 
Select swa.claim_seq, case
		when current_group_id in ('GP0008','GP0001') then 'a_REX'
		when current_group_id in ('GP0003') then 'c_CPD'
		when current_group_id in ('GP0002') then 'b_DYEO-TECHNICAL'
		when current_group_id in ('GP0007') then 'd_JEO-MR'
		when current_group_id in ('GP701') then 'e_EO-EHS'
		when current_group_id in ('GP0004') then 'g_CEO-MR'
		else null
	end as Role_Name, user_name,case
		when case_status in ('CD610', 'CD006', 'CD005', 'CD004', 'CD606', 'CD003', 'CD034', 'CD593', 'CD598', 'CD604', 'CD592', 'CD605', 'CD599',  'CD00020', 'CD595', 'CD592')then 'offline' else 'Online' end as mr_type,
	   case
		when case_status in ('CD006','CD0011') then 'b_Approved'
		when case_status in ('CD610','CD005','CD004','CD606','CD003','CDAB0002','CDAP003','CD604','CD605','CD0006','CD0007','CD00045','CD00040','CD0009','CD0006','CD00050') then 'a_Verified'
		when case_status in ('CDAB0003','CD593','CD598','CDAP004','CD592','CD0013','CDAB0008','CD599','CD00020','CDAPDCL4','CD00039','CD00026','CD0008','CD0010','CD00031','CD00021','CD00038') then 'c_Pending'
		else null end as case_status,
		crt_dt as crt_dt,
	   'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM crt_dt) <= 3 
       THEN TO_CHAR(MOD(EXTRACT(YEAR FROM crt_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM crt_dt), 100), 'FM00')
       ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM crt_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM crt_dt) + 1, 100), 'FM00') END) AS crt_dt_f_YEAR
  from (select current_group_id,next_group_id,crt_dt,crt_usr,case_status,claim_seq from dwh_ehs.scheme_workflow_audit_dm) swa
left join (select cmb_dtl_id,cmb_dtl_name as case_status_name
		   from dwh_ehs.ehfm_cmb_dtls_cd) ecd on swa.case_status = ecd.cmb_dtl_id
left join (select cmb_dtl_id,cmb_dtl_name as group_name
		   from dwh_ehs.ehfm_cmb_dtls_cd) ecd2 on swa.current_group_id = ecd2.cmb_dtl_id
left join (select cmb_dtl_id,cmb_dtl_name as case_status_name_a
		   from dwh_ehs.asrim_combo_ehs
		   where cmb_hdr_id = 'CH001') ecd3 on swa.case_status = ecd3.cmb_dtl_id
left join (select user_id,(isnull(first_name,'') + ' '+ isnull(middle_name,'') + ' ' + isnull(last_name,'')) as user_name
		   from dwh_ehs.ehfm_users_dm) eud on swa.crt_usr = eud.user_id
left join (select grp_id,grp_name
		   from dwh_ehs.ehfm_grps_dm) egd on swa.current_group_id = egd.grp_id
left join (select grp_id,grp_name as next_role_name
		   from dwh_ehs.ehfm_grps_dm) egd2 on swa.next_group_id = egd2.grp_id
left join (select claim_seq, 'online' as mr_type
		   from dwh_ehs.trn_onlinecr_patientdtls_dm 
		   union 
		   select claim_seq, 'offline' as mr_type
		   from dwh_ehs.trn_reimbursement_patientdtls_dm) tpd on swa.claim_seq = tpd.claim_seq;
		  
	
		  

create materialized view dwh_ehs.ehs_emp_pen_enroll_details_mv as 
select   
eef.enroll_id,eef.enroll_name,en.enroll_prnt_id as household_enroll_id, eef.aadhar_id, eef.enroll_sno, eef.enroll_dob, eef.blood_group,  eef.ehf_card_no,
case when enroll_gender='M' then 'Male' when enroll_gender='F' then 'Female' else '' end as gender, eef.enroll_status as enroll_status_code, ace.cmb_dtl_name as enroll_status,
eef.enroll_relation_code, relation_name as enroll_relation,
en.emp_code, en.emp_hphone as emp_phno, en.emp_hemail as emp_email, dsg.dept_designation as emp_dept_designation,en.post_dist as ddo_district_code, emp_ddo_district,en.emp_hmand_munci as emp_municipality_code,emp_municipality ,en.emp_hdist as emp_district_code, emp_district, en_d.loc_parnt_id as emp_state_code, emp_state,
emp_type as employee_type_code, cmb.cmb_dtl_name as employee_type, case when NVL(eef.enroll_sno::text, eef.enroll_relation_code )=0 then 0 when NVL(eef.enroll_sno::text, eef.enroll_relation_code) is null then null  else 1 end as is_dependent,
case when eef.enroll_status in ('CD3020','CD3016') then 1 else 0 end as is_card_generated, DATE(enrolled_date) as enrolled_date,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select distinct emp_code,enroll_prnt_id, emp_hphone, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod, emp_marital_stat,post_dist,emp_hstate,emp_hmand_munci, emp_type,emp_hdist,crt_dt from 
			(select emp_code,emp_hphone,enroll_prnt_id, emp_marital_stat, emp_hemail, serv_dsgn, dept_designation,emp_caste,dept_hod,post_dist,emp_hdist,emp_hstate,emp_hmand_munci,emp_type,crt_dt, rank() over(partition by emp_code order by crt_dt desc) as ranking from dwh_ehs.ehf_enrollment_dm )
				where ranking=1 
) en
inner join (select aadhar_id ,ehf_card_no, enroll_id , enroll_sno , blood_group, enroll_name ,enroll_gender ,enroll_status , enroll_prnt_id ,enroll_dob, enroll_relation as enroll_relation_code,crt_dt as enrolled_date from rawdata_ehs.ehf_enrollment_family) eef on eef.enroll_prnt_id = en.enroll_prnt_id
--where eef.enroll_prnt_id is null order by en.crt_dt desc
left join (select cmb_dtl_id , cmb_dtl_name  from dwh_ehs.asrim_combo_ehs) ace on ace.cmb_dtl_id = eef.enroll_status
LEFT JOIN(SELECT cmb_dtl_id,cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd) cmb ON cmb.cmb_dtl_id = en.emp_type
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = eef.enroll_relation_code
left join ( select distinct  dsgn_id, dept_designation,hod  from 
			(select *, rank() over(partition by dsgn_id order by crt_dt desc) as ranking from dwh_ehs.ehf_designation_mst_dm )
			where ranking=1
) dsg on dsg.dsgn_id = en.dept_designation  and dsg.hod = en.dept_hod 
left join (select loc_id,loc_name as emp_ddo_district, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_d on ddo_d.loc_id = en.post_dist
left join (select loc_id,loc_name as emp_district, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) en_d on en_d.loc_id = en.emp_hdist
left join (select loc_id,loc_name as emp_state, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) en_s on en_s.loc_id = en_d.loc_parnt_id
left join (select loc_id,loc_name as emp_municipality, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) en_m on en_m.loc_id = en.emp_hmand_munci ;





create materialized view dwh_ehs.ehs_jrnlst_enroll_details_mv as 
select 
journal_enroll_id,journal_enroll_prnt_id as household_enroll_id,  aadhar_id,journal_card_no,journal_enroll_sno,case when NVL(jf.journal_enroll_sno::text, jf.enroll_relation_code )=0 then 0 when NVL(jf.journal_enroll_sno::text, jf.enroll_relation_code ) is null then null  else 1 end as is_dependent, name, case when gender='M' then 'Male' when gender='F' then 'Female' else '' end as gender ,enroll_status as enroll_status_code , cmb_dtl_name as enroll_status, enroll_relation_code, relation_name as relation_name, DATE(enrolled_date) as enrolled_date,
case when jf.enroll_status ='CD6105' then 1 else 0 end as is_card_generated, CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from 
(select aadhar_id,journal_card_no,journal_enroll_id,journal_enroll_sno, name, gender ,enroll_status, journal_enroll_prnt_id,dob::timestamp, relation as enroll_relation_code, crt_dt::timestamp as enrolled_date from dwh_ehs.ehf_jrnlst_family_dm) jf 
left join (select cmb_dtl_id , cmb_dtl_name  from dwh_ehs.ehfm_cmb_dtls_cd ) ace on ace.cmb_dtl_id = jf.enroll_status
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = jf.enroll_relation_code;

		  
		  

create materialized view dwh_ehs.wt_case_preauth_claim_details_mv as
select *,
case when rwn=1 then preauth_initiated_amount else 0 end as case_preauth_initiated_amount,
case when rwn=1 then preauth_approved_amount else 0 end as case_preauth_approved_amount,
case when rwn=1 then claim_submitted_amount else 0 end  as case_claim_submitted_amount,
case when rwn=1 then claim_approved_amount else 0 end  as case_claim_approved_amount,
case when rwn=1 then claim_paid_amount else 0 end  as case_claim_paid_amount,
CURRENT_TIMESTAMP::TIMESTAMP as last_refreshed_dt
from
(select  
ec.case_id,esp.dis_main_id as speciality_code,esp.dis_main_name as name_of_the_speciality ,
esp.dis_active_yn as speciality_active_yn,
case when esp.dis_active_yn='N' and esp.lst_upd_dt is not null then esp.lst_upd_dt 
	when esp.dis_active_yn='N' and esp.lst_upd_dt is null then esp.crt_dt 
	else null end as speciality_end_date,
emt.icd_proc_code as procedure_code, emt.proc_name as name_of_the_procedure,NVL(emt.icd_amt,emt.common_cat_amt,emt.hosp_stay_amt) as procedure_defined_amount,
((emt.asri_code) || ' - ' || (esp.dis_main_name)) as speciality_desc, therapy_raised_date,
case when emt.medical_surg='S' then 'Surgical' when emt.medical_surg='M' then 'Medical' end as procedure_type,
emt.active_yn as procedure_active_yn,
case when emt.active_yn='N' and emt.lst_upd_dt is not null then emt.lst_upd_dt 
	when emt.active_yn='N' and emt.lst_upd_dt is null then emt.crt_dt 
	else null end as procedure_end_date,
case when ec.patient_scheme='CD501' then 'EHS'
	when  ec.patient_scheme='CD502' then 'WJHS'
	else '' end as patient_scheme_type,
ec.case_status, cmb1.cmb_dtl_name as latest_case_status , ec.lst_upd_dt as case_latest_update_date,
case when ec.cs_preauth_dt is not null then 1 else 0 end as is_preauth_initiated,
ec.cs_preauth_dt as  preauth_initiated_date, 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.cs_preauth_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_preauth_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_preauth_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_preauth_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_preauth_dt) + 1, 100), 'FM00') END) AS FY_Preauth_Initiated_Date,
 TO_CHAR(ec.cs_preauth_dt, 'Month') as preauth_initiated_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.cs_preauth_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.cs_preauth_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.cs_preauth_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null
    END AS preauth_initiated_quarter,
preauth_total_package_amt as preauth_initiated_amount,
case when ec.preauth_aprv_dt is not null then 1 else 0 end as is_preauth_approved, 
ec.preauth_aprv_dt as preauth_approved_date, 
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.preauth_aprv_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.preauth_aprv_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.preauth_aprv_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.preauth_aprv_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.preauth_aprv_dt) + 1, 100), 'FM00') END) AS fy_preauth_apprv_date,
 TO_CHAR(ec.preauth_aprv_dt, 'Month') as preauth_approved_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.preauth_aprv_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.preauth_aprv_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.preauth_aprv_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null    END AS preauth_approved_quarter,
case when preauth_total_package_amt>=200000 and aud3.case_id is not null then aud3.apprv_amt
else pck_appv_amt end as preauth_approved_amount,
ec.case_regn_date ,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.case_regn_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.case_regn_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.case_regn_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.case_regn_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.case_regn_date) + 1, 100), 'FM00') END) AS FY_Case_Registered_Date,
 TO_CHAR(ec.case_regn_date, 'Month') as case_regn_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.case_regn_date) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.case_regn_date) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.case_regn_date) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null    END AS case_regn_quarter,
ec.surg_count, ec.cs_surg_dt as case_surgery_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.cs_surg_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_surg_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_surg_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_surg_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_surg_dt) + 1, 100), 'FM00') END) AS FY_Case_Surgery_Date,
 TO_CHAR(ec.cs_surg_dt, 'Month') as fy_case_surgery_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.cs_surg_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.cs_surg_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.cs_surg_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     END AS case_surg_quarter,
case when ec.cs_death_dt is not null then 1 else 0 end as is_dead,
ec.cs_death_dt as death_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.cs_death_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_death_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_death_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_death_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_death_dt) + 1, 100), 'FM00') END) AS FY_death_date,
 TO_CHAR(ec.cs_death_dt, 'Month') as death_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.cs_death_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.cs_death_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.cs_death_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     END AS death_quarter,
ec.cs_dis_dt as case_discharge_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.cs_dis_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_dis_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_dis_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_dis_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.cs_dis_dt) + 1, 100), 'FM00') END) AS FY_Case_discharge_Date,
 TO_CHAR(ec.cs_dis_dt, 'Month') as case_discharge_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.cs_dis_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.cs_dis_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.cs_dis_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     END AS case_discharge_quarter,
case when clm_sub_dt is not null then 1 else 0 end as is_claim_submitted,clm_sub_dt  as Claim_submitted_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM ec.clm_sub_dt) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.clm_sub_dt), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM ec.clm_sub_dt), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM ec.clm_sub_dt) + 1, 100), 'FM00') END) AS fy_claim_submit_date,
 TO_CHAR(ec.clm_sub_dt, 'Month') as claim_submitted_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     END AS claim_submitted_quarter,
case when acc.case_id is not null then acc.claim_bill_amt else null end as claim_submitted_amount,
case when aud.act_id = 'CD94' or ec.case_status in ('CD94','CD51') then 1 else 0 end as is_claim_approved,
case when aud.act_id = 'CD94'  then aud.crt_dt when ec.case_status='CD94' then ec.lst_upd_dt  else null end as claim_approved_date,
case when aud.act_id = 'CD94' or ec.case_status in ('CD94','CD51') then ec.cs_cl_amount else 0  end as claim_approved_amount,
case when aud.act_id = 'CD94' or ec.case_status='CD94'  then 
				'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM NVL(aud.crt_dt,ec.lst_upd_dt)) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud.crt_dt,ec.lst_upd_dt)) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud.crt_dt,ec.lst_upd_dt)), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud.crt_dt,ec.lst_upd_dt)), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud.crt_dt,ec.lst_upd_dt)) + 1, 100), 'FM00') END) 
else null end as fy_claim_approved_date,
 case when aud.act_id = 'CD94'  then  TO_CHAR(aud.crt_dt, 'Month') 
	 when ec.case_status='CD94' then TO_CHAR(ec.lst_upd_dt, 'Month')
 else null end as claim_approved_month,
case when aud.act_id = 'CD94' or ec.case_status='CD94'  then 
	(CASE 
        WHEN EXTRACT(MONTH FROM NVL(aud.crt_dt,ec.lst_upd_dt)) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM NVL(aud.crt_dt,ec.lst_upd_dt)) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM NVL(aud.crt_dt,ec.lst_upd_dt)) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     end)
  else null end as claim_approved_quarter,
case when aud2.act_id='CD51' or ec.case_status='CD51' then 1 else 0 end as is_claim_paid,
NVL(aud2.crt_dt,ec.lst_upd_dt) as claim_paid_date, 
case when aud2.act_id = 'CD51' or ec.case_status='CD51'  then 
				'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM NVL(aud2.crt_dt,ec.lst_upd_dt)) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud2.crt_dt,ec.lst_upd_dt)) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud2.crt_dt,ec.lst_upd_dt)), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud2.crt_dt,ec.lst_upd_dt)), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM NVL(aud2.crt_dt,ec.lst_upd_dt)) + 1, 100), 'FM00') END) 
else null end as fy_claim_paid_date,
 case when aud2.act_id = 'CD51'  then  TO_CHAR(aud2.crt_dt, 'Month') 
	 when ec.case_status='CD51' then TO_CHAR(ec.lst_upd_dt, 'Month')
 else null end as fy_claim_paid_month,
case when aud2.act_id = 'CD51' or ec.case_status='CD51'  then 
	(CASE 
        WHEN EXTRACT(MONTH FROM NVL(aud2.crt_dt,ec.lst_upd_dt)) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM NVL(aud2.crt_dt,ec.lst_upd_dt)) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM NVL(aud2.crt_dt,ec.lst_upd_dt)) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     end)
  else null end as claim_paid_quarter,
ec.cs_cl_amount as claim_paid_amount,
case when ec.cs_cl_amount < 50000 then '0 to 0.5 Lakh'
     when ec.cs_cl_amount BETWEEN 50000 AND 100000 then '0.5 to 1 Lakh'
     when ec.cs_cl_amount BETWEEN 100000 AND 150000 then '1 to 1.5 Lakhs'
     when ec.cs_cl_amount BETWEEN 150000 AND 200000 then '1.5 to 2 Lakhs'
     when ec.cs_cl_amount BETWEEN 200000 AND 250000 then '2 to 2.5 Lakhs'
     when ec.cs_cl_amount BETWEEN 250000 AND 300000 then '2.5 to 3 Lakhs'
     when ec.cs_cl_amount BETWEEN 300000 AND 350000 then '3 to 3.5 Lakhs'
     when ec.cs_cl_amount BETWEEN 350000 AND 400000 then '3.5 to 4 Lakhs'
     when ec.cs_cl_amount BETWEEN 400000 AND 450000 then '4 to 4.5 Lakhs'
     when ec.cs_cl_amount BETWEEN 450000 AND 500000 then '4.5 to 5 Lakhs'
     when ec.cs_cl_amount BETWEEN 500000 AND 1000000 then '5 to 10 Lakhs'
     when ec.cs_cl_amount>1000000 then  'Above 10 lakhs' end as Claim_Amt_Freq_Bucket,
--patient details
case_patient_no, card_no as patient_card_no,FAMILY_CARD_NO,name as patient_name, patient_card_type,
case when patient_ipop = 'OP' then 'Out Patient' when patient_ipop = 'IP' then 'In Patient' when patient_ipop = 'RG' then 'Registered Patient' when patient_ipop = 'REF' then 'Referred Patient'
else ' ' end as patient_type, 
case when relation=0 then 0 else '1' end as is_dependent, relation_name as patient_relation,ep.contact_no as patient_contact_no,
patient_village_code,PATIENT_VILLAGE_NAME,patient_mandal_code, PATIENT_MANDAL_NAME,post_dist as ddo_pat_district_code,ddo_l.loc_name as DDO_DISTRICT,ddo_state_name,patient_district_code, PATIENT_DISTRICT_NAME,alp.loc_parnt_id as patient_state_code, PATIENT_STATE_NAME ,
ep.age as patient_age,patient_gender,ep.enroll_dob as patient_dob,patient_address,reg_hosp_date as patient_hosp_reg_date,
'FY' || 
       (CASE WHEN EXTRACT(MONTH FROM reg_hosp_date) <= 3 
        THEN TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date) - 1, 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date), 100), 'FM00')
        ELSE TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date), 100), 'FM00') || '-' || TO_CHAR(MOD(EXTRACT(YEAR FROM reg_hosp_date) + 1, 100), 'FM00') END) AS fy_reg_hosp_date,
 TO_CHAR(reg_hosp_date, 'Month') as fy_reg_hosp_month,
      CASE 
        WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM ec.clm_sub_dt) BETWEEN 10 AND 12 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM aud.crt_dt) BETWEEN 1 AND 3 THEN 'Q4'
        ELSE Null     END AS reg_hosp_quarter,
-- hospital details
ec.case_hosp_code, hosp_name,hosp_email,hosp_dist, HOSPITAL_DISTRICT_NAME, elhd.loc_parnt_id as hosp_state_code, HOSPITAL_STATE_NAME,
hospital_type,govt_hosp_type,hospital_contact_no ,nabh_flg, hospital_address, hosp_active_yn, hosp_empnl_ref_num, hosp_empnl_date,
eeh.hosp_establish_year,eeh.hosp_md_tel_ph,eeh.hosp_md_email ,eeh.hosp_md_ceo_name ,eeh.status as hospital_status_code, ace.cmb_dtl_name as hospital_status,
ROW_NUMBER() OVER (PARTITION BY ec.case_id) as rwn
--select count(ec.case_id)
from 
(select case_id , case_hosp_code , case_patient_no , case_status , 
		case_regn_date , cs_preauth_dt , preauth_aprv_dt ,cs_surg_dt , cs_dis_dt , clm_sub_dt , lst_upd_dt , 
		preauth_total_package_amt, cs_cl_amount , surg_count,patient_scheme, pck_appv_amt, cs_death_dt
from dwh_ehs.ehf_case_ft /*where DATE(case_regn_date)>='2019-04-01' */
--where case_id='AP1335501' 
) ec 
left join (select distinct case_id , act_id , act_by , crt_dt,apprv_amt
	from
	(select case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft where act_id ='CD94'
        )
	where  ranking=1
) aud on aud.case_id = ec.case_id
left join (select distinct case_id , act_id , act_by , crt_dt,apprv_amt
	from
	(select case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft where act_id ='CD51'
        )
	where  ranking=1
) aud2 on aud2.case_id = ec.case_id
left join (select distinct case_id , act_id , act_by , crt_dt,apprv_amt
	from
	(select case_id , act_id , act_by , crt_dt , apprv_amt, RANK() OVER(PARTITION by case_id, act_id  order by crt_dt desc) as ranking
	  from dwh_ehs.ehf_audit_ft where act_id ='CD215'
        )
	where  ranking=1
) aud3 on aud3.case_id = ec.case_id
left join (select distinct case_id , claim_bill_amt  from dwh_ehs.ehf_case_claim_dm ) acc on acc.case_id = ec.case_id
LEFT JOIN (SELECT cmb_dtl_id, cmb_dtl_name FROM dwh_ehs.ehfm_cmb_dtls_cd ) cmb1 ON cmb1.cmb_dtl_id = ec.case_status
left join (select patient_id, card_no, name,
		CASE WHEN POSITION('/' IN card_no) > 0  THEN SUBSTRING(card_no, 1, POSITION('/' IN card_no) - 1) ELSE card_no end AS FAMILY_CARD_NO,
        case when card_type = 'P' then 'Pensioner' when card_type='E' then 'Employee' else ' 'end as patient_card_type,occupation_cd,age,
		case when gender='M' then 'Male' when gender='F' then 'Female' else '' end as patient_gender,relation,
		((house_no)+ '  '+(street)) as patient_address,district_code as patient_district_code,mandal_code as patient_mandal_code,village_code as patient_village_code,state as patient_state_code
		,patient_ipop,patient_scheme, designation, post_dist, contact_no, employee_no, reg_hosp_date,enroll_dob  from dwh_ehs.ehf_patient_dm
) ep on ep.patient_id = ec.case_patient_no
left join (select relation_id, relation_name from dwh_ehs.ehfm_relation_mst_dm) rl on rl.relation_id = ep.relation
LEFT JOIN (SELECT LOC_ID AS MANDAL_CODE,LGD_CODE AS MANDAL_LGD_CODE,LOC_NAME AS PATIENT_MANDAL_NAME
           FROM dwh_ehs.ehfm_locations_dm) al ON al.MANDAL_CODE = patient_mandal_code
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_DISTRICT_NAME, loc_parnt_id 
           FROM dwh_ehs.ehfm_locations_dm) alp ON alp.LOC_ID = ep.patient_district_code
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_STATE_NAME 
           FROM dwh_ehs.ehfm_locations_dm) als on als.loc_id = alp.loc_parnt_id
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS PATIENT_VILLAGE_NAME	
           FROM dwh_ehs.ehfm_locations_dm) alv ON alv.LOC_ID = ep.patient_village_code
left join (select loc_id,loc_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_l on ddo_l.loc_id = ep.post_dist
left join (select loc_id,loc_name AS ddo_state_name, loc_parnt_id from dwh_ehs.ehfm_locations_dm ) ddo_ls on ddo_ls.loc_id = ddo_l.loc_parnt_id
LEFT JOIN(SELECT hosp_id,hosp_name,hosp_email,hosp_dist,case when hosp_type='C' then 'Corporate' when hosp_type='G' then 'Government' end as hospital_type,govt_hosp_type,state_code,nabh_flg
			, NVL(hosp_contact_no,cug_no) as hospital_contact_no, ((house_no)+ '  '+(street)) as hospital_address, hosp_active_yn, hosp_empnl_ref_num, hosp_empnl_date
				FROM   dwh_ehs.ehfm_hospitals_dm
) eh ON eh.hosp_id  = ec.case_hosp_code
left join (select hospinfo_id ,hosp_establish_year,hosp_md_tel_ph,hosp_md_email , hosp_md_ceo_name , status from dwh_ehs.ehf_empnl_hospinfo_dm ) eeh on eeh.hospinfo_id = eh.hosp_empnl_ref_num
left join (select cmb_dtl_id , cmb_dtl_name  from dwh_ehs.asrim_combo_ehs) ace on ace.cmb_dtl_id = eeh.status
left join (SELECT LOC_ID,LOC_NAME AS HOSPITAL_DISTRICT_NAME	, loc_parnt_id   FROM dwh_ehs.ehfm_locations_dm) elhd on elhd.loc_id = eh.hosp_dist
LEFT JOIN (SELECT LOC_ID,LOC_NAME AS HOSPITAL_STATE_NAME     FROM dwh_ehs.ehfm_locations_dm) elhs on elhs.loc_id = elhd.loc_parnt_id 
LEFT JOIN (SELECT distinct case_id,asri_cat_code,icd_proc_code, crt_dt  as therapy_raised_date FROM dwh_ehs.ehf_case_therapy_dm where activeyn='Y' ) ect ON ect.case_id = ec.case_id 
LEFT JOIN (SELECT  asri_code, icd_proc_code, proc_name,icd_amt,common_cat_amt,hosp_stay_amt, medical_surg,active_yn,crt_dt, lst_upd_dt  FROM dwh_ehs.ehfm_main_therapy_dm where state='CD201' ) emt ON emt.asri_code = ect.asri_cat_code AND emt.icd_proc_code = ect.icd_proc_code 
LEFT JOIN (SELECT distinct dis_main_id,dis_main_name,dis_active_yn,crt_dt, lst_upd_dt FROM dwh_ehs.ehfm_specialities_dm) esp ON esp.dis_main_id = emt.asri_code
);










		  

          





   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   










